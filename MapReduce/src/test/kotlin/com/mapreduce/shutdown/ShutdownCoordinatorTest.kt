package com.mapreduce.shutdown

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.ShutdownEvent
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration

class ShutdownCoordinatorTest {

    private lateinit var config: FrameworkConfig
    private lateinit var shutdownConfig: FrameworkConfig.ShutdownConfig
    private lateinit var workerConfig: FrameworkConfig.WorkerConfig
    private lateinit var leaderManager: LeaderManager
    private lateinit var taskRepository: TaskRepository
    private lateinit var meterRegistry: MeterRegistry
    private lateinit var coordinator: ShutdownCoordinator

    private val podId = "test-pod-1"

    @BeforeEach
    fun setUp() {
        config = mock<FrameworkConfig>()
        shutdownConfig = mock<FrameworkConfig.ShutdownConfig>()
        workerConfig = mock<FrameworkConfig.WorkerConfig>()
        leaderManager = mock<LeaderManager>()
        taskRepository = mock<TaskRepository>()
        meterRegistry = SimpleMeterRegistry()

        whenever(config.shutdown()).thenReturn(shutdownConfig)
        whenever(config.worker()).thenReturn(workerConfig)
        whenever(workerConfig.id()).thenReturn(podId)

        // Fast timeouts for tests
        whenever(shutdownConfig.drainTimeout()).thenReturn(Duration.ofMillis(100))
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofMillis(50))
        whenever(shutdownConfig.releaseTimeout()).thenReturn(Duration.ofMillis(50))
        whenever(shutdownConfig.logInterval()).thenReturn(Duration.ofSeconds(60))

        coordinator = ShutdownCoordinator(
            config, leaderManager, taskRepository, meterRegistry,
        )
    }

    // ── Initial state ─────────────────────────────────────────────

    @Test
    fun `initial state is RUNNING`() {
        assertEquals(ShutdownState.RUNNING, coordinator.state)
    }

    @Test
    fun `isShuttingDown is false initially`() {
        assertFalse(coordinator.isShuttingDown)
    }

    @Test
    fun `drainDeadline is null initially`() {
        assertNull(coordinator.drainDeadline)
    }

    // ── In-flight task tracking ─────────────────────────────────

    @Test
    fun `trackTaskStart and trackTaskEnd update inFlightTasks`() {
        coordinator.trackTaskStart()
        coordinator.trackTaskStart()
        coordinator.trackTaskStart()

        assertEquals(3, coordinator.inFlightTasks)

        coordinator.trackTaskEnd()

        assertEquals(2, coordinator.inFlightTasks)
    }

    @Test
    fun `inFlightTasks returns 0 initially`() {
        assertEquals(0, coordinator.inFlightTasks)
    }

    // ── trackTaskEnd during drain ──────────────────────────────────

    @Test
    fun `trackTaskEnd during drain increments completed counter in shutdown metrics`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)
        whenever(shutdownConfig.drainTimeout()).thenReturn(Duration.ofSeconds(2))

        // Simulate 3 in-flight tasks
        coordinator.trackTaskStart()
        coordinator.trackTaskStart()
        coordinator.trackTaskStart()

        // Complete tasks on a background thread during drain (mirrors production)
        val thread = Thread {
            Thread.sleep(50)
            coordinator.trackTaskEnd()
            coordinator.trackTaskEnd()
            coordinator.trackTaskEnd()
        }
        thread.start()

        coordinator.onShutdown(ShutdownEvent()) // blocks until drain completes
        thread.join()

        val counter = meterRegistry.find("taskqueue_shutdown_tasks_completed").counter()
        assertNotNull(counter)
        assertEquals(3.0, counter!!.count())
    }

    @Test
    fun `trackTaskEnd before shutdown does not increment drain counter`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        // Tasks complete while still RUNNING — should NOT count
        coordinator.trackTaskStart()
        coordinator.trackTaskEnd()
        coordinator.trackTaskStart()
        coordinator.trackTaskEnd()

        coordinator.onShutdown(ShutdownEvent())

        val counter = meterRegistry.find("taskqueue_shutdown_tasks_completed").counter()
        assertNotNull(counter)
        assertEquals(0.0, counter!!.count())
    }

    // ── onShutdown transitions ────────────────────────────────────

    @Test
    fun `onShutdown transitions through all phases to TERMINATED`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
        assertTrue(coordinator.isShuttingDown)
    }

    @Test
    fun `onShutdown sets drainDeadline`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        assertNotNull(coordinator.drainDeadline)
    }

    @Test
    fun `onShutdown calls leaderManager shutdown`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        runBlocking { verify(leaderManager).shutdown() }
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `onShutdown calls leaderManager shutdown even when not leader`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        // Always called to clean up election scope
        runBlocking { verify(leaderManager).shutdown() }
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `onShutdown releases tasks via taskRepository`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(5)

        coordinator.onShutdown(ShutdownEvent())

        verify(taskRepository).releaseTasksByPod(podId)
    }

    @Test
    fun `onShutdown records shutdown duration metric`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        val timer = meterRegistry.find("taskqueue_shutdown_duration_seconds").timer()
        assertNotNull(timer)
        assertTrue(timer!!.count() > 0)
    }

    @Test
    fun `onShutdown tolerates leaderManager shutdown exception`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)
        runBlocking {
            whenever(leaderManager.shutdown()).thenThrow(RuntimeException("boom"))
        }

        coordinator.onShutdown(ShutdownEvent())

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `onShutdown tolerates taskRepository exception during release`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenThrow(RuntimeException("DB down"))

        coordinator.onShutdown(ShutdownEvent())

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `onShutdown skips drain when no in-flight tasks`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }
}
