package com.mapreduce.shutdown

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.ShutdownStateChanged
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.event.Event
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.Semaphore

class ShutdownCoordinatorTest {

    private lateinit var config: FrameworkConfig
    private lateinit var shutdownConfig: FrameworkConfig.ShutdownConfig
    private lateinit var workerConfig: FrameworkConfig.WorkerConfig
    private lateinit var leaderManager: LeaderManager
    private lateinit var taskRepository: TaskRepository
    private lateinit var meterRegistry: MeterRegistry
    private lateinit var shutdownStateEvent: Event<ShutdownStateChanged>
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

        shutdownStateEvent = mock<Event<ShutdownStateChanged>>()

        whenever(config.shutdown()).thenReturn(shutdownConfig)
        whenever(config.worker()).thenReturn(workerConfig)
        whenever(workerConfig.id()).thenReturn(podId)

        // Fast timeouts for tests
        whenever(shutdownConfig.drainTimeout()).thenReturn(Duration.ofMillis(100))
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofMillis(50))
        whenever(shutdownConfig.releaseTimeout()).thenReturn(Duration.ofMillis(50))
        whenever(shutdownConfig.logInterval()).thenReturn(Duration.ofSeconds(60))

        coordinator = ShutdownCoordinator(
            config, leaderManager, taskRepository, meterRegistry, shutdownStateEvent,
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

    // ── Bulkhead integration ──────────────────────────────────────

    @Test
    fun `registerBulkhead tracks semaphore and size`() {
        val sem = Semaphore(8)
        coordinator.registerBulkhead(sem, 8)

        assertEquals(8, coordinator.bulkheadSize)
    }

    @Test
    fun `inFlightTasks equals size minus availablePermits`() {
        val sem = Semaphore(4)
        coordinator.registerBulkhead(sem, 4)

        sem.acquire(3)

        assertEquals(3, coordinator.inFlightTasks)
    }

    @Test
    fun `inFlightTasks returns 0 when no semaphore registered`() {
        assertEquals(0, coordinator.inFlightTasks)
    }

    @Test
    fun `inFlightTasks returns 0 when all permits available`() {
        val sem = Semaphore(4)
        coordinator.registerBulkhead(sem, 4)

        assertEquals(0, coordinator.inFlightTasks)
    }

    // ── Leader scope callbacks ────────────────────────────────────

    @Test
    fun `registerLeaderScopeCallback stores callback`() {
        var called = false
        coordinator.registerLeaderScopeCallback { called = true }

        // Callback is stored but not invoked during registration
        assertFalse(called)
    }

    // ── recordDrainCompletion ─────────────────────────────────────

    @Test
    fun `recordDrainCompletion increments counter reflected in shutdown metrics`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.recordDrainCompletion()
        coordinator.recordDrainCompletion()
        coordinator.recordDrainCompletion()

        coordinator.onShutdown(ShutdownEvent())

        // The counter value is emitted as a metric during Phase 4
        val counter = meterRegistry.find("taskqueue_shutdown_tasks_completed").counter()
        assertNotNull(counter)
        assertEquals(3.0, counter!!.count())
    }

    // ── Metrics registration ──────────────────────────────────────

    @Test
    fun `registerMetrics creates gauges`() {
        coordinator.registerMetrics()

        val stateGauge = meterRegistry.find("taskqueue_shutdown_state").gauge()
        val inflightGauge = meterRegistry.find("taskqueue_shutdown_inflight_tasks").gauge()

        assertNotNull(stateGauge)
        assertNotNull(inflightGauge)
        assertEquals(ShutdownState.RUNNING.ordinal.toDouble(), stateGauge!!.value())
        assertEquals(0.0, inflightGauge!!.value())
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
    fun `onShutdown calls leader teardown when pod was leader`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        var leaderCallbackInvoked = false
        coordinator.registerLeaderScopeCallback { leaderCallbackInvoked = true }

        coordinator.onShutdown(ShutdownEvent())

        assertTrue(leaderCallbackInvoked)
        verify(leaderManager).releaseLeaseExplicitly()
        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `onShutdown invokes all registered leader scope callbacks`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        val invocations = mutableListOf<Int>()
        coordinator.registerLeaderScopeCallback { invocations.add(1) }
        coordinator.registerLeaderScopeCallback { invocations.add(2) }
        coordinator.registerLeaderScopeCallback { invocations.add(3) }

        coordinator.onShutdown(ShutdownEvent())

        assertEquals(listOf(1, 2, 3), invocations)
    }

    @Test
    fun `onShutdown skips leader teardown when pod was not leader`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        verify(leaderManager, never()).releaseLeaseExplicitly()
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
    fun `onShutdown fires state change events for each transition`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.onShutdown(ShutdownEvent())

        // Capture all three fireAsync invocations
        val captor = argumentCaptor<ShutdownStateChanged>()
        verify(shutdownStateEvent, atLeast(3)).fireAsync(captor.capture())

        val events = captor.allValues
        assertTrue(events.any {
            it.previousState == ShutdownState.RUNNING && it.newState == ShutdownState.DRAINING
        })
        assertTrue(events.any {
            it.previousState == ShutdownState.DRAINING && it.newState == ShutdownState.RELEASING
        })
        assertTrue(events.any {
            it.previousState == ShutdownState.RELEASING && it.newState == ShutdownState.TERMINATED
        })
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
    fun `onShutdown tolerates leader scope callback exception`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        coordinator.registerLeaderScopeCallback { throw RuntimeException("boom") }

        // Should not propagate — continues to next phases
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
    fun `onShutdown skips drain when no bulkhead registered`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        // No semaphore registered — Phase 2 should be a no-op
        coordinator.onShutdown(ShutdownEvent())

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }

    @Test
    fun `onShutdown skips drain when no in-flight tasks`() {
        whenever(leaderManager.isActive).thenReturn(false)
        whenever(taskRepository.releaseTasksByPod(podId)).thenReturn(0)

        val sem = Semaphore(4)
        coordinator.registerBulkhead(sem, 4)
        // All permits available → 0 in-flight

        coordinator.onShutdown(ShutdownEvent())

        assertEquals(ShutdownState.TERMINATED, coordinator.state)
    }
}
