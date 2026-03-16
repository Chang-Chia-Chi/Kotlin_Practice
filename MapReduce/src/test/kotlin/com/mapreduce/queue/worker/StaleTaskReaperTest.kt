package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskDeadLettered
import com.mapreduce.event.TaskReclaimed
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.event.Event
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.lang.reflect.InvocationTargetException
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

/**
 * Tests the stale task reaper's reap logic and config validation.
 *
 * Since [StaleTaskReaper.reap] is private and runs inside an internal
 * coroutine scope, we invoke it via reflection to keep tests fast,
 * deterministic, and free of Thread.sleep().
 */
class StaleTaskReaperTest {

    private lateinit var config: FrameworkConfig
    private lateinit var reaperConfig: FrameworkConfig.ReaperConfig
    private lateinit var heartbeatConfig: FrameworkConfig.HeartbeatConfig
    private lateinit var taskRepository: TaskRepository
    private lateinit var leaderManager: LeaderManager
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var deadLetterEvent: Event<TaskDeadLettered>
    private lateinit var taskReclaimedEvent: Event<TaskReclaimed>
    private lateinit var reaper: StaleTaskReaper

    @BeforeEach
    fun setUp() {
        config = mock<FrameworkConfig>()
        reaperConfig = mock<FrameworkConfig.ReaperConfig>()
        heartbeatConfig = mock<FrameworkConfig.HeartbeatConfig>()
        whenever(config.reaper()).thenReturn(reaperConfig)
        whenever(config.heartbeat()).thenReturn(heartbeatConfig)
        whenever(reaperConfig.scanInterval()).thenReturn(Duration.ofSeconds(30))
        whenever(reaperConfig.staleThreshold()).thenReturn(Duration.ofSeconds(90))
        whenever(reaperConfig.batchSize()).thenReturn(50)
        whenever(heartbeatConfig.interval()).thenReturn(Duration.ofSeconds(30))

        taskRepository = mock<TaskRepository>()
        leaderManager = mock<LeaderManager>()
        whenever(leaderManager.token).thenReturn(5L)

        shutdownCoordinator = mock<ShutdownCoordinator>()
        meterRegistry = SimpleMeterRegistry()

        deadLetterEvent = mock<Event<TaskDeadLettered>>()
        taskReclaimedEvent = mock<Event<TaskReclaimed>>()

        reaper = StaleTaskReaper(
            config, taskRepository, leaderManager,
            shutdownCoordinator, meterRegistry, deadLetterEvent, taskReclaimedEvent,
        )
    }

    // ── Config validation ─────────────────────────────────────────

    @Test
    fun `validateConfig passes when stale-threshold equals 3x heartbeat`() {
        whenever(heartbeatConfig.interval()).thenReturn(Duration.ofSeconds(30))
        whenever(reaperConfig.staleThreshold()).thenReturn(Duration.ofSeconds(90))

        // Should not throw
        invokeValidateConfig()
    }

    @Test
    fun `validateConfig fails when stale-threshold less than 3x heartbeat`() {
        whenever(heartbeatConfig.interval()).thenReturn(Duration.ofSeconds(30))
        whenever(reaperConfig.staleThreshold()).thenReturn(Duration.ofSeconds(60))

        val ex = assertThrows(InvocationTargetException::class.java) {
            invokeValidateConfig()
        }
        assertTrue(ex.cause is IllegalArgumentException)
    }

    // ── Reap logic ────────────────────────────────────────────────

    @Test
    fun `reap with no stale tasks is a no-op`() {
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(emptyList())

        invokeReap()

        verify(taskRepository, never()).reclaimStaleTask(any(), any(), any())
    }

    @Test
    fun `reap reclaims stale task to PENDING`() {
        val staleTask = staleTask("task-1", claimedBy = "dead-pod")
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(listOf(staleTask))
        whenever(taskRepository.reclaimStaleTask(any(), any(), any()))
            .thenReturn(false) // reclaimed to PENDING

        invokeReap()

        verify(taskRepository).reclaimStaleTask(
            eq("task-1"),
            eq(5L),
            any(),
        )
        verify(taskReclaimedEvent).fireAsync(any())
        verify(deadLetterEvent, never()).fireAsync(any())
    }

    @Test
    fun `reap dead-letters task with exhausted retries and fires event`() {
        val staleTask = staleTask("task-2", retryCount = 2, maxRetries = 3)
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(listOf(staleTask))
        whenever(taskRepository.reclaimStaleTask(any(), any(), any()))
            .thenReturn(true) // dead-lettered

        invokeReap()

        verify(taskRepository).reclaimStaleTask(
            eq("task-2"),
            eq(5L),
            any(),
        )
        verify(deadLetterEvent).fireAsync(any())
        verify(taskReclaimedEvent).fireAsync(any())
    }

    @Test
    fun `reap skips task already handled by another leader`() {
        val staleTask = staleTask("task-3")
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(listOf(staleTask))
        whenever(taskRepository.reclaimStaleTask(any(), any(), any()))
            .thenReturn(null) // fence rejected

        invokeReap()

        verify(taskReclaimedEvent, never()).fireAsync(any())
        verify(deadLetterEvent, never()).fireAsync(any())
    }

    @Test
    fun `reap processes batch of mixed results`() {
        val tasks = listOf(
            staleTask("t-pending", claimedBy = "pod-a"),
            staleTask("t-dead", claimedBy = "pod-b", retryCount = 2, maxRetries = 3),
            staleTask("t-skipped", claimedBy = "pod-c"),
        )
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(tasks)

        whenever(taskRepository.reclaimStaleTask(eq("t-pending"), any(), any()))
            .thenReturn(false) // reclaimed
        whenever(taskRepository.reclaimStaleTask(eq("t-dead"), any(), any()))
            .thenReturn(true)  // dead-lettered
        whenever(taskRepository.reclaimStaleTask(eq("t-skipped"), any(), any()))
            .thenReturn(null)  // already handled

        invokeReap()

        // reclaimed counter: 2 (pending + dead), dead-lettered counter: 1
        val reclaimed = meterRegistry.counter("taskqueue.reaper.reclaimed").count()
        val deadLettered = meterRegistry.counter("taskqueue.reaper.dead_lettered").count()
        assertEquals(2.0, reclaimed)
        assertEquals(1.0, deadLettered)
    }

    @Test
    fun `reap records scan duration metric`() {
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(emptyList())

        invokeReap()

        val scanTimer = meterRegistry.timer("taskqueue.reaper.scan_duration")
        assertEquals(1, scanTimer.count())
    }

    // ── helpers ───────────────────────────────────────────────────

    private fun staleTask(
        taskId: String,
        handler: String = "test.handler",
        claimedBy: String = "dead-pod",
        retryCount: Int = 0,
        maxRetries: Int = 3,
    ) = Task(
        taskId = taskId,
        handler = handler,
        queue = "default",
        payload = "{}",
        status = TaskStatus.CLAIMED,
        claimedBy = claimedBy,
        claimedAt = Instant.now().minus(Duration.ofMinutes(5)),
        lastHeartbeat = Instant.now().minus(Duration.ofMinutes(3)),
        retryCount = retryCount,
        maxRetries = maxRetries,
        createdAt = Instant.now().minus(Duration.ofMinutes(10)),
    )

    /** Invoke the private `reap()` method via reflection. */
    private fun invokeReap() {
        val method = StaleTaskReaper::class.java.getDeclaredMethod("reap")
        method.isAccessible = true
        method.invoke(reaper)
    }

    /** Invoke the private `validateConfig()` method via reflection. */
    private fun invokeValidateConfig() {
        val method = StaleTaskReaper::class.java.getDeclaredMethod("validateConfig")
        method.isAccessible = true
        method.invoke(reaper)
    }
}
