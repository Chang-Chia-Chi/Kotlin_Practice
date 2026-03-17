package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

class StaleTaskReaperTest {

    private lateinit var config: FrameworkConfig
    private lateinit var reaperConfig: FrameworkConfig.ReaperConfig
    private lateinit var taskRepository: TaskRepository
    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var leaderManager: LeaderManager
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var reaper: StaleTaskReaper

    @BeforeEach
    fun setUp() {
        config = mock<FrameworkConfig>()
        reaperConfig = mock<FrameworkConfig.ReaperConfig>()
        whenever(config.reaper()).thenReturn(reaperConfig)
        whenever(reaperConfig.scanInterval()).thenReturn(Duration.ofSeconds(30))
        whenever(reaperConfig.staleThreshold()).thenReturn(Duration.ofMinutes(5))
        whenever(reaperConfig.batchSize()).thenReturn(50)

        taskRepository = mock<TaskRepository>()
        taskGroupRepository = mock<TaskGroupRepository>()
        leaderManager = mock<LeaderManager>()
        whenever(leaderManager.token).thenReturn(5L)

        shutdownCoordinator = mock<ShutdownCoordinator>()
        meterRegistry = SimpleMeterRegistry()

        reaper = StaleTaskReaper(
            config, taskRepository, taskGroupRepository, leaderManager,
            shutdownCoordinator, meterRegistry,
        )
    }

    // ── Reap logic ────────────────────────────────────────────────────

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
            .thenReturn(false)

        invokeReap()

        verify(taskRepository).reclaimStaleTask(
            eq("task-1"),
            eq(5L),
            any(),
        )
    }

    @Test
    fun `reap dead-letters task with exhausted retries`() {
        val staleTask = staleTask("task-2", retryCount = 2, maxRetries = 3)
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(listOf(staleTask))
        whenever(taskRepository.reclaimStaleTask(any(), any(), any()))
            .thenReturn(true)

        invokeReap()

        verify(taskRepository).reclaimStaleTask(
            eq("task-2"),
            eq(5L),
            any(),
        )
    }

    @Test
    fun `reap skips task already handled by another leader`() {
        val staleTask = staleTask("task-3")
        whenever(taskRepository.findStaleTasks(any(), any())).thenReturn(listOf(staleTask))
        whenever(taskRepository.reclaimStaleTask(any(), any(), any()))
            .thenReturn(null)

        invokeReap()

        // Only one call to reclaimStaleTask, which returned null
        verify(taskRepository).reclaimStaleTask(any(), any(), any())
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
            .thenReturn(false)
        whenever(taskRepository.reclaimStaleTask(eq("t-dead"), any(), any()))
            .thenReturn(true)
        whenever(taskRepository.reclaimStaleTask(eq("t-skipped"), any(), any()))
            .thenReturn(null)

        invokeReap()

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

    // ── helpers ───────────────────────────────────────────────────────

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
        claimedAt = Instant.now().minus(Duration.ofMinutes(10)),
        retryCount = retryCount,
        maxRetries = maxRetries,
        createdAt = Instant.now().minus(Duration.ofMinutes(15)),
    )

    private fun invokeReap() {
        val method = StaleTaskReaper::class.java.getDeclaredMethod("reap")
        method.isAccessible = true
        method.invoke(reaper)
    }
}
