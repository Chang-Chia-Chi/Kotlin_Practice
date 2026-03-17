package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.StartupEvent
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.TimeUnit

class MapReduceOrchestratorTest {

    private lateinit var config: FrameworkConfig
    private lateinit var leaderConfig: FrameworkConfig.LeaderConfig
    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var taskRepository: TaskRepository
    private lateinit var leaderManager: LeaderManager
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var orchestrator: MapReduceOrchestrator

    @BeforeEach
    fun setUp() {
        config = mock()
        leaderConfig = mock()
        whenever(config.leader()).thenReturn(leaderConfig)
        whenever(leaderConfig.monitorInterval()).thenReturn(Duration.ofMillis(50))

        taskGroupRepository = mock()
        taskRepository = mock()
        leaderManager = mock()
        shutdownCoordinator = mock()
        meterRegistry = SimpleMeterRegistry()

        doNothing().whenever(shutdownCoordinator).registerLeaderScopeCallback(any())

        whenever(leaderManager.isActive).thenReturn(true)
        whenever(leaderManager.token).thenReturn(1L)

        whenever(taskGroupRepository.findGroupsByStatus(GroupStatus.ACTIVE)).thenReturn(emptyList())

        orchestrator = MapReduceOrchestrator(
            config, taskGroupRepository, taskRepository,
            leaderManager, shutdownCoordinator, meterRegistry,
        )
    }

    @Test
    fun `polls queue depth on every cycle`() {
        whenever(taskRepository.countPendingByQueue()).thenReturn(mapOf("default" to 5))

        startAndAwait {
            verify(taskRepository, atLeast(1)).countPendingByQueue()
        }
    }

    @Test
    fun `recovery sweep detects stuck ACTIVE groups`() {
        val stuckGroup = TaskGroup(
            groupId = "g-stuck",
            groupType = "wc",
            status = GroupStatus.ACTIVE,
            phase = "map",
            phaseTotal = 10,
            phaseCompleted = 10,
            phaseFailed = 0,
            onCompleteHandler = "wc.__phase_complete",
        )
        whenever(taskGroupRepository.findGroupsByStatus(GroupStatus.ACTIVE)).thenReturn(listOf(stuckGroup))
        whenever(taskRepository.countByGroupAndStatus(any(), any())).thenReturn(10)
        whenever(taskRepository.findByGroupAndHandler(any(), any())).thenReturn(null)

        // Recovery sweep runs at 5x interval, so wait longer
        startAndAwait(timeout = 5) {
            verify(taskGroupRepository, atLeast(1)).findGroupsByStatus(GroupStatus.ACTIVE)
        }
    }

    // ── Helpers ──────────────────────────────────────────────────

    private fun startAndAwait(timeout: Long = 3, assertions: () -> Unit) {
        val startupEvent = mock<StartupEvent>()
        orchestrator.onStart(startupEvent)

        await.atMost(timeout, TimeUnit.SECONDS).untilAsserted(assertions)
    }
}
