package com.mapreduce.queue.metrics

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
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

class QueueDepthExporterTest {

    private lateinit var config: FrameworkConfig
    private lateinit var metricsConfig: FrameworkConfig.MetricsConfig
    private lateinit var taskRepository: TaskRepository
    private lateinit var leaderManager: LeaderManager
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var exporter: QueueDepthExporter

    @BeforeEach
    fun setUp() {
        config = mock()
        metricsConfig = mock()
        whenever(config.metrics()).thenReturn(metricsConfig)
        whenever(metricsConfig.queueDepthInterval()).thenReturn(Duration.ofMillis(50))

        taskRepository = mock()
        leaderManager = mock()
        shutdownCoordinator = mock()
        meterRegistry = SimpleMeterRegistry()

        doNothing().whenever(shutdownCoordinator).registerLeaderScopeCallback(any())

        whenever(leaderManager.isActive).thenReturn(true)

        exporter = QueueDepthExporter(
            config, taskRepository,
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
    fun `registers queue depth gauge per queue name`() {
        whenever(taskRepository.countPendingByQueue()).thenReturn(mapOf("mr" to 3, "default" to 7))

        startAndAwait {
            verify(taskRepository, atLeast(1)).countPendingByQueue()
        }

        val mrGauge = meterRegistry.find("framework.queue.depth").tag("queue_name", "mr").gauge()
        assert(mrGauge != null) { "Expected gauge for queue 'mr'" }
    }

    // ── Helpers ──────────────────────────────────────────────────

    private fun startAndAwait(timeout: Long = 3, assertions: () -> Unit) {
        val startupEvent = mock<StartupEvent>()
        exporter.onStart(startupEvent)

        await.atMost(timeout, TimeUnit.SECONDS).untilAsserted(assertions)
    }
}
