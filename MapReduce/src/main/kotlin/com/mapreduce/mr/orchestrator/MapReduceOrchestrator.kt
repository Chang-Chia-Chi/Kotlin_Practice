package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Leader-only loop that publishes queue depth gauges for HPA autoscaling.
 *
 * Phase transitions are handled reactively by callback tasks — this class
 * no longer monitors job/group state.
 */
@ApplicationScoped
class MapReduceOrchestrator(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val leaderManager: LeaderManager,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(MapReduceOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val queueDepths = ConcurrentHashMap<String, AtomicLong>()

    fun onStart(@Observes ev: StartupEvent) {
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.metrics().queueDepthInterval().toMillis()
        scope.launch {
            delay(interval)
            while (isActive) {
                if (leaderManager.isActive) {
                    try {
                        withContext(Dispatchers.IO) { pollQueueDepth() }
                    } catch (e: Exception) {
                        log.warnf(e, "Failed to poll queue depth")
                    }
                }
                delay(interval)
            }
        }
    }

    private fun pollQueueDepth() {
        val counts = taskRepository.countPendingByQueue()
        for ((queue, count) in counts) {
            queueDepths.computeIfAbsent(queue) { q ->
                AtomicLong(0).also { gauge ->
                    meterRegistry.gauge(
                        "framework.queue.depth",
                        listOf(Tag.of("queue_name", q)),
                        gauge,
                    ) { it.toDouble() }
                }
            }.set(count.toLong())
        }
        for ((queue, depth) in queueDepths) {
            if (queue !in counts) depth.set(0)
        }
    }
}
