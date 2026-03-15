package com.mapreduce.observability

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
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
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Kubernetes HPA autoscaling metrics.
 *
 * Exposes two core signals for the Horizontal Pod Autoscaler:
 *
 * - **Scale Up:** `framework.queue.depth` — leader-only gauge of PENDING tasks per queue.
 *   Only the leader pod queries the database; non-leaders expose 0.
 *   Prometheus aggregates into a single cluster-wide metric.
 *
 * - **Scale Down:** `framework.worker.bulkhead.utilization` — per-pod gauge (0.0–1.0)
 *   of concurrent execution slots in use. HPA averages across pods.
 *
 * Also provides operational health metrics for dashboards and alerts:
 * - `framework.task.duration.seconds` — task processing latency
 * - `framework.task.errors.total` — task failure counter
 * - `framework.orchestration.duration.seconds` — job completion time
 */
@ApplicationScoped
class AutoscalingMetrics(
    private val config: FrameworkConfig,
    private val leaderManager: LeaderManager,
    private val taskRepository: TaskRepository,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(AutoscalingMetrics::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** Per-queue AtomicLong backing the queue depth gauge. Updated by leader-only poll. */
    private val queueDepths = ConcurrentHashMap<String, AtomicLong>()

    fun onStart(@Observes ev: StartupEvent) {
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }
        registerBulkheadUtilizationGauge()
        startQueueDepthPoller()
        log.info("Autoscaling metrics registered")
    }

    // ── Scale Up Signal: Queue Depth (Leader-Only) ────────────────

    private fun startQueueDepthPoller() {
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
                } else {
                    queueDepths.values.forEach { it.set(0) }
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
        // Zero out queues that had pending tasks before but no longer do
        for ((queue, depth) in queueDepths) {
            if (queue !in counts) {
                depth.set(0)
            }
        }
    }

    // ── Scale Down Signal: Bulkhead Utilization ───────────────────

    private fun registerBulkheadUtilizationGauge() {
        val podId = config.worker().id()
        meterRegistry.gauge(
            "framework.worker.bulkhead.utilization",
            listOf(Tag.of("pod_id", podId)),
            shutdownCoordinator,
        ) { coordinator ->
            val size = coordinator.bulkheadSize
            if (size == 0) 0.0
            else coordinator.inFlightTasks.toDouble() / size
        }
    }

    // ── Operational: Task Duration ────────────────────────────────

    /**
     * Record task processing latency.
     * @param status one of "Success", "Retry", "DeadLetter"
     */
    fun recordTaskDuration(handler: String, status: String, durationNanos: Long) {
        Timer.builder("framework.task.duration.seconds")
            .tag("handler", handler)
            .tag("status", status)
            .register(meterRegistry)
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }

    // ── Operational: Task Errors ──────────────────────────────────

    fun recordTaskError(handler: String, errorType: String) {
        meterRegistry.counter(
            "framework.task.errors.total",
            "handler", handler,
            "error_type", errorType,
        ).increment()
    }

    // ── Operational: Orchestration Duration ───────────────────────

    /**
     * Record end-to-end orchestration duration from submission to terminal state.
     * @param orchestrationType e.g. "MapReduce"
     */
    fun recordOrchestrationDuration(
        orchestrationType: String,
        identifier: String,
        createdAt: Instant,
    ) {
        val duration = Duration.between(createdAt, Instant.now())
        Timer.builder("framework.orchestration.duration.seconds")
            .tag("orchestration_type", orchestrationType)
            .tag("identifier", identifier)
            .register(meterRegistry)
            .record(duration.toMillis(), TimeUnit.MILLISECONDS)
    }
}
