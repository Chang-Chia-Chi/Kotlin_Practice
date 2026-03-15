package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.worker.WorkerLoop
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.time.Instant

/**
 * Health contributor for the worker poll loop.
 *
 * - **Liveness:** Is the claim coroutine alive? Compares [WorkerLoop.lastPollTimestamp]
 *   against a configurable stale threshold (default 3× poll interval).
 * - **Readiness:** Is the handler registry populated?
 */
@ApplicationScoped
class WorkerLoopHealthContributor(
    private val workerLoop: WorkerLoop,
    private val handlerRegistry: HandlerRegistry,
    private val config: FrameworkConfig,
) : HealthContributor {

    override val name: String = "worker-loop"

    override fun liveness(): ProbeResult {
        val threshold = config.health().workerLoopStaleThreshold()
        val elapsed = Duration.between(workerLoop.lastPollTimestamp, Instant.now())

        return if (elapsed <= threshold) {
            ProbeResult(
                status = HealthStatus.UP,
                details = mapOf("lastPollAge" to elapsed.seconds),
            )
        } else {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "lastPollAge" to elapsed.seconds,
                    "threshold" to threshold.seconds,
                    "reason" to "Claim coroutine hasn't polled in ${elapsed.seconds}s (threshold ${threshold.seconds}s)",
                ),
            )
        }
    }

    override fun readiness(): ProbeResult {
        val handlers = handlerRegistry.registeredHandlers()
        return if (handlers.isNotEmpty()) {
            ProbeResult(
                status = HealthStatus.UP,
                details = mapOf("handlers" to handlers.size),
            )
        } else {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "handlers" to 0,
                    "reason" to "No handlers registered — CDI initialization may not be complete",
                ),
            )
        }
    }
}
