package com.mapreduce.observability.health

import com.mapreduce.queue.pipeline.CircuitBreakerMiddleware
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.worker.PodCircuitBreaker
import jakarta.enterprise.context.ApplicationScoped

/**
 * Health contributor for circuit breakers (pod-level + per-handler).
 *
 * - **Liveness:** null (an open breaker doesn't mean the pod should be restarted).
 * - **Readiness:**
 *   - **UP** — all breakers closed, pod breaker closed.
 *   - **DEGRADED** — some per-handler breakers open, but not all. Pod can still do useful work.
 *   - **DOWN** — pod-level breaker tripped OR all per-handler breakers open.
 */
@ApplicationScoped
class CircuitBreakerHealthContributor(
    private val podCircuitBreaker: PodCircuitBreaker,
    private val circuitBreakerMiddleware: CircuitBreakerMiddleware,
    private val handlerRegistry: HandlerRegistry,
) : HealthContributor {

    override val name: String = "circuit-breakers"

    override fun liveness(): ProbeResult? = null

    override fun readiness(): ProbeResult {
        // Pod-level breaker trumps everything
        if (podCircuitBreaker.isTripped) {
            return ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "podBreaker" to "TRIPPED",
                    "reason" to "Consecutive failure threshold exceeded — pod quarantined",
                ),
            )
        }

        val suppressed = circuitBreakerMiddleware.suppressedHandlers()
        if (suppressed.isEmpty()) {
            return ProbeResult(
                status = HealthStatus.UP,
                details = mapOf("podBreaker" to "CLOSED"),
            )
        }

        val totalHandlers = handlerRegistry.registeredHandlers().size
        return if (totalHandlers > 0 && suppressed.size >= totalHandlers) {
            // All per-handler breakers open — pod can't do any useful work
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "podBreaker" to "CLOSED",
                    "open" to suppressed.toList(),
                    "reason" to "All handler circuit breakers are open",
                ),
            )
        } else {
            // Some breakers open — degraded but still functional
            ProbeResult(
                status = HealthStatus.DEGRADED,
                details = mapOf(
                    "podBreaker" to "CLOSED",
                    "open" to suppressed.toList(),
                ),
            )
        }
    }
}
