package com.mapreduce.observability.health

import com.mapreduce.queue.worker.PodCircuitBreaker
import jakarta.enterprise.context.ApplicationScoped

/**
 * Health contributor for the pod-level circuit breaker.
 *
 * - **Liveness:** null (an open breaker doesn't mean the pod should be restarted).
 * - **Readiness:** DOWN when pod-level breaker tripped, UP otherwise.
 */
@ApplicationScoped
class CircuitBreakerHealthContributor(
    private val podCircuitBreaker: PodCircuitBreaker,
) : HealthContributor {

    override val name: String = "circuit-breaker"

    override fun liveness(): ProbeResult? = null

    override fun readiness(): ProbeResult =
        if (podCircuitBreaker.isTripped) {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "podBreaker" to "TRIPPED",
                    "reason" to "Consecutive failure threshold exceeded — pod quarantined",
                ),
            )
        } else {
            ProbeResult(
                status = HealthStatus.UP,
                details = mapOf("podBreaker" to "CLOSED"),
            )
        }
}
