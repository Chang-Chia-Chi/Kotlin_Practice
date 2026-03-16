package com.mapreduce.observability.health

import com.mapreduce.queue.worker.PodCircuitBreaker
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Readiness

@Readiness
@ApplicationScoped
class CircuitBreakerHealthContributor(
    private val podCircuitBreaker: PodCircuitBreaker,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("circuit-breaker")
        return if (podCircuitBreaker.isTripped) {
            builder.down()
                .withData("podBreaker", "TRIPPED")
                .withData("reason", "Consecutive failure threshold exceeded — pod quarantined")
                .build()
        } else {
            builder.up()
                .withData("podBreaker", "CLOSED")
                .build()
        }
    }
}
