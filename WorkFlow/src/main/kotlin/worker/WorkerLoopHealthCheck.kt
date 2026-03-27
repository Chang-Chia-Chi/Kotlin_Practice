package com.workflow.worker

import com.workflow.config.FrameworkConfig
import jakarta.inject.Singleton
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import java.time.Duration
import java.time.Instant

@Liveness
@Singleton
class WorkerLoopHealthCheck(
    private val workerLoop: WorkerLoop,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val lastActivity = workerLoop.lastActivityTimestamp
        val threshold = config.worker().pollInterval().multipliedBy(5)
        val age = Duration.between(lastActivity, Instant.now())

        return if (age < threshold) {
            HealthCheckResponse.up("worker-loop")
        } else {
            HealthCheckResponse.named("worker-loop")
                .down()
                .withData("last_activity_age_seconds", age.seconds)
                .withData("threshold_seconds", threshold.seconds)
                .build()
        }
    }
}
