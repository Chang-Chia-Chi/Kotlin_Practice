package com.workflow.leader

import com.workflow.config.FrameworkConfig
import jakarta.inject.Singleton
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import java.time.Duration
import java.time.Instant

@Liveness
@Singleton
class LeaderHealthCheck(
    private val leaderElection: LeaderElection,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        if (!leaderElection.isActive) {
            return HealthCheckResponse.up("leader-election")
        }

        val age = Duration.between(leaderElection.lastHeartbeat, Instant.now())
        val threshold = config.leaderElection().healthThreshold()

        return if (age < threshold) {
            HealthCheckResponse.up("leader-election")
        } else {
            HealthCheckResponse.named("leader-election")
                .down()
                .withData("heartbeat_age_seconds", age.seconds)
                .withData("threshold_seconds", threshold.seconds)
                .build()
        }
    }
}
