package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import java.time.Duration
import java.time.Instant

@Liveness
@ApplicationScoped
class LeaderElectionHealthContributor(
    private val leaderManager: LeaderManager,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("leader-election")

        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            return builder.up().withData("mode", "dev").build()
        }

        val leaderCfg = config.leaderElection()
        val threshold = leaderCfg.renewDeadline().plus(leaderCfg.retryPeriod())
        val elapsed = Duration.between(leaderManager.lastHeartbeat, Instant.now())

        return if (elapsed <= threshold) {
            builder.up()
                .withData("heartbeatAge", elapsed.seconds.toString())
                .withData("epoch", leaderManager.token.toString())
                .build()
        } else {
            builder.down()
                .withData("heartbeatAge", elapsed.seconds.toString())
                .withData("threshold", threshold.seconds.toString())
                .withData("reason", "Election loop heartbeat stale")
                .build()
        }
    }
}
