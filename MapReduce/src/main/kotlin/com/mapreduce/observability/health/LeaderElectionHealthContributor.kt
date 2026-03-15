package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.time.Instant

/**
 * Health contributor for the leader election thread.
 *
 * - **Liveness:** Is the election thread alive? Compares [LeaderManager.lastHeartbeat]
 *   against `renewDeadline + retryPeriod`. In dev mode (no K8s), always UP.
 * - **Readiness:** null by default. When `leader-readiness-enabled` is true,
 *   returns DOWN if this pod is not the leader.
 */
@ApplicationScoped
class LeaderElectionHealthContributor(
    private val leaderManager: LeaderManager,
    private val config: FrameworkConfig,
) : HealthContributor {

    override val name: String = "leader-election"

    override fun liveness(): ProbeResult {
        // In dev mode, the election thread doesn't run — always healthy
        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            return ProbeResult(
                status = HealthStatus.UP,
                details = mapOf("mode" to "dev"),
            )
        }

        val leaderCfg = config.leaderElection()
        val threshold = leaderCfg.renewDeadline().plus(leaderCfg.retryPeriod())
        val elapsed = Duration.between(leaderManager.lastHeartbeat, Instant.now())

        return if (elapsed <= threshold) {
            ProbeResult(
                status = HealthStatus.UP,
                details = mapOf(
                    "heartbeatAge" to elapsed.seconds,
                    "epoch" to leaderManager.token,
                ),
            )
        } else {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "heartbeatAge" to elapsed.seconds,
                    "threshold" to threshold.seconds,
                    "reason" to "Election loop heartbeat stale",
                ),
            )
        }
    }

    override fun readiness(): ProbeResult? {
        if (!config.health().leaderReadinessEnabled()) return null

        return if (leaderManager.isActive) {
            ProbeResult(
                status = HealthStatus.UP,
                details = mapOf("isLeader" to true, "epoch" to leaderManager.token),
            )
        } else {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "isLeader" to false,
                    "reason" to "Pod is not leader and leader-readiness is enabled",
                ),
            )
        }
    }
}
