package com.mapreduce.observability

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.worker.PodCircuitBreaker
import com.mapreduce.shutdown.ShutdownCoordinator
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.eclipse.microprofile.health.Readiness
import org.jdbi.v3.core.Jdbi
import java.time.Duration
import java.time.Instant

/**
 * Liveness probe: checks database connectivity AND election thread health.
 *
 * The election loop updates [LeaderManager.lastHeartbeat] on every iteration.
 * If the heartbeat is stale (exceeds renewDeadline + retryPeriod), the election
 * thread is likely dead (hung, deadlocked, or crashed). K8s should restart the pod.
 */
@Liveness
@ApplicationScoped
class LivenessCheck(
    private val jdbi: Jdbi,
    private val leaderManager: LeaderManager,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("mapreduce-liveness")

        // Check database connectivity
        val dbHealthy = try {
            jdbi.withHandle<Int, Exception> { h ->
                h.createQuery("SELECT 1 FROM DUAL").mapTo(Int::class.java).one()
            }
            true
        } catch (e: Exception) {
            builder.withData("database", "disconnected")
                .withData("error", e.message ?: "unknown")
            false
        }

        if (dbHealthy) {
            builder.withData("database", "connected")
        }

        // Check election thread liveness (only in K8s mode)
        val electionHealthy = if (System.getenv("KUBERNETES_SERVICE_HOST") != null) {
            val leaderCfg = config.leaderElection()
            val heartbeatThreshold = leaderCfg.renewDeadline().plus(leaderCfg.retryPeriod())
            val lastHeartbeat = leaderManager.lastHeartbeat
            val elapsed = Duration.between(lastHeartbeat, Instant.now())

            builder.withData("election_last_heartbeat", lastHeartbeat.toString())
                .withData("election_heartbeat_age_ms", elapsed.toMillis().toString())

            if (elapsed > heartbeatThreshold) {
                builder.withData("election_thread", "stale")
                false
            } else {
                builder.withData("election_thread", "alive")
                true
            }
        } else {
            builder.withData("election_thread", "dev-mode")
            true
        }

        return if (dbHealthy && electionHealthy) {
            builder.up().build()
        } else {
            builder.down().build()
        }
    }
}

/**
 * Readiness probe: database + circuit breaker + leader status.
 */
@Readiness
@ApplicationScoped
class ReadinessCheck(
    private val jdbi: Jdbi,
    private val leaderManager: LeaderManager,
    private val circuitBreaker: PodCircuitBreaker,
    private val shutdownCoordinator: ShutdownCoordinator,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("mapreduce-readiness")

        // During shutdown, return 503 so K8s removes pod from Service endpoints
        if (shutdownCoordinator.isShuttingDown) {
            return builder
                .withData("shutdown_state", shutdownCoordinator.state.name)
                .withData("in_flight_tasks", shutdownCoordinator.inFlightTasks.toString())
                .withData("drain_deadline", shutdownCoordinator.drainDeadline?.toString() ?: "N/A")
                .down()
                .build()
        }

        if (circuitBreaker.isTripped) {
            return builder
                .withData("circuit_breaker", "TRIPPED")
                .withData("reason", "Consecutive failure threshold exceeded — pod quarantined")
                .down()
                .build()
        }

        return try {
            jdbi.withHandle<Int, Exception> { h ->
                h.createQuery("SELECT 1 FROM DUAL").mapTo(Int::class.java).one()
            }
            builder
                .withData("database", "connected")
                .withData("leader", leaderManager.isActive.toString())
                .withData("epoch", leaderManager.token.toString())
                .withData("circuit_breaker", "CLOSED")
                .up()
                .build()
        } catch (e: Exception) {
            builder
                .withData("database", "disconnected")
                .withData("error", e.message ?: "unknown")
                .down()
                .build()
        }
    }
}
