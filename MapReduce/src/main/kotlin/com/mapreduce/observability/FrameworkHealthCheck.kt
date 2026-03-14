package com.mapreduce.observability

import com.mapreduce.leader.LeaderElection
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.eclipse.microprofile.health.Readiness
import org.jdbi.v3.core.Jdbi

@Liveness
@Readiness
@ApplicationScoped
class FrameworkHealthCheck(
    private val jdbi: Jdbi,
    private val leaderElection: LeaderElection,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("mapreduce-framework")
        return try {
            jdbi.withHandle<Int, Exception> { h ->
                h.createQuery("SELECT 1 FROM DUAL").mapTo(Int::class.java).one()
            }
            builder
                .withData("database", "connected")
                .withData("leader", leaderElection.isLeader.toString())
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
