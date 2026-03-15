package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.shutdown.ShutdownCoordinator
import jakarta.enterprise.inject.Instance
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response

/**
 * Diagnostic endpoint for per-subsystem health breakdown.
 *
 * Returns a detailed JSON document at `/q/health/detail` with the full
 * per-contributor liveness/readiness status and details maps. This is
 * not used by K8s probes — it's for dashboards, runbooks, and incident response.
 */
@Path("/q/health/detail")
class HealthDetailResource(
    private val contributors: Instance<HealthContributor>,
    private val config: FrameworkConfig,
    private val leaderManager: LeaderManager,
    private val shutdownCoordinator: ShutdownCoordinator,
) {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    fun detail(): Response {
        if (!config.health().detailEndpointEnabled()) {
            return Response.status(Response.Status.NOT_FOUND).build()
        }

        val livenessChecks = mutableMapOf<String, Any>()
        val readinessChecks = mutableMapOf<String, Any>()
        var overallLiveness = HealthStatus.UP
        var overallReadiness = HealthStatus.UP

        for (contributor in contributors) {
            val live = contributor.liveness()
            if (live != null) {
                livenessChecks[contributor.name] = buildCheckEntry(live)
                if (live.status == HealthStatus.DOWN) overallLiveness = HealthStatus.DOWN
                else if (live.status == HealthStatus.DEGRADED && overallLiveness == HealthStatus.UP) {
                    overallLiveness = HealthStatus.DEGRADED
                }
            }

            val ready = contributor.readiness()
            if (ready != null) {
                readinessChecks[contributor.name] = buildCheckEntry(ready)
                if (ready.status == HealthStatus.DOWN) overallReadiness = HealthStatus.DOWN
                else if (ready.status == HealthStatus.DEGRADED && overallReadiness == HealthStatus.UP) {
                    overallReadiness = HealthStatus.DEGRADED
                }
            }
        }

        val result = linkedMapOf<String, Any>(
            "pod" to config.worker().id(),
            "isLeader" to leaderManager.isActive,
            "epoch" to leaderManager.token,
            "shutdownState" to shutdownCoordinator.state.name,
            "liveness" to linkedMapOf(
                "status" to overallLiveness.name,
                "checks" to livenessChecks,
            ),
            "readiness" to linkedMapOf(
                "status" to overallReadiness.name,
                "checks" to readinessChecks,
            ),
            "bulkhead" to linkedMapOf(
                "limit" to shutdownCoordinator.bulkheadSize,
                "active" to shutdownCoordinator.inFlightTasks,
                "idle" to (shutdownCoordinator.bulkheadSize - shutdownCoordinator.inFlightTasks),
            ),
        )

        return Response.ok(result).build()
    }

    private fun buildCheckEntry(probe: ProbeResult): Map<String, Any> {
        val entry = linkedMapOf<String, Any>("status" to probe.status.name)
        entry.putAll(probe.details)
        return entry
    }
}
