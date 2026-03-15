package com.mapreduce.observability.health

import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import jakarta.enterprise.context.ApplicationScoped

/**
 * Health contributor for the shutdown coordinator.
 *
 * - **Liveness:** null (shutdown doesn't mean the pod is dead).
 * - **Readiness:** DOWN when the pod is shutting down (DRAINING, RELEASING, or TERMINATED).
 *   K8s removes the pod from Service endpoints so it stops receiving new traffic.
 */
@ApplicationScoped
class ShutdownHealthContributor(
    private val shutdownCoordinator: ShutdownCoordinator,
) : HealthContributor {

    override val name: String = "shutdown"

    override fun liveness(): ProbeResult? = null

    override fun readiness(): ProbeResult {
        val state = shutdownCoordinator.state
        return if (state == ShutdownState.RUNNING) {
            ProbeResult(status = HealthStatus.UP)
        } else {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "state" to state.name,
                    "inFlightTasks" to shutdownCoordinator.inFlightTasks,
                    "drainDeadline" to (shutdownCoordinator.drainDeadline?.toString() ?: "N/A"),
                ),
            )
        }
    }
}
