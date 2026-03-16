package com.mapreduce.observability.health

import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Readiness

@Readiness
@ApplicationScoped
class ShutdownHealthContributor(
    private val shutdownCoordinator: ShutdownCoordinator,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("shutdown")
        val state = shutdownCoordinator.state
        return if (state == ShutdownState.RUNNING) {
            builder.up().build()
        } else {
            builder.down()
                .withData("state", state.name)
                .withData("inFlightTasks", shutdownCoordinator.inFlightTasks.toString())
                .withData("drainDeadline", shutdownCoordinator.drainDeadline?.toString() ?: "N/A")
                .build()
        }
    }
}
