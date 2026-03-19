package com.mapreduce.observability.health

import com.mapreduce.queue.worker.WorkerLoop
import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Readiness

// TODO: won't health check of shutdown coordinator cause early exit?
@Readiness
@ApplicationScoped
class ShutdownHealthContributor(
    private val shutdownCoordinator: ShutdownCoordinator,
    private val workerLoop: WorkerLoop,
) : HealthCheck {
    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("shutdown")
        val state = shutdownCoordinator.state
        return if (state == ShutdownState.RUNNING) {
            builder.up().build()
        } else {
            builder
                .down()
                .withData("state", state.name)
                .withData("inFlightTasks", workerLoop.inFlightTasks.toString())
                .withData("drainDeadline", shutdownCoordinator.drainDeadline?.toString() ?: "N/A")
                .build()
        }
    }
}
