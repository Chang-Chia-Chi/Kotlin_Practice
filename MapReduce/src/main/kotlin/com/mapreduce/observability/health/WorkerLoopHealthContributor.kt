package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.worker.WorkerLoop
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import java.time.Duration
import java.time.Instant

@Liveness
@ApplicationScoped
class WorkerLoopHealthContributor(
    private val workerLoop: WorkerLoop,
    private val handlerRegistry: HandlerRegistry,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("worker-loop")
        val threshold = config.health().workerLoopStaleThreshold()
        val elapsed = Duration.between(workerLoop.lastPollTimestamp, Instant.now())

        return if (elapsed <= threshold) {
            builder.up()
                .withData("lastPollAge", elapsed.seconds.toString())
                .withData("handlers", handlerRegistry.registeredHandlers().size.toString())
                .build()
        } else {
            builder.down()
                .withData("lastPollAge", elapsed.seconds.toString())
                .withData("threshold", threshold.seconds.toString())
                .withData("reason", "Claim coroutine hasn't polled in ${elapsed.seconds}s")
                .build()
        }
    }
}
