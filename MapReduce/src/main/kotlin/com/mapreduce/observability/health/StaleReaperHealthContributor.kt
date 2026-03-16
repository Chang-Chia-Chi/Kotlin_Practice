package com.mapreduce.observability.health

import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.worker.StaleTaskReaper
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import java.time.Duration
import java.time.Instant

@Liveness
@ApplicationScoped
class StaleReaperHealthContributor(
    private val staleTaskReaper: StaleTaskReaper,
    private val leaderManager: LeaderManager,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("stale-reaper")
        if (!leaderManager.isActive) {
            return builder.up().withData("mode", "not-leader").build()
        }

        val scanInterval = staleTaskReaper.scanInterval
        val threshold = scanInterval.multipliedBy(3)
        val elapsed = Duration.between(staleTaskReaper.lastScanTimestamp, Instant.now())

        return if (elapsed <= threshold) {
            builder.up()
                .withData("lastScanAge", elapsed.seconds.toString())
                .build()
        } else {
            builder.down()
                .withData("lastScanAge", elapsed.seconds.toString())
                .withData("threshold", threshold.seconds.toString())
                .withData("reason", "Reaper hasn't scanned in ${elapsed.seconds}s (threshold ${threshold.seconds}s)")
                .build()
        }
    }
}
