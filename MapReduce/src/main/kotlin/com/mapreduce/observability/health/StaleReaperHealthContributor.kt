package com.mapreduce.observability.health

import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.worker.StaleTaskReaper
import jakarta.enterprise.context.ApplicationScoped
import java.time.Duration
import java.time.Instant

/**
 * Health contributor for the stale task reaper (leader only).
 *
 * - **Liveness:** Is the reaper coroutine alive? Compares [StaleTaskReaper.lastScanTimestamp]
 *   against 3× scan interval. Only checked when this pod is the leader.
 *   Returns null for both dimensions if not the leader.
 * - **Readiness:** null (the reaper is not required for readiness).
 */
@ApplicationScoped
class StaleReaperHealthContributor(
    private val staleTaskReaper: StaleTaskReaper,
    private val leaderManager: LeaderManager,
) : HealthContributor {

    override val name: String = "stale-reaper"

    override fun liveness(): ProbeResult? {
        if (!leaderManager.isActive) return null

        val scanInterval = staleTaskReaper.scanInterval
        val threshold = scanInterval.multipliedBy(3)
        val elapsed = Duration.between(staleTaskReaper.lastScanTimestamp, Instant.now())

        return if (elapsed <= threshold) {
            ProbeResult(
                status = HealthStatus.UP,
                details = mapOf("lastScanAge" to elapsed.seconds),
            )
        } else {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "lastScanAge" to elapsed.seconds,
                    "threshold" to threshold.seconds,
                    "reason" to "Reaper hasn't scanned in ${elapsed.seconds}s (threshold ${threshold.seconds}s)",
                ),
            )
        }
    }

    override fun readiness(): ProbeResult? = null
}
