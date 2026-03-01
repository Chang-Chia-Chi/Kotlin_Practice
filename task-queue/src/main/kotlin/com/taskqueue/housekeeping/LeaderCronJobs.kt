package com.taskqueue.housekeeping

import com.taskqueue.election.LeaderElectionService
import com.taskqueue.queue.TaskQueueDao
import io.quarkus.scheduler.Scheduled
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger

/**
 * Leader-only housekeeping jobs. Every method checks [leaderElection.isLeader] first —
 * if this pod is not the leader, the method returns immediately (no-op).
 *
 * ### Why leader-only?
 * - **Stale reclaim**: Multiple pods reclaiming the same tasks would cause duplicate processing.
 *   A single reclaimer avoids coordination overhead.
 * - **Promote scheduled**: Single promoter avoids redundant UPDATE scans.
 * - **Deadline expiry**: Idempotent (UPDATE with WHERE guard), but running on all pods wastes cycles.
 * - **Purge**: DELETE of old rows — safe to run from one pod only.
 *
 * ### Gap during leader failover
 * If the leader dies, housekeeping pauses for up to `leaseDurationSeconds` (default 15s)
 * until a new leader is elected. This is acceptable because:
 * - Task consumption continues on all pods (consumer is leader-independent).
 * - Stale tasks accumulate for at most 15s beyond their timeout — reclaimed on the next cycle.
 */
@Singleton
class LeaderCronJobs(
    private val dao: TaskQueueDao,
    private val leaderElection: LeaderElectionService,
    private val taskProducer: TaskProducer,
    @ConfigProperty(name = "task.reclaim.stale-minutes", defaultValue = "5")
    private val staleMinutes: Int,
    @ConfigProperty(name = "task.cleanup.retention-days", defaultValue = "7")
    private val retentionDays: Int,
) {

    private val log = Logger.getLogger(LeaderCronJobs::class.java)

    /**
     * Produce root tasks from all registered [TaskProducerJob] beans.
     * This is the entry point for business-logic-driven task creation.
     */
    @Scheduled(cron = "{task.produce.cron}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun produceTasks() {
        taskProducer.produceAll()
    }

    /**
     * Promote RETRYABLE/SCHEDULED tasks to PENDING when their SCHEDULED_AT has arrived.
     * This drives the exponential backoff and snooze mechanisms.
     */
    @Scheduled(every = "{task.promote.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun promoteScheduledTasks() {
        if (!leaderElection.isLeader.value) return

        try {
            val count = dao.promoteScheduledTasks()
            if (count > 0) {
                log.infof("Promoted %d scheduled/retryable task(s) to PENDING", count)
            }
        } catch (e: Exception) {
            log.errorf(e, "Scheduled task promotion failed — will retry next cycle")
        }
    }

    /**
     * Reclaim tasks stuck in PROCESSING due to pod crashes.
     *
     * A task with UPDATED_AT older than [staleMinutes] is assumed abandoned:
     * - Past deadline → EXPIRED
     * - Otherwise → PENDING (eligible for re-consumption)
     *
     * Idempotency: handlers should tolerate replays (see design doc §8.1).
     */
    @Scheduled(every = "{task.reclaim.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun reclaimStaleTasks() {
        if (!leaderElection.isLeader.value) return

        try {
            val count = dao.reclaimStaleTasks(staleMinutes)
            if (count > 0) {
                log.infof("Reclaimed %d stale task(s)", count)
            }
        } catch (e: Exception) {
            log.errorf(e, "Stale reclaim failed — will retry next cycle")
        }
    }

    /**
     * Expire PENDING tasks whose deadline has passed without being claimed.
     */
    @Scheduled(every = "{task.expiry.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun expireOverdueTasks() {
        if (!leaderElection.isLeader.value) return

        try {
            val count = dao.expireOverdueTasks()
            if (count > 0) {
                log.infof("Expired %d overdue task(s)", count)
            }
        } catch (e: Exception) {
            log.errorf(e, "Deadline expiry failed — will retry next cycle")
        }
    }

    /**
     * Purge terminal tasks (DONE/CANCELLED/DISCARDED/EXPIRED) older than retention period.
     * Prevents unbounded table growth.
     */
    @Scheduled(cron = "{task.cleanup.cron}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun purgeOldTasks() {
        if (!leaderElection.isLeader.value) return

        try {
            val count = dao.purgeOldTasks(retentionDays)
            log.infof("Purged %d old task(s) beyond %d-day retention", count, retentionDays)
        } catch (e: Exception) {
            log.errorf(e, "Purge failed — will retry next cycle")
        }
    }

    /**
     * Periodic status summary for monitoring/alerting.
     * Logs counts by status — wire this to your metrics exporter if desired.
     */
    @Scheduled(every = "1m", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    fun logQueueMetrics() {
        if (!leaderElection.isLeader.value) return

        try {
            val counts = dao.countByStatus()
            if (counts.isNotEmpty()) {
                log.infof("Queue status: %s", counts)
            }
        } catch (e: Exception) {
            log.debugf(e, "Metrics query failed — non-critical")
        }
    }
}
