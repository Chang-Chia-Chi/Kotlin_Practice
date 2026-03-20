package com.mapreduce.queue.reaper

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.NotLeader
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant

/**
 * Leader-only reaper that reclaims stale CLAIMED tasks using
 * [claimed_at]-based detection and fenced writes.
 *
 * A task is considered stale when it has been CLAIMED longer than
 * the configured [stale-threshold]. This threshold should exceed
 * the maximum expected task execution time (pipeline timeout).
 */
@ApplicationScoped
class StaleTaskReaper(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val workflowStepRepository: WorkflowStepRepository,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(StaleTaskReaper::class.java)

    /** Updated on every reap cycle — used by health probes to detect a hung reaper. */
    @Volatile
    private var _lastScanTimestamp: Instant = Instant.now()
    val lastScanTimestamp: Instant get() = _lastScanTimestamp

    /** The configured scan interval — exposed for health probe threshold calculation. */
    val scanInterval: Duration get() = config.reaper().scanInterval()

    @Scheduled(
        every = "{taskqueue.reaper.scan-interval}",
        delayed = "{taskqueue.reaper.scan-interval}",
        concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
        skipExecutionIf = NotLeader::class,
    )
    fun reap() {
        _lastScanTimestamp = Instant.now()

        val scanStart = System.nanoTime()
        val threshold = Instant.now().minus(config.reaper().staleThreshold())
        val batchSize = config.reaper().batchSize()

        val staleTasks = taskRepository.findStaleTasks(threshold, batchSize)

        var reclaimedCount = 0
        var deadLetteredCount = 0

        for (task in staleTasks) {
            val staleAge = Duration.between(task.claimedAt ?: Instant.now(), Instant.now())
            val errorMessage = "Reclaimed: task stale (pod: ${task.claimedBy ?: "unknown"})"

            val deadLettered: Boolean? = if (task.stepId != null) {
                workflowStepRepository.reclaimStepTask(task.taskId, task.stepId, errorMessage)?.deadLettered
            } else {
                taskRepository.reclaimStaleTask(task.taskId, errorMessage)
            }

            if (deadLettered == null) {
                log.debugf("Skipped stale task %s — already handled", task.taskId)
                continue
            }

            reclaimedCount++
            log.warnf(
                "Reclaimed stale task %s (handler=%s, claimed_by=%s, stale_age=%ds)",
                task.taskId, task.handler, task.claimedBy, staleAge.seconds,
            )

            meterRegistry.timer("taskqueue.reaper.stale_age", "handler", task.handler)
                .record(staleAge)

            if (deadLettered) {
                deadLetteredCount++
            }
        }

        // Fail ACTIVE steps that have exceeded their deadline
        val expiredCount = reapExpiredSteps()

        val scanDurationNanos = System.nanoTime() - scanStart
        meterRegistry.timer("taskqueue.reaper.scan_duration")
            .record(Duration.ofNanos(scanDurationNanos))

        if (reclaimedCount > 0) {
            meterRegistry.counter("taskqueue.reaper.reclaimed").increment(reclaimedCount.toDouble())
            log.infof("Reclaimed %d stale task(s) (%d dead-lettered)", reclaimedCount, deadLetteredCount)
        }
        if (deadLetteredCount > 0) {
            meterRegistry.counter("taskqueue.reaper.dead_lettered").increment(deadLetteredCount.toDouble())
        }
        if (expiredCount > 0) {
            meterRegistry.counter("taskqueue.reaper.steps_expired").increment(expiredCount.toDouble())
        }
    }

    /**
     * Bulk-fail ACTIVE steps whose [deadline_at] has passed.
     * This prevents steps from stalling indefinitely when tasks are perpetually
     * requeued without consuming retries (e.g., repeated shutdown-aware timeouts).
     */
    private fun reapExpiredSteps(): Int {
        val count = workflowStepRepository.failExpiredSteps(Instant.now())
        if (count > 0) {
            log.warnf("Failed %d step(s) past their deadline", count)
        }
        return count
    }
}
