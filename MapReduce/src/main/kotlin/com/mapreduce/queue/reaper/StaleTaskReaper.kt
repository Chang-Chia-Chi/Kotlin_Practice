package com.mapreduce.queue.reaper

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.leader.NotLeader
import com.mapreduce.queue.repository.TaskGroupRepository
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
    private val taskGroupRepository: TaskGroupRepository,
    private val leaderManager: LeaderManager,
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
        val leaderEpoch = leaderManager.token

        val staleTasks = taskRepository.findStaleTasks(threshold, batchSize)

        var reclaimedCount = 0
        var deadLetteredCount = 0

        for (task in staleTasks) {
            val staleAge = Duration.between(task.claimedAt ?: Instant.now(), Instant.now())
            val errorMessage = "Reclaimed: task stale (pod: ${task.claimedBy ?: "unknown"})"

            val result = taskRepository.reclaimStaleTask(
                task.taskId, leaderEpoch, errorMessage,
            )

            if (result == null) {
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

            if (result) {
                deadLetteredCount++
                if (task.groupId != null) {
                    taskGroupRepository.resolveGroupTask(groupId = task.groupId, failed = true)
                }
            }
        }

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
    }
}
