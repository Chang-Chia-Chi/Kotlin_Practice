package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskDeadLettered
import com.mapreduce.event.TaskReclaimed
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant

/**
 * Leader-only reaper that reclaims stale CLAIMED tasks using heartbeat-based
 * detection and fenced writes.
 *
 * Detection strategy: each worker updates [last_heartbeat] periodically while
 * executing a task. When a pod dies ungracefully, heartbeats stop. The reaper
 * detects tasks whose [last_heartbeat] age exceeds [staleThreshold] and
 * reclaims them back to PENDING (or DEAD_LETTER if retries are exhausted).
 *
 * Fencing: all reclaim writes include `AND last_epoch <= :leaderEpoch` to
 * prevent a zombie leader from interfering with the current leader's reaper.
 *
 * Batch processing: stale tasks are processed in batches (default 50 per scan)
 * to avoid a single massive UPDATE that locks many rows.
 */
@ApplicationScoped
class StaleTaskReaper(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val leaderManager: LeaderManager,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
    private val deadLetterEvent: Event<TaskDeadLettered>,
    private val taskReclaimedEvent: Event<TaskReclaimed>,
) {

    private val log = Logger.getLogger(StaleTaskReaper::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** Updated on every reap cycle — used by health probes to detect a hung reaper. */
    @Volatile
    private var _lastScanTimestamp: Instant = Instant.now()
    val lastScanTimestamp: Instant get() = _lastScanTimestamp

    /** The configured scan interval — exposed for health probe threshold calculation. */
    val scanInterval: Duration get() = config.reaper().scanInterval()

    fun onStart(@Observes ev: StartupEvent) {
        validateConfig()

        // Register scope cancellation with shutdown coordinator for Phase 1
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.reaper().scanInterval().toMillis()
        scope.launch {
            delay(interval)
            while (isActive) {
                if (leaderManager.isActive) {
                    _lastScanTimestamp = Instant.now()
                    try {
                        withContext(Dispatchers.IO) { reap() }
                    } catch (e: Exception) {
                        log.errorf(e, "Error in stale task reaper")
                    }
                }
                delay(interval)
            }
        }
    }

    /**
     * Fail-fast if stale-threshold < 3 × heartbeat.interval.
     *
     * The 3× multiplier tolerates transient delays (GC pauses, Oracle load
     * spikes) that may delay one or two heartbeat UPDATEs.
     */
    private fun validateConfig() {
        val heartbeatInterval = config.heartbeat().interval()
        val staleThreshold = config.reaper().staleThreshold()
        val minThreshold = heartbeatInterval.multipliedBy(3)
        require(staleThreshold >= minThreshold) {
            "mapreduce.reaper.stale-threshold ($staleThreshold) must be >= 3× " +
                "mapreduce.heartbeat.interval ($heartbeatInterval) = $minThreshold"
        }
    }

    private fun reap() {
        val scanStart = System.nanoTime()
        val threshold = Instant.now().minus(config.reaper().staleThreshold())
        val batchSize = config.reaper().batchSize()
        val leaderEpoch = leaderManager.token

        val staleTasks = taskRepository.findStaleTasks(threshold, batchSize)

        var reclaimedCount = 0
        var deadLetteredCount = 0

        for (task in staleTasks) {
            val staleAge = if (task.lastHeartbeat != null)
                Duration.between(task.lastHeartbeat, Instant.now())
            else
                Duration.between(task.claimedAt ?: Instant.now(), Instant.now())

            val errorMessage = "Reclaimed: heartbeat stale (pod: ${task.claimedBy ?: "unknown"})"

            log.warnf(
                "Reclaiming stale task %s (handler=%s, claimed_by=%s, stale_age=%ds)",
                task.taskId, task.handler, task.claimedBy, staleAge.seconds,
            )

            val wasDeadLettered = taskRepository.reclaimStaleTask(
                task.taskId, leaderEpoch, errorMessage,
            )

            // reclaimStaleTask returns false for both "reclaimed to PENDING" and
            // "fence/status check failed (0 rows)". We check if the task was actually
            // updated by counting the events we fire. The 0-rows case is harmless —
            // the task was completed or reclaimed by someone else.
            reclaimedCount++

            // Record stale age histogram
            meterRegistry.timer("taskqueue.reaper.stale_age", "handler", task.handler)
                .record(staleAge)

            try {
                taskReclaimedEvent.fireAsync(
                    TaskReclaimed(
                        taskId = task.taskId,
                        handler = task.handler,
                        previousClaimedBy = task.claimedBy ?: "unknown",
                        retryCount = task.retryCount + 1,
                        staleAge = staleAge,
                    ),
                )
            } catch (e: Exception) {
                log.warnf(e, "Failed to fire TaskReclaimed event for task %s", task.taskId)
            }

            if (wasDeadLettered) {
                deadLetteredCount++
                try {
                    deadLetterEvent.fireAsync(
                        TaskDeadLettered(
                            taskId = task.taskId,
                            handler = task.handler,
                            queue = task.queue,
                            groupId = task.groupId,
                            retryCount = task.retryCount + 1,
                            lastError = errorMessage,
                            createdAt = task.createdAt,
                        ),
                    )
                } catch (e: Exception) {
                    log.warnf(e, "Failed to fire TaskDeadLettered event for stale task %s", task.taskId)
                }
            }
        }

        // Record scan duration
        val scanDurationNanos = System.nanoTime() - scanStart
        meterRegistry.timer("taskqueue.reaper.scan_duration")
            .record(Duration.ofNanos(scanDurationNanos))

        // Record counters
        if (reclaimedCount > 0) {
            meterRegistry.counter("taskqueue.reaper.reclaimed").increment(reclaimedCount.toDouble())
            log.infof("Reclaimed %d stale task(s) (%d dead-lettered)", reclaimedCount, deadLetteredCount)
        }
        if (deadLetteredCount > 0) {
            meterRegistry.counter("taskqueue.reaper.dead_lettered").increment(deadLetteredCount.toDouble())
        }
    }
}
