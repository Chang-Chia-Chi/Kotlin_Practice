package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
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
 */
@ApplicationScoped
class StaleTaskReaper(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val leaderManager: LeaderManager,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
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
