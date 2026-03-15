package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskDeadLettered
import com.mapreduce.event.TaskReclaimed
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
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
 * Leader-only reaper that reclaims stale CLAIMED tasks.
 *
 * Tasks stay CLAIMED when a pod crashes mid-execution. The reaper detects
 * these via [FrameworkConfig.WorkerConfig.staleThreshold] and flips them
 * back to PENDING (or DEAD_LETTER if retries are exhausted).
 */
@ApplicationScoped
class StaleTaskReaper(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val leaderManager: LeaderManager,
    private val shutdownCoordinator: ShutdownCoordinator,
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
    val scanInterval: Duration get() = config.leader().monitorInterval()

    fun onStart(@Observes ev: StartupEvent) {
        // Register scope cancellation with shutdown coordinator for Phase 1
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.leader().monitorInterval().toMillis()
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

    private fun reap() {
        val threshold = Instant.now().minus(config.worker().staleThreshold())
        val staleTasks = taskRepository.findStaleTasks(threshold)
        for (task in staleTasks) {
            log.warnf("Reclaiming stale task %s (handler=%s, claimed_by=%s)",
                task.taskId, task.handler, task.claimedBy)
            val wasDeadLettered = taskRepository.reclaimStaleTask(task.taskId)
            try {
                taskReclaimedEvent.fireAsync(TaskReclaimed(
                    taskId = task.taskId,
                    handler = task.handler,
                    previousClaimedBy = task.claimedBy ?: "unknown",
                    retryCount = task.retryCount + 1,
                ))
            } catch (e: Exception) {
                log.warnf(e, "Failed to fire TaskReclaimed event for task %s", task.taskId)
            }
            if (wasDeadLettered) {
                try {
                    deadLetterEvent.fireAsync(
                        TaskDeadLettered(
                            taskId = task.taskId,
                            handler = task.handler,
                            queue = task.queue,
                            groupId = task.groupId,
                            retryCount = task.retryCount + 1,
                            lastError = task.errorMessage ?: "Stale reclaim exhausted retries",
                            createdAt = task.createdAt,
                        ),
                    )
                } catch (e: Exception) {
                    log.warnf(e, "Failed to fire TaskDeadLettered event for stale task %s", task.taskId)
                }
            }
        }
        if (staleTasks.isNotEmpty()) {
            log.infof("Reclaimed %d stale task(s)", staleTasks.size)
        }
    }
}
