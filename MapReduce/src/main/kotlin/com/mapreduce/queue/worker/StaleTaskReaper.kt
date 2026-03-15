package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
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
) {

    private val log = Logger.getLogger(StaleTaskReaper::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun onStart(@Observes ev: StartupEvent) {
        // Register scope cancellation with shutdown coordinator for Phase 1
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.leader().monitorInterval().toMillis()
        scope.launch {
            delay(interval)
            while (isActive) {
                if (leaderManager.isActive) {
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
            taskRepository.reclaimStaleTask(task.taskId)
        }
        if (staleTasks.isNotEmpty()) {
            log.infof("Reclaimed %d stale task(s)", staleTasks.size)
        }
    }
}
