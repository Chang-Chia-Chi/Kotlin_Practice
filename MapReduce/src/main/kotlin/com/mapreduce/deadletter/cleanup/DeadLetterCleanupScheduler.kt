package com.mapreduce.deadletter.cleanup

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
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

/**
 * Leader-only scheduler that enqueues the dead-letter cleanup task periodically.
 *
 * The cleanup task itself is a normal task on the queue with handler
 * [DeadLetterCleanupHandler.HANDLER_NAME]. This scheduler simply ensures
 * the task is enqueued at the configured interval.
 */
@ApplicationScoped
class DeadLetterCleanupScheduler(
    private val config: FrameworkConfig,
    private val leaderManager: LeaderManager,
    private val taskRepository: TaskRepository,
    private val shutdownCoordinator: ShutdownCoordinator,
) {

    private val log = Logger.getLogger(DeadLetterCleanupScheduler::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun onStart(@Observes ev: StartupEvent) {
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val intervalHours = config.deadLetter().cleanupScheduleHours()
        val intervalMs = intervalHours * 3600 * 1000L

        scope.launch {
            delay(intervalMs) // initial delay — skip immediately at startup
            while (isActive) {
                if (leaderManager.isActive) {
                    try {
                        withContext(Dispatchers.IO) { enqueueCleanup() }
                    } catch (e: Exception) {
                        log.warnf(e, "Failed to enqueue dead-letter cleanup task")
                    }
                }
                delay(intervalMs)
            }
        }
    }

    private fun enqueueCleanup() {
        taskRepository.enqueue(
            EnqueueRequest(
                handler = DeadLetterCleanupHandler.HANDLER_NAME,
                payload = "{}",
                queue = "default",
                maxRetries = 3,
            ),
        )
        log.info("Enqueued dead-letter cleanup task")
    }
}
