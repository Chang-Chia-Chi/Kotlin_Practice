package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskClaimed
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CancellationException
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
import java.util.concurrent.Semaphore

/**
 * Coroutine-based poll loop with bulkhead-controlled parallelism.
 *
 * Uses two separate scopes to enable graceful shutdown:
 * - [pollScope]: drives the claim loop — exits when [ShutdownCoordinator] enters DRAINING.
 * - [taskScope]: runs in-flight task handlers — drained by the coordinator, then force-cancelled.
 *
 * Each in-flight task has a companion heartbeat coroutine that updates [last_heartbeat]
 * every [HeartbeatConfig.interval] so the stale task reaper can distinguish live tasks
 * from orphaned ones.
 *
 * Total cluster parallelism = pods × bulkhead.
 */
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val dispatcher: TaskDispatcher,
    private val taskRepository: TaskRepository,
    private val circuitBreaker: PodCircuitBreaker,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val taskClaimedEvent: Event<TaskClaimed>,
) {

    private val log = Logger.getLogger(WorkerLoop::class.java)
    private val pollScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val taskScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val bulkheadSize = config.worker().bulkheadSize()
    private val semaphore = Semaphore(bulkheadSize)

    /** Updated on every poll loop iteration — used by health probes to detect a hung worker. */
    @Volatile
    private var _lastPollTimestamp: Instant = Instant.now()
    val lastPollTimestamp: Instant get() = _lastPollTimestamp

    fun onStart(@Observes ev: StartupEvent) {
        val pollInterval = config.worker().pollInterval().toMillis()
        val heartbeatIntervalMs = config.heartbeat().interval().toMillis()
        val workerId = config.worker().id()
        val queues = config.worker().queues()

        // Register the bulkhead with the shutdown coordinator for drain tracking
        shutdownCoordinator.registerBulkhead(semaphore, bulkheadSize)
        shutdownCoordinator.registerMetrics()
        // Cancel scopes on shutdown — pollScope stops claiming, taskScope is force-cancelled
        // after the drain phase completes (Phase 3 releases any remaining tasks via SQL)
        shutdownCoordinator.registerLeaderScopeCallback {
            pollScope.cancel()
            taskScope.cancel()
        }

        log.infof("Worker starting: id=%s, bulkhead=%d, poll=%dms, heartbeat=%dms, queues=%s",
            workerId, bulkheadSize, pollInterval, heartbeatIntervalMs, queues)

        pollScope.launch {
            while (isActive) {
                _lastPollTimestamp = Instant.now()

                // Check coordinator state before every claim attempt
                if (shutdownCoordinator.state != ShutdownState.RUNNING) {
                    log.info("Shutdown signaled, stopping claim loop")
                    break
                }

                if (circuitBreaker.isTripped) {
                    delay(pollInterval)
                    continue
                }

                if (!semaphore.tryAcquire()) {
                    delay(pollInterval)
                    continue
                }

                try {
                    val task = withContext(Dispatchers.IO) { dispatcher.claimTask() }
                    if (task != null) {
                        log.debugf("Claimed task %s [handler=%s, queue=%s]",
                            task.taskId, task.handler, task.queue)
                        try {
                            taskClaimedEvent.fireAsync(TaskClaimed(
                                taskId = task.taskId,
                                handler = task.handler,
                                queue = task.queue,
                                groupId = task.groupId,
                            ))
                        } catch (e: Exception) {
                            log.warnf(e, "Failed to fire TaskClaimed event for task %s", task.taskId)
                        }
                        taskScope.launch {
                            try {
                                // Launch heartbeat alongside handler execution.
                                // The heartbeat updates last_heartbeat periodically so the
                                // stale task reaper can distinguish live tasks from orphaned ones.
                                val heartbeatJob = launch {
                                    while (isActive) {
                                        delay(heartbeatIntervalMs)
                                        try {
                                            taskRepository.updateHeartbeat(
                                                task.taskId, task.executionGeneration,
                                            )
                                        } catch (e: CancellationException) {
                                            throw e
                                        } catch (e: Exception) {
                                            // Heartbeat failure is non-fatal (§3.5).
                                            // Missing one heartbeat is tolerated by the 3× threshold.
                                            log.debugf(
                                                "Heartbeat update failed for task %s (non-fatal)",
                                                task.taskId,
                                            )
                                        }
                                    }
                                }
                                try {
                                    dispatcher.execute(task)
                                } finally {
                                    heartbeatJob.cancel()
                                }
                            } finally {
                                semaphore.release()
                                // Track completions during drain window
                                if (shutdownCoordinator.isShuttingDown) {
                                    shutdownCoordinator.recordDrainCompletion()
                                }
                            }
                        }
                    } else {
                        semaphore.release()
                        delay(pollInterval)
                    }
                } catch (e: CancellationException) {
                    semaphore.release()
                    throw e
                } catch (e: Exception) {
                    semaphore.release()
                    log.errorf(e, "Error in worker claim loop")
                    delay(pollInterval)
                }
            }
        }
    }
}
