package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import com.mapreduce.queue.model.Task
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
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
 * Each in-flight task has a companion heartbeat coroutine that updates [last_heartbeat]
 * every [HeartbeatConfig.interval] so the stale task reaper can distinguish live tasks
 * from orphaned ones.
 */
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val dispatcher: TaskDispatcher,
    private val taskRepository: TaskRepository,
    private val circuitBreaker: PodCircuitBreaker,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
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

        shutdownCoordinator.registerBulkhead(semaphore, bulkheadSize)
        shutdownCoordinator.registerMetrics()
        meterRegistry.gauge(
            "framework.worker.bulkhead.utilization",
            listOf(Tag.of("pod_id", workerId)),
            shutdownCoordinator,
        ) { coordinator ->
            val size = coordinator.bulkheadSize
            if (size == 0) 0.0 else coordinator.inFlightTasks.toDouble() / size
        }
        shutdownCoordinator.registerLeaderScopeCallback {
            pollScope.cancel()
            taskScope.cancel()
        }

        log.infof("Worker starting: id=%s, bulkhead=%d, poll=%dms, heartbeat=%dms, queues=%s",
            workerId, bulkheadSize, pollInterval, heartbeatIntervalMs, queues)

        pollScope.launch {
            while (isActive) {
                _lastPollTimestamp = Instant.now()

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
                        taskScope.launch { executeWithHeartbeat(task, heartbeatIntervalMs) }
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

    /**
     * Runs a claimed task with a companion heartbeat coroutine.
     * Semaphore is released in finally to guarantee bulkhead slot is freed.
     */
    private suspend fun CoroutineScope.executeWithHeartbeat(task: Task, heartbeatIntervalMs: Long) {
        try {
            val heartbeatJob = launchHeartbeat(task, heartbeatIntervalMs)
            try {
                dispatcher.execute(task)
            } finally {
                heartbeatJob.cancel()
            }
        } finally {
            semaphore.release()
            if (shutdownCoordinator.isShuttingDown) {
                shutdownCoordinator.recordDrainCompletion()
            }
        }
    }

    private fun CoroutineScope.launchHeartbeat(task: Task, intervalMs: Long): Job = launch {
        while (isActive) {
            delay(intervalMs)
            try {
                taskRepository.updateHeartbeat(task.taskId, task.executionGeneration)
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                log.debugf("Heartbeat update failed for task %s (non-fatal)", task.taskId)
            }
        }
    }
}
