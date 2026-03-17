package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
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
import kotlinx.coroutines.SupervisorJob
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
 * Stale task detection relies on [claimed_at] age — the leader-only
 * [StaleTaskReaper] reclaims tasks that have been CLAIMED longer than
 * the configured stale threshold.
 */
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val dispatcher: TaskDispatcher,
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
        log.infof("Worker starting: id=%s, bulkhead=%d, poll=%dms, queues=%s",
            workerId, bulkheadSize, pollInterval, queues)

        pollScope.launch {
            while (isActive) {
                _lastPollTimestamp = Instant.now()

                if (shutdownCoordinator.state != ShutdownState.RUNNING) {
                    log.info("Shutdown signaled, stopping claim loop")
                    break
                }

                if (!semaphore.tryAcquire()) {
                    delay(pollInterval)
                    continue
                }

                try {
                    // Re-check after acquiring semaphore — narrows the race window
                    // between the top-of-loop check and the actual claim call
                    if (shutdownCoordinator.isShuttingDown) {
                        semaphore.release()
                        break
                    }
                    val task = withContext(Dispatchers.IO) { dispatcher.claimTask() }
                    if (task != null) {
                        log.debugf("Claimed task %s [handler=%s, queue=%s]",
                            task.taskId, task.handler, task.queue)
                        taskScope.launch { executeTask(task) }
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

    private suspend fun executeTask(task: Task) {
        try {
            dispatcher.execute(task)
        } finally {
            semaphore.release()
            if (shutdownCoordinator.isShuttingDown) {
                shutdownCoordinator.recordDrainCompletion()
            }
        }
    }
}
