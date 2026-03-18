package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import com.mapreduce.util.unorderedMapAsync
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.time.Instant
import java.util.concurrent.Semaphore

/**
 * Flow-based poll loop with bulkhead-controlled parallelism.
 *
 * The pipeline is structured as:
 * ```
 * claimFlow()                           // single-threaded: poll DB, backoff on empty/error
 *   .onEach { update health timestamp }
 *   .unorderedMapAsync(bulkheadSize)    // fan-out: bounded concurrent execution
 *   .onCompletion { log }
 *   .collect {}
 * ```
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
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val bulkheadSize = config.worker().bulkheadSize()
    private val semaphore = Semaphore(bulkheadSize)

    /** Updated on every poll iteration — used by health probes to detect a hung worker. */
    @Volatile
    private var _lastPollTimestamp: Instant = Instant.now()
    val lastPollTimestamp: Instant get() = _lastPollTimestamp

    fun onStart(@Observes ev: StartupEvent) {
        val workerId = config.worker().id()
        val queues = config.worker().queues()
        val pollInterval = config.worker().pollInterval().toMillis()

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

        scope.launch {
            claimFlow(pollInterval)
                .onEach { _lastPollTimestamp = Instant.now() }
                .unorderedMapAsync(bulkheadSize) { task -> executeTask(task) }
                .onCompletion { log.info("Worker loop terminated") }
                .collect {}
        }
    }

    /**
     * Single-threaded claim flow. Emits claimed [Task] items; handles
     * backoff on empty poll or transient errors. Terminates on shutdown.
     */
    private fun claimFlow(pollIntervalMs: Long): Flow<com.mapreduce.queue.model.Task> = flow {
        while (true) {
            if (shutdownCoordinator.state != ShutdownState.RUNNING) {
                log.info("Shutdown signaled, stopping claim loop")
                return@flow
            }

            try {
                val task = withContext(Dispatchers.IO) { dispatcher.claimTask() }
                if (task != null) {
                    log.debugf("Claimed task %s [handler=%s, queue=%s]",
                        task.taskId, task.handler, task.queue)
                    emit(task)
                } else {
                    delay(pollIntervalMs)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.errorf(e, "Error in worker claim loop")
                delay(pollIntervalMs)
            }
        }
    }

    private suspend fun executeTask(task: com.mapreduce.queue.model.Task) {
        semaphore.acquire() // track in-flight count for shutdown drain + metrics
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
