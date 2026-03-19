package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownParticipant
import com.mapreduce.shutdown.ShutdownSignal
import com.mapreduce.util.unorderedMapAsync
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
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

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
    private val taskRepository: TaskRepository,
    private val dispatcher: TaskDispatcher,
) : ShutdownParticipant {

    private val log = Logger.getLogger(WorkerLoop::class.java)
    private val _accepting = AtomicBoolean(true)
    private val _inFlightTasks = AtomicInteger(0)
    val inFlightTasks: Int get() = _inFlightTasks.get()

    override val shutdownOrder: Int = 0
    override val shutdownTimeout: Duration get() = config.shutdown().drainTimeout()

    private val scope = CoroutineScope(
        SupervisorJob() + Dispatchers.Default + ShutdownSignal { !_accepting.get() },
    )
    private val bulkheadSize = config.worker().bulkheadSize()

    /** Updated on every poll iteration — used by health probes to detect a hung worker. */
    @Volatile
    private var _lastPollTimestamp: Instant = Instant.now()
    val lastPollTimestamp: Instant get() = _lastPollTimestamp

    override suspend fun shutdown() {
        _accepting.set(false)
        log.infof("Draining %d in-flight task(s)", inFlightTasks)
        val logInterval = config.shutdown().logInterval().toMillis()
        while (inFlightTasks > 0) {
            delay(logInterval)
            if (inFlightTasks > 0) {
                log.infof("Draining: %d task(s) still in-flight", inFlightTasks)
            }
        }
        log.info("All tasks drained")
        releaseTasks()
    }

    private fun releaseTasks() {
        val podId = config.worker().id()
        try {
            val released = taskRepository.releaseTasksByPod(podId)
            if (released > 0) {
                log.infof("Released %d task(s) back to PENDING", released)
            }
        } catch (e: Exception) {
            log.errorf(e, "Failed to release tasks — stale reaper will recover them")
        }
    }

    fun onStart(@Observes ev: StartupEvent) {
        val workerId = config.worker().id()
        val queues = config.worker().queues()
        val pollInterval = config.worker().pollInterval().toMillis()

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
            if (!_accepting.get()) {
                log.info("Shutdown signaled, stopping claim loop")
                return@flow
            }

            try {
                val task = withContext(Dispatchers.IO) {
                    taskRepository.claim(config.worker().id(), config.worker().queues())
                }
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
        _inFlightTasks.incrementAndGet()
        try {
            dispatcher.execute(task)
        } finally {
            _inFlightTasks.decrementAndGet()
        }
    }
}
