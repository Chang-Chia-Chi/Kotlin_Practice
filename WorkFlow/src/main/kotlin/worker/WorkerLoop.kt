package com.workflow.worker

import com.workflow.config.FrameworkConfig
import com.workflow.engine.BarrierService
import com.workflow.engine.Task
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.extension.unorderedMapAsync
import com.workflow.shutdown.ShutdownParticipant
import com.workflow.shutdown.ShutdownSignal
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Workers drain at order 10, after the leader (order 1) stops sweeper
 * patrols. This ensures in-flight work completes without sweeper
 * interference.
 */
const val SHUTDOWN_ORDER_WORKER = 10

/**
 * Poll loop that claims PENDING tasks, executes handlers, and feeds results
 * into the barrier for workflow progression.
 *
 * **Pipeline:**
 * ```
 * claimFlow(pollIntervalMs)          // updates _lastPollTimestamp each iteration
 *   .unorderedMapAsync(concurrency) { processTask(it) }
 *   .onCompletion { log }
 *   .collect {}
 * ```
 *
 * **Lifecycle:**
 * - Production: auto-starts via [onStart] observing [StartupEvent].
 * - Tests: call [start] with a test-controlled [CoroutineScope].
 * - Shutdown: [shutdown] sets [_accepting] to false; [claimFlow] checks
 *   this flag each iteration and returns, terminating the pipeline
 *   naturally. [shutdown] then joins the active job, waiting for
 *   in-flight [processTask] calls to complete.
 *
 * **Error contract:** The [processTask] transform catches ALL non-cancellation
 * exceptions from handler execution and reports them to
 * [BarrierService.onTaskCompleted] with [TaskStatus.FAILED] BEFORE returning.
 * No exception escapes the transform.
 *
 * **Retry semantics:** On handler failure, if `task.retryCount < task.maxRetries`,
 * the task is atomically reset to PENDING via [TaskRepository.resetForRetry]
 * (clears claim, increments retry count). The barrier is NOT called -- the
 * task re-enters the claimable pool. If retries are exhausted, the task is
 * marked FAILED and the barrier fires.
 *
 * **Deadline reaper:** A separate coroutine runs alongside the main loop.
 * It polls [TaskRepository.findExpired] at [pollInterval] intervals,
 * marking expired PROCESSING tasks as FAILED via the barrier. Uses
 * cooperative shutdown via the same [_accepting] flag.
 */
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val taskRepo: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val barrierService: BarrierService,
) : ShutdownParticipant {
    private val log = LoggerFactory.getLogger(WorkerLoop::class.java)

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val _accepting = AtomicBoolean(true)
    private val _inFlightTasks = AtomicInteger(0)
    val inFlightTasks: Int get() = _inFlightTasks.get()

    @Volatile
    private var _lastPollTimestamp: Instant = Instant.now()
    val lastPollTimestamp: Instant get() = _lastPollTimestamp

    @Volatile
    private var activeJob: Job? = null

    fun onStart(
        @Observes ev: StartupEvent,
    ) {
        start(scope)
    }

    fun start(scope: CoroutineScope): Job {
        val workerConfig = config.worker()
        val workerId = workerConfig.id()
        val concurrency = workerConfig.concurrency()
        val pollInterval = workerConfig.pollInterval()

        val childJob = SupervisorJob(scope.coroutineContext.job)
        activeJob = childJob
        val workerScope = scope + childJob + ShutdownSignal { !_accepting.get() }

        // Main poll loop
        workerScope.launch {
            claimFlow(pollInterval.toMillis())
                .unorderedMapAsync(concurrency) { task -> processTask(task) }
                .onCompletion { log.info("Worker loop terminated") }
                .collect {}
        }

        // Deadline reaper
        workerScope.launch {
            reapExpiredTasks(pollInterval)
        }

        log.info("Worker loop started: workerId={}, concurrency={}, pollInterval={}", workerId, concurrency, pollInterval)
        return childJob
    }

    override val shutdownOrder: Int = SHUTDOWN_ORDER_WORKER

    override val shutdownTimeout: Duration get() = config.shutdown().globalTimeout()

    override suspend fun shutdown() {
        log.info("Worker loop shutting down")
        _accepting.set(false)
        activeJob?.join()
        releaseUnclaimedTasks()
        log.info("Worker loop shutdown complete")
    }

    /**
     * Single-threaded claim flow. Emits claimed [Task] items; handles
     * backoff on empty poll or transient errors. Terminates when
     * [_accepting] is set to false.
     */
    private fun claimFlow(pollIntervalMs: Long): Flow<Task> = flow {
        val workerId = config.worker().id()
        while (_accepting.get()) {
            try {
                val tasks = taskRepo.claimNext(workerId, 1)
                _lastPollTimestamp = Instant.now()
                if (tasks.isNotEmpty()) {
                    for (task in tasks) {
                        emit(task)
                    }
                } else {
                    delay(pollIntervalMs)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Error in claim loop", e)
                delay(pollIntervalMs)
            }
        }
        log.info("Shutdown signaled, stopping claim loop")
    }

    private suspend fun processTask(task: Task) {
        _inFlightTasks.incrementAndGet()
        try {
            val handler = handlerRegistry.resolve(task.handlerKey)
            val input = HandlerInput(
                taskId = task.id,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                payload = task.payloadJson,
            )
            val output = handler.execute(input)

            barrierService.onTaskCompleted(
                taskId = task.id,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                result = TaskStatus.COMPLETED,
                resultJson = output.result,
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleTaskFailure(task, e)
        } finally {
            _inFlightTasks.decrementAndGet()
        }
    }

    private suspend fun handleTaskFailure(task: Task, cause: Exception) {
        log.warn(
            "Task {} failed (retry {}/{}): {}",
            task.id,
            task.retryCount,
            task.maxRetries,
            cause.message,
        )

        if (task.retryCount < task.maxRetries) {
            try {
                taskRepo.resetForRetry(task.id, task.retryCount + 1)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Failed to reset task {} for retry, reporting as FAILED", task.id, e)
                reportTaskFailed(task)
            }
        } else {
            reportTaskFailed(task)
        }
    }

    private suspend fun reportTaskFailed(task: Task) {
        try {
            barrierService.onTaskCompleted(
                taskId = task.id,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                result = TaskStatus.FAILED,
                resultJson = null,
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Failed to report task {} as FAILED to barrier", task.id, e)
        }
    }

    private suspend fun reapExpiredTasks(pollInterval: Duration) {
        while (_accepting.get()) {
            delay(pollInterval.toMillis())
            try {
                val expired = taskRepo.findExpired(Instant.now())
                for (task in expired) {
                    log.warn("Reaping expired task {} (deadline={})", task.id, task.deadlineAt)
                    reportTaskFailed(task)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Deadline reaper failed", e)
            }
        }
    }

    private suspend fun releaseUnclaimedTasks() {
        val workerId = config.worker().id()
        try {
            val released = taskRepo.releaseByWorker(workerId)
            if (released > 0) {
                log.info("Released {} task(s) back to PENDING", released)
            }
        } catch (e: Exception) {
            log.error("Failed to release tasks — deadline reaper will recover them", e)
        }
    }
}
