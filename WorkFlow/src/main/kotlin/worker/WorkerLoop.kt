package com.workflow.worker

import com.workflow.config.FrameworkConfig
import com.workflow.engine.BarrierService
import com.workflow.engine.Task
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.extension.indefinitelyRepeat
import com.workflow.extension.takeUntilSignal
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
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

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
 * **Lifecycle:**
 * - Production: auto-starts via [onStart] observing [StartupEvent].
 * - Tests: call [start] with a test-controlled [CoroutineScope].
 * - Shutdown: [com.workflow.shutdown.ShutdownCoordinator] calls [shutdown],
 *   which signals [takeUntilSignal] to cancel the flow. In-flight handler
 *   executions run to completion (or until scope cancellation); no new
 *   claims are made.
 *
 * **Loop structure:**
 * ```
 * indefinitelyRepeat(Unit)
 *   .unorderedMapAsync(concurrency) { pollAndProcess() }
 *   .takeUntilSignal(stopChannel)
 *   .collect {}
 * ```
 *
 * **Error contract:** The transform catches ALL exceptions from handler
 * execution and reports them to [BarrierService.onTaskCompleted] with
 * [TaskStatus.FAILED] BEFORE returning. No exception escapes the transform.
 * This is critical because [unorderedMapAsync] logs and drops escaped
 * exceptions -- the barrier would never be notified, leaving the workflow
 * stuck.
 *
 * **Retry semantics:** On handler failure, if `task.retryCount < task.maxRetries`,
 * the task is atomically reset to PENDING via [TaskRepository.resetForRetry]
 * (clears claim, increments retry count). The barrier is NOT called -- the
 * task re-enters the claimable pool. If retries are exhausted, the task is
 * marked FAILED and the barrier fires.
 *
 * **Deadline reaper:** A separate coroutine runs alongside the main loop
 * within the same scope. It polls [TaskRepository.findExpired] at
 * [com.workflow.config.FrameworkConfig.WorkerConfig.pollInterval] intervals,
 * marking expired PROCESSING tasks as FAILED via the barrier.
 */
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val taskRepo: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val barrierService: BarrierService,
) : ShutdownParticipant {
    private val log = LoggerFactory.getLogger(WorkerLoop::class.java)

    internal var scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val stopChannel = Channel<Unit>(Channel.RENDEZVOUS)
    private val stopRequested = AtomicBoolean(false)
    private var activeJob: Job? = null

    fun onStart(
        @Observes ev: StartupEvent,
    ) {
        start(scope)
    }

    fun start(scope: CoroutineScope) {
        val workerConfig = config.worker()
        val workerId = workerConfig.id()
        val concurrency = workerConfig.concurrency()
        val pollInterval = workerConfig.pollInterval()

        val childJob = SupervisorJob(scope.coroutineContext.job)
        activeJob = childJob
        val workerScope = scope + childJob + ShutdownSignal { stopRequested.get() }

        // Main poll loop
        workerScope.launch {
            indefinitelyRepeat(Unit)
                .takeUntilSignal(stopChannel)
                .unorderedMapAsync(concurrency) { pollAndProcess(workerId, pollInterval) }
                .collect {}
        }

        // Deadline reaper
        workerScope.launch {
            reapExpiredTasks(pollInterval)
        }

        log.info("Worker loop started: workerId={}, concurrency={}, pollInterval={}", workerId, concurrency, pollInterval)
    }

    override val shutdownOrder: Int = SHUTDOWN_ORDER_WORKER

    override val shutdownTimeout: Duration get() = config.shutdown().globalTimeout()

    override suspend fun shutdown() {
        log.info("Worker loop shutting down")
        stopRequested.set(true)
        stopChannel.trySend(Unit)
        activeJob?.cancelAndJoin()
        log.info("Worker loop shutdown complete")
    }

    private suspend fun pollAndProcess(
        workerId: String,
        pollInterval: Duration,
    ) {
        val tasks =
            try {
                taskRepo.claimNext(workerId, 1)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Failed to claim tasks", e)
                delay(pollInterval.toMillis())
                return
            }

        if (tasks.isEmpty()) {
            delay(pollInterval.toMillis())
            return
        }

        for (task in tasks) {
            processTask(task)
        }
    }

    private suspend fun processTask(task: Task) {
        try {
            val handler = handlerRegistry.resolve(task.handlerKey)
            val input =
                HandlerInput(
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
        }
    }

    private suspend fun handleTaskFailure(
        task: Task,
        cause: Exception,
    ) {
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
        while (true) {
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
}
