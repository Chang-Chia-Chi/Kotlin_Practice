package com.workflow.worker

import com.workflow.config.FrameworkConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.dsl.WorkflowDefinition
import com.workflow.engine.BarrierService
import com.workflow.engine.InputResolver
import com.workflow.engine.Task
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.engine.SequenceInfo
import com.workflow.engine.WorkflowRepository
import com.workflow.engine.buildSequenceMap
import com.workflow.extension.indefinitelyRepeat
import com.workflow.extension.takeUntilSignal
import com.workflow.extension.unorderedMapAsync
import com.workflow.shutdown.ShutdownParticipant
import com.workflow.shutdown.ShutdownSignal
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
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
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Workers drain at order 10, after the leader (order 1) stops sweeper
 * patrols. This ensures in-flight work completes without sweeper
 * interference.
 */
const val SHUTDOWN_ORDER_WORKER = 10
private const val MAX_DEFINITION_CACHE_SIZE = 1024

/**
 * Poll loop that claims PENDING tasks, executes handlers, and feeds results
 * into the barrier for workflow progression.
 *
 * **Pipeline:**
 * ```
 * indefinitelyRepeat(Unit)
 *   .takeUntilSignal(stopChannel)
 *   .unorderedMapAsync(concurrency) { pollAndProcess(workerId, pollInterval, batchSize) }
 *   .collect {}
 * ```
 *
 * **Lifecycle:**
 * - Production: auto-starts via [onStart] observing [StartupEvent].
 * - Tests: call [start] with a test-controlled [CoroutineScope].
 * - Shutdown: [shutdown] sets [_accepting] to false, signals [stopChannel],
 *   and cancels the scope; cancellation wakes any delay(), children exit,
 *   join returns.
 *
 * **Error contract:** The [processTask] transform catches ALL non-cancellation
 * exceptions from handler execution and reports them to
 * [BarrierService.onTaskCompleted] with [TaskStatus.FAILED] BEFORE returning.
 * No exception escapes the transform. If the barrier call itself fails for a
 * COMPLETED task, the failure is routed through the retry/failure path
 * ([handleTaskFailure]), which may trigger [TaskRepository.resetForRetry] if
 * retries remain, or [reportTaskFailed] if exhausted.
 *
 * **Retry semantics:** On handler failure, if `task.retryCount < task.maxRetries`,
 * the task is atomically reset to PENDING via [TaskRepository.resetForRetry]
 * (clears claim, increments retry count). The barrier is NOT called -- the
 * task re-enters the claimable pool. If retries are exhausted, the task is
 * marked FAILED and the barrier fires.
 */
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val taskRepo: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val barrierService: BarrierService,
    private val meterRegistry: MeterRegistry,
    private val inputResolver: InputResolver,
    private val workflowRepo: WorkflowRepository,
    private val objectMapper: ObjectMapper,
) : ShutdownParticipant {
    private val log = LoggerFactory.getLogger(WorkerLoop::class.java)

    private val _accepting = AtomicBoolean(true)
    private val _inFlightTasks = AtomicInteger(0)
    val inFlightTasks: Int get() = _inFlightTasks.get()

    private val stopChannel = Channel<Unit>(Channel.RENDEZVOUS)

    @Volatile
    private var _lastActivityTimestamp: Instant = Instant.now()
    val lastActivityTimestamp: Instant get() = _lastActivityTimestamp

    private lateinit var claimTotal: (String) -> Counter
    private lateinit var claimedTasksTotal: Counter

    private data class CachedDefinition(
        val definition: WorkflowDefinition,
        val sequenceMap: Map<Int, SequenceInfo>,
    )

    private val definitionCache: MutableMap<String, CachedDefinition> =
        Collections.synchronizedMap(object : LinkedHashMap<String, CachedDefinition>(128, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, CachedDefinition>): Boolean =
                size > MAX_DEFINITION_CACHE_SIZE
        })

    @Volatile
    private var activeJob: Job? = null

    fun onStart(
        @Observes ev: StartupEvent,
    ) {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO.limitedParallelism(config.worker().concurrency()))
        start(scope)
    }

    fun start(scope: CoroutineScope): Job {
        val workerConfig = config.worker()
        val workerId = workerConfig.id()
        val concurrency = workerConfig.concurrency()
        val pollInterval = workerConfig.pollInterval()
        val batchSize = workerConfig.batchSize()

        val podTag = Tags.of("pod", workerId)
        meterRegistry.gauge(
            "taskqueue_worker_in_flight_tasks",
            podTag,
            _inFlightTasks,
        ) { it.get().toDouble() }
        meterRegistry.gauge(
            "taskqueue_worker_concurrency_limit",
            podTag,
            concurrency,
        ) { it.toDouble() }
        claimTotal = { outcome: String ->
            meterRegistry.counter("taskqueue_claim_total", "pod", workerId, "outcome", outcome)
        }
        claimedTasksTotal = meterRegistry.counter("taskqueue_claimed_tasks_total", "pod", workerId)

        _accepting.set(true)

        val job =
            scope.launch(ShutdownSignal { !_accepting.get() }) {
                indefinitelyRepeat(Unit)
                    .takeUntilSignal(stopChannel)
                    .unorderedMapAsync(concurrency) { pollAndProcess(workerId, pollInterval, batchSize) }
                    .collect {}
            }
        activeJob = job

        log.info("Worker loop started: workerId={}, concurrency={}, batchSize={}, pollInterval={}", workerId, concurrency, batchSize, pollInterval)
        return job
    }

    override val shutdownOrder: Int = SHUTDOWN_ORDER_WORKER

    override val shutdownTimeout: Duration get() = config.shutdown().globalTimeout()

    override suspend fun shutdown() {
        log.info("Worker loop shutting down")
        _accepting.set(false)
        stopChannel.trySend(Unit)
        withTimeoutOrNull(shutdownTimeout.toMillis()) {
            activeJob?.join()
        }
        activeJob?.cancelAndJoin()
        log.info("Worker loop shutdown complete")
    }

    private suspend fun pollAndProcess(
        workerId: String,
        pollInterval: Duration,
        batchSize: Int,
    ) = withContext(MDCContext(mapOf("worker_id" to workerId))) {
        val tasks =
            try {
                taskRepo.claimNext(workerId, batchSize)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Failed to claim tasks", e)
                claimTotal("error").increment()
                delay(pollInterval.toMillis())
                return@withContext
            }
        _lastActivityTimestamp = Instant.now()

        if (tasks.isEmpty()) {
            claimTotal("empty").increment()
            delay(pollInterval.toMillis())
            return@withContext
        }

        claimTotal("success").increment()
        claimedTasksTotal.increment(tasks.size.toDouble())

        for (task in tasks) {
            processTask(task)
        }
    }

    private suspend fun processTask(task: Task) {
        val taskMdc = MDC.getCopyOfContextMap().orEmpty() + mapOf(
            "task_id" to task.id,
            "handler_key" to task.handlerKey,
            "workflow_id" to task.workflowId,
            "attempt" to task.retryCount.toString(),
        )
        withContext(MDCContext(taskMdc)) {
            _inFlightTasks.incrementAndGet()
            try {
                val handler = handlerRegistry.resolve(task.handlerKey)

                val resolvedInputs = resolveInputs(task)

                val input =
                    HandlerInput(
                        taskId = task.id,
                        workflowId = task.workflowId,
                        sequenceNumber = task.sequenceNumber,
                        inputs = resolvedInputs,
                        item = task.item,
                    )
                val output = handler.execute(input)

                try {
                    barrierService.onTaskCompleted(
                        taskId = task.id,
                        workflowId = task.workflowId,
                        sequenceNumber = task.sequenceNumber,
                        status = TaskStatus.COMPLETED,
                        resultJson = output.result,
                        claimedBy = task.claimedBy,
                        claimedAt = task.claimedAt,
                    )
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.error("Barrier failed for COMPLETED task {}, falling through to failure path", task.id, e)
                    handleTaskFailure(task, e)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                handleTaskFailure(task, e)
            } finally {
                _inFlightTasks.decrementAndGet()
                _lastActivityTimestamp = Instant.now()
            }
        }
    }

    private suspend fun resolveInputs(task: Task): String? {
        val cached = definitionCache.getOrPut(task.workflowId) {
            val workflow = workflowRepo.findById(task.workflowId)
                ?: throw IllegalStateException(
                    "Workflow ${task.workflowId} not found while resolving inputs for task ${task.id}"
                )
            val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
            CachedDefinition(definition, buildSequenceMap(definition))
        }

        val seqInfo = cached.sequenceMap[task.sequenceNumber] ?: return null
        val activityInputs = seqInfo.activity.inputs
        if (activityInputs.isEmpty()) return null

        return inputResolver.resolve(activityInputs, cached.sequenceMap) { seq ->
            taskRepo.findByWorkflowAndSequence(task.workflowId, seq)
        }
    }

    private suspend fun handleTaskFailure(
        task: Task,
        cause: Exception,
    ) {
        log.warn(
            "Task {} (handler={}) failed (retry {}/{}): {}",
            task.id,
            task.handlerKey,
            task.retryCount,
            task.maxRetries,
            cause.message,
            cause,
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
                status = TaskStatus.FAILED,
                resultJson = null,
                claimedBy = task.claimedBy,
                claimedAt = task.claimedAt,
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Failed to report task {} as FAILED to barrier", task.id, e)
        }
    }

}
