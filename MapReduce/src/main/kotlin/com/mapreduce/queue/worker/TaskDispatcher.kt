package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.Middleware
import com.mapreduce.queue.pipeline.TaskExecutionContext
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.queue.spi.TaskHandler
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Claims a single task from the queue, resolves its handler, executes
 * it through the middleware chain, and records the outcome.
 *
 * Tracing and metrics are handled inline here — CDI interceptors cannot
 * wrap Kotlin suspend functions (ArC does not support the Continuation
 * parameter that the compiler adds). Timeout and error classification
 * remain as [Middleware] implementations because they carry domain-specific
 * logic (shutdown-aware timeout, re-enqueue-based retry) that no existing
 * library provides.
 */
@ApplicationScoped
class TaskDispatcher(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val taskGroupRepository: TaskGroupRepository,
    private val handlerRegistry: HandlerRegistry,
    middlewares: Instance<Middleware>,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
    private val tracer: Tracer,
) {

    private val log = Logger.getLogger(TaskDispatcher::class.java)
    private val pipeline: List<Middleware> = middlewares.toList().sortedBy { it.order }
    private val inflightGauges = ConcurrentHashMap<String, AtomicInteger>()

    /** Try to claim a task from subscribed queues. Returns null if no work available. */
    fun claimTask(): Task? =
        taskRepository.claim(config.worker().id(), config.worker().queues())

    /** Execute a claimed task through the middleware pipeline and process the result. */
    suspend fun execute(task: Task) {
        val handler = handlerRegistry.resolve(task.handler)
        if (handler == null) {
            log.errorf("No handler for '%s' — dead-lettering task %s", task.handler, task.taskId)
            taskRepository.deadLetter(task.taskId, "No handler registered for '${task.handler}'")
            return
        }

        val ctx = buildExecutionContext(task)
        val span = startSpan(ctx)
        val inflight = inflightGauge(ctx.handler)
        inflight.incrementAndGet()
        val startNanos = System.nanoTime()

        val result = try {
            buildChain(handler)(ctx)
        } catch (e: Exception) {
            log.errorf(e, "Pipeline escaped with exception for handler '%s' task %s", task.handler, task.taskId)
            span.recordException(e)
            TaskResult.Failure("Pipeline error: ${e.javaClass.simpleName}: ${e.message}")
        } finally {
            inflight.decrementAndGet()
        }

        endSpan(span, result)
        processResult(task, result, task.executionGeneration, startNanos)
    }

    private fun buildChain(handler: TaskHandler): suspend (TaskExecutionContext) -> TaskResult {
        val terminal: suspend (TaskExecutionContext) -> TaskResult = { ctx ->
            handler.handle(ctx.taskContext)
        }
        return pipeline.foldRight(terminal) { middleware, next ->
            { ctx -> middleware.invoke(ctx, next) }
        }
    }

    // ── Tracing ────────────────────────────────────────────────────

    private fun startSpan(ctx: TaskExecutionContext): Span =
        tracer.spanBuilder("task.execute ${ctx.handler}")
            .setAttribute("task.id", ctx.taskId)
            .setAttribute("task.handler", ctx.handler)
            .setAttribute("task.queue", ctx.queue)
            .setAttribute("task.retryCount", ctx.retryCount.toLong())
            .apply { ctx.groupId?.let { setAttribute("task.groupId", it) } }
            .startSpan()

    private fun endSpan(span: Span, result: TaskResult) {
        when (result) {
            is TaskResult.Success -> span.setStatus(StatusCode.OK)
            is TaskResult.Retry -> span.setStatus(StatusCode.OK, "retry: ${result.reason}")
            is TaskResult.Failure -> span.setStatus(StatusCode.ERROR, result.message)
            is TaskResult.DeadLetter -> span.setStatus(StatusCode.ERROR, "dead-letter: ${result.reason}")
        }
        span.end()
    }

    // ── Metrics ────────────────────────────────────────────────────

    private fun inflightGauge(handler: String): AtomicInteger =
        inflightGauges.computeIfAbsent(handler) { name ->
            AtomicInteger(0).also { gauge ->
                meterRegistry.gauge(
                    "taskqueue.handler.inflight",
                    listOf(Tag.of("handler", name)),
                    gauge,
                ) { it.toDouble() }
            }
        }

    private fun recordMetrics(task: Task, result: TaskResult, durationNanos: Long) {
        val resultLabel = when (result) {
            is TaskResult.Success -> "success"
            is TaskResult.Retry -> "retry"
            is TaskResult.Failure -> "failure"
            is TaskResult.DeadLetter -> "dead_letter"
        }

        meterRegistry.timer(
            "taskqueue.handler.duration",
            "handler", task.handler,
            "queue", task.queue,
            "result", resultLabel,
        ).record(durationNanos, TimeUnit.NANOSECONDS)

        meterRegistry.counter(
            "taskqueue.handler.executions",
            "handler", task.handler,
            "result", resultLabel,
        ).increment()
    }

    // ── Result processing ──────────────────────────────────────────

    private fun processResult(task: Task, result: TaskResult, gen: String?, startNanos: Long) {
        recordMetrics(task, result, System.nanoTime() - startNanos)

        when (result) {
            is TaskResult.Success -> {
                if (task.groupId != null) {
                    taskGroupRepository.resolveGroupTask(
                        taskId = task.taskId, groupId = task.groupId,
                        executionGeneration = gen,
                        outputUri = result.outputUri, outputMetadata = result.outputMetadata,
                    )
                } else {
                    taskRepository.complete(task.taskId, gen)
                }
            }
            is TaskResult.Retry -> {
                if (result.consumeRetry) {
                    val deadLettered = taskRepository.fail(task.taskId, result.reason, result.delay, gen)
                    if (deadLettered && task.groupId != null) {
                        taskGroupRepository.resolveGroupTask(groupId = task.groupId, failed = true)
                    }
                } else {
                    taskRepository.requeue(task.taskId, result.delay, gen)
                }
            }
            is TaskResult.Failure -> {
                val deadLettered = taskRepository.fail(task.taskId, result.message, executionGeneration = gen)
                if (deadLettered && task.groupId != null) {
                    taskGroupRepository.resolveGroupTask(groupId = task.groupId, failed = true)
                }
            }
            is TaskResult.DeadLetter -> {
                taskRepository.deadLetter(task.taskId, result.reason)
                if (task.groupId != null) {
                    taskGroupRepository.resolveGroupTask(groupId = task.groupId, failed = true)
                }
            }
        }
    }

    // ── Context building ───────────────────────────────────────────

    private fun buildExecutionContext(task: Task) = TaskExecutionContext(
        taskId = task.taskId,
        handler = task.handler,
        queue = task.queue,
        groupId = task.groupId,
        payload = task.payload,
        metadata = task.metadata,
        retryCount = task.retryCount,
        maxRetries = task.maxRetries,
        claimedAt = task.claimedAt,
        executionGeneration = task.executionGeneration,
        taskContext = TaskContext(
            task.taskId, task.payload, task.groupId, task.metadata, task.executionGeneration,
            retryCount = task.retryCount, maxRetries = task.maxRetries,
            shuttingDownSupplier = { shutdownCoordinator.isShuttingDown },
        ),
    )
}
