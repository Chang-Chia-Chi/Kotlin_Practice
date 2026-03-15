package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskCompleted
import com.mapreduce.event.TaskDeadLettered
import com.mapreduce.event.TaskResultType
import com.mapreduce.observability.AutoscalingMetrics
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.HandlerPipelineBuilder
import com.mapreduce.queue.pipeline.TaskExecutionContext
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import org.jboss.logging.Logger

/**
 * Claims a single task from the queue, resolves its handler, executes
 * it through the middleware pipeline, and records the outcome.
 *
 * The [WorkerLoop] calls this in a bulkhead-controlled loop.
 *
 * Cross-cutting concerns (metrics, tracing, timeout, circuit breaking,
 * error classification) are handled by the [HandlerPipelineBuilder]
 * middleware chain — the dispatcher only processes the resulting
 * [TaskResult] (DB updates, events, pod-level circuit breaker).
 */
@ApplicationScoped
class TaskDispatcher(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val pipelineBuilder: HandlerPipelineBuilder,
    private val circuitBreaker: PodCircuitBreaker,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val autoscalingMetrics: AutoscalingMetrics,
    private val deadLetterEvent: Event<TaskDeadLettered>,
    private val taskCompletedEvent: Event<TaskCompleted>,
) {

    private val log = Logger.getLogger(TaskDispatcher::class.java)

    /** Try to claim a task from subscribed queues. Returns null if no work available. */
    fun claimTask(): Task? =
        taskRepository.claim(config.worker().id(), config.worker().queues())

    /** Execute a claimed task through the middleware pipeline and process the result. */
    suspend fun execute(task: Task) {
        val handler = handlerRegistry.resolve(task.handler)
        if (handler == null) {
            log.errorf("No handler for '%s' — dead-lettering task %s", task.handler, task.taskId)
            taskRepository.deadLetter(task.taskId, "No handler registered for '${task.handler}'")
            fireDeadLetterEvent(task, "No handler registered for '${task.handler}'")
            fireTaskCompleted(task, TaskResultType.DEAD_LETTERED, 0, "No handler registered for '${task.handler}'")
            return
        }

        val executionContext = TaskExecutionContext(
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

        val chain = pipelineBuilder.chainFor(handler)
        val gen = task.executionGeneration
        val start = System.nanoTime()

        // The pipeline handles metrics, tracing, timeout, circuit breaking, and
        // error classification. It always returns a TaskResult (never throws under
        // normal operation). The catch block is defensive against middleware bugs.
        val result = try {
            chain(executionContext)
        } catch (e: Exception) {
            log.errorf(e, "Pipeline escaped with exception for handler '%s' task %s", task.handler, task.taskId)
            TaskResult.Failure("Pipeline error: ${e.javaClass.simpleName}: ${e.message}")
        }

        processResult(task, result, gen, start)
    }

    private fun processResult(task: Task, result: TaskResult, gen: String?, startNanos: Long) {
        val durationNanos = System.nanoTime() - startNanos
        val durationMs = durationNanos / 1_000_000

        when (result) {
            is TaskResult.Success -> {
                taskRepository.complete(task.taskId, gen)
                autoscalingMetrics.recordTaskDuration(task.handler, "Success", durationNanos)
                circuitBreaker.recordSuccess()
                fireTaskCompleted(task, TaskResultType.SUCCESS, durationMs, null)
            }
            is TaskResult.Retry -> {
                if (result.consumeRetry) {
                    val wasDeadLettered = taskRepository.fail(task.taskId, result.reason, result.delay, gen)
                    autoscalingMetrics.recordTaskDuration(task.handler, "Retry", durationNanos)
                    autoscalingMetrics.recordTaskError(task.handler, "retry")
                    circuitBreaker.recordFailure()
                    if (wasDeadLettered) fireDeadLetterEvent(task, result.reason)
                    val resultType = if (wasDeadLettered) TaskResultType.DEAD_LETTERED else TaskResultType.RETRY
                    fireTaskCompleted(task, resultType, durationMs, result.reason)
                } else {
                    // System-level requeue (circuit breaker, shutdown timeout) —
                    // no retry increment, no CB failure recording
                    taskRepository.requeue(task.taskId, result.delay, gen)
                    autoscalingMetrics.recordTaskDuration(task.handler, "Retry", durationNanos)
                    fireTaskCompleted(task, TaskResultType.RETRY, durationMs, result.reason)
                }
            }
            is TaskResult.Failure -> {
                val wasDeadLettered = taskRepository.fail(task.taskId, result.message, executionGeneration = gen)
                autoscalingMetrics.recordTaskDuration(task.handler, "DeadLetter", durationNanos)
                autoscalingMetrics.recordTaskError(task.handler, "failure")
                circuitBreaker.recordFailure()
                if (wasDeadLettered) fireDeadLetterEvent(task, result.message)
                val resultType = if (wasDeadLettered) TaskResultType.DEAD_LETTERED else TaskResultType.FAILED
                fireTaskCompleted(task, resultType, durationMs, result.message)
            }
            is TaskResult.DeadLetter -> {
                taskRepository.deadLetter(task.taskId, result.reason)
                autoscalingMetrics.recordTaskDuration(task.handler, "DeadLetter", durationNanos)
                autoscalingMetrics.recordTaskError(task.handler, "dead_letter")
                circuitBreaker.recordFailure()
                fireDeadLetterEvent(task, result.reason)
                fireTaskCompleted(task, TaskResultType.DEAD_LETTERED, durationMs, result.reason)
            }
        }
    }

    private fun fireDeadLetterEvent(task: Task, error: String) {
        try {
            deadLetterEvent.fireAsync(
                TaskDeadLettered(
                    taskId = task.taskId,
                    handler = task.handler,
                    queue = task.queue,
                    groupId = task.groupId,
                    retryCount = task.retryCount,
                    lastError = error,
                    createdAt = task.createdAt,
                ),
            )
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire TaskDeadLettered event for task %s", task.taskId)
        }
    }

    private fun fireTaskCompleted(task: Task, result: TaskResultType, durationMs: Long, error: String?) {
        try {
            taskCompletedEvent.fireAsync(TaskCompleted(
                taskId = task.taskId,
                handler = task.handler,
                queue = task.queue,
                groupId = task.groupId,
                result = result,
                durationMs = durationMs,
                retryCount = task.retryCount,
                errorMessage = error,
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire TaskCompleted event for task %s", task.taskId)
        }
    }
}
