package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskCompleted
import com.mapreduce.event.TaskDeadLettered
import com.mapreduce.event.TaskResultType
import com.mapreduce.observability.AutoscalingMetrics
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import org.jboss.logging.Logger
import java.util.concurrent.TimeUnit

/**
 * Claims a single task from the queue, resolves its handler, invokes it,
 * and records the outcome. The [WorkerLoop] calls this in a bulkhead-controlled loop.
 */
@ApplicationScoped
class TaskDispatcher(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val meterRegistry: MeterRegistry,
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

    /** Execute a claimed task: resolve handler, invoke, record outcome. */
    suspend fun execute(task: Task) {
        val handler = handlerRegistry.resolve(task.handler)
        if (handler == null) {
            log.errorf("No handler for '%s' — dead-lettering task %s", task.handler, task.taskId)
            taskRepository.deadLetter(task.taskId, "No handler registered for '${task.handler}'")
            meterRegistry.counter("task.dead_letter", "handler", task.handler).increment()
            fireDeadLetterEvent(task, "No handler registered for '${task.handler}'")
            fireTaskCompleted(task, TaskResultType.DEAD_LETTERED, 0, "No handler registered for '${task.handler}'")
            return
        }

        val ctx = TaskContext(
            task.taskId, task.payload, task.groupId, task.metadata, task.executionGeneration,
            retryCount = task.retryCount, maxRetries = task.maxRetries,
            shuttingDownSupplier = { shutdownCoordinator.isShuttingDown },
        )
        val gen = task.executionGeneration
        val start = System.nanoTime()

        try {
            when (val result = handler.handle(ctx)) {
                is TaskResult.Success -> {
                    taskRepository.complete(task.taskId, gen)
                    recordMetrics(task.handler, start, "success")
                    autoscalingMetrics.recordTaskDuration(task.handler, "Success", System.nanoTime() - start)
                    circuitBreaker.recordSuccess()
                    fireTaskCompleted(task, TaskResultType.SUCCESS, (System.nanoTime() - start) / 1_000_000, null)
                }
                is TaskResult.Retry -> {
                    val wasDeadLettered = taskRepository.fail(task.taskId, result.reason, result.delay, gen)
                    recordMetrics(task.handler, start, "retry")
                    autoscalingMetrics.recordTaskDuration(task.handler, "Retry", System.nanoTime() - start)
                    autoscalingMetrics.recordTaskError(task.handler, "retry")
                    circuitBreaker.recordFailure()
                    if (wasDeadLettered) fireDeadLetterEvent(task, result.reason)
                    val resultType = if (wasDeadLettered) TaskResultType.DEAD_LETTERED else TaskResultType.RETRY
                    fireTaskCompleted(task, resultType, (System.nanoTime() - start) / 1_000_000, result.reason)
                }
                is TaskResult.Failure -> {
                    val wasDeadLettered = taskRepository.fail(task.taskId, result.message, executionGeneration = gen)
                    recordMetrics(task.handler, start, "failure")
                    autoscalingMetrics.recordTaskDuration(task.handler, "DeadLetter", System.nanoTime() - start)
                    autoscalingMetrics.recordTaskError(task.handler, "failure")
                    circuitBreaker.recordFailure()
                    if (wasDeadLettered) fireDeadLetterEvent(task, result.message)
                    val resultType = if (wasDeadLettered) TaskResultType.DEAD_LETTERED else TaskResultType.FAILED
                    fireTaskCompleted(task, resultType, (System.nanoTime() - start) / 1_000_000, result.message)
                }
            }
        } catch (e: Exception) {
            log.errorf(e, "Handler '%s' threw for task %s", task.handler, task.taskId)
            val errorMsg = e.message ?: "Unknown error"
            val wasDeadLettered = taskRepository.fail(task.taskId, errorMsg, executionGeneration = gen)
            recordMetrics(task.handler, start, "error")
            autoscalingMetrics.recordTaskDuration(task.handler, "DeadLetter", System.nanoTime() - start)
            autoscalingMetrics.recordTaskError(task.handler, e.javaClass.simpleName)
            circuitBreaker.recordFailure()
            if (wasDeadLettered) fireDeadLetterEvent(task, errorMsg)
            val resultType = if (wasDeadLettered) TaskResultType.DEAD_LETTERED else TaskResultType.FAILED
            fireTaskCompleted(task, resultType, (System.nanoTime() - start) / 1_000_000, errorMsg)
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

    private fun recordMetrics(handler: String, startNanos: Long, outcome: String) {
        val durationNanos = System.nanoTime() - startNanos
        meterRegistry.timer("task.handler.duration", "handler", handler, "outcome", outcome)
            .record(durationNanos, TimeUnit.NANOSECONDS)
        meterRegistry.counter("task.handler.total", "handler", handler, "outcome", outcome)
            .increment()
    }
}
