package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.pipeline.Middleware
import com.mapreduce.queue.pipeline.TaskExecutionContext
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.queue.spi.TaskHandler
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.TimeUnit

/**
 * Claims a single task from the queue, resolves its handler, executes
 * it through the middleware chain, and records the outcome.
 */
@ApplicationScoped
class TaskDispatcher(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    middlewares: Instance<Middleware>,
    private val circuitBreaker: PodCircuitBreaker,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(TaskDispatcher::class.java)
    private val pipeline: List<Middleware> = middlewares.toList().sortedBy { it.order }

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

        val chain = buildChain(handler)
        val gen = task.executionGeneration
        val start = System.nanoTime()

        val result = try {
            chain(executionContext)
        } catch (e: Exception) {
            log.errorf(e, "Pipeline escaped with exception for handler '%s' task %s", task.handler, task.taskId)
            TaskResult.Failure("Pipeline error: ${e.javaClass.simpleName}: ${e.message}")
        }

        processResult(task, result, gen, start)
    }

    private fun buildChain(handler: TaskHandler): suspend (TaskExecutionContext) -> TaskResult {
        val terminal: suspend (TaskExecutionContext) -> TaskResult = { ctx ->
            handler.handle(ctx.taskContext)
        }
        return pipeline.foldRight(terminal) { middleware, next ->
            { ctx -> middleware.invoke(ctx, next) }
        }
    }

    private fun processResult(task: Task, result: TaskResult, gen: String?, startNanos: Long) {
        val durationNanos = System.nanoTime() - startNanos

        when (result) {
            is TaskResult.Success -> {
                taskRepository.complete(task.taskId, gen)
                recordTaskDuration(task.handler, "Success", durationNanos)
                circuitBreaker.recordSuccess()
            }
            is TaskResult.Retry -> {
                if (result.consumeRetry) {
                    taskRepository.fail(task.taskId, result.reason, result.delay, gen)
                    recordTaskDuration(task.handler, "Retry", durationNanos)
                    recordTaskError(task.handler, "retry")
                    circuitBreaker.recordFailure()
                } else {
                    taskRepository.requeue(task.taskId, result.delay, gen)
                    recordTaskDuration(task.handler, "Retry", durationNanos)
                }
            }
            is TaskResult.Failure -> {
                taskRepository.fail(task.taskId, result.message, executionGeneration = gen)
                recordTaskDuration(task.handler, "DeadLetter", durationNanos)
                recordTaskError(task.handler, "failure")
                circuitBreaker.recordFailure()
            }
            is TaskResult.DeadLetter -> {
                taskRepository.deadLetter(task.taskId, result.reason)
                recordTaskDuration(task.handler, "DeadLetter", durationNanos)
                recordTaskError(task.handler, "dead_letter")
                circuitBreaker.recordFailure()
            }
        }
    }

    private fun recordTaskDuration(handler: String, status: String, durationNanos: Long) {
        Timer.builder("framework.task.duration.seconds")
            .tag("handler", handler)
            .tag("status", status)
            .register(meterRegistry)
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }

    private fun recordTaskError(handler: String, errorType: String) {
        meterRegistry.counter(
            "framework.task.errors.total",
            "handler", handler,
            "error_type", errorType,
        ).increment()
    }
}
