package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
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
            return
        }

        val ctx = TaskContext(task.taskId, task.payload, task.groupId, task.metadata, task.executionGeneration)
        val gen = task.executionGeneration
        val start = System.nanoTime()

        try {
            when (val result = handler.handle(ctx)) {
                is TaskResult.Success -> {
                    taskRepository.complete(task.taskId, gen)
                    recordMetrics(task.handler, start, "success")
                    circuitBreaker.recordSuccess()
                }
                is TaskResult.Retry -> {
                    taskRepository.fail(task.taskId, result.reason, result.delay, gen)
                    recordMetrics(task.handler, start, "retry")
                    circuitBreaker.recordFailure()
                }
                is TaskResult.Failure -> {
                    taskRepository.fail(task.taskId, result.message, executionGeneration = gen)
                    recordMetrics(task.handler, start, "failure")
                    circuitBreaker.recordFailure()
                }
            }
        } catch (e: Exception) {
            log.errorf(e, "Handler '%s' threw for task %s", task.handler, task.taskId)
            taskRepository.fail(task.taskId, e.message ?: "Unknown error", executionGeneration = gen)
            recordMetrics(task.handler, start, "error")
            circuitBreaker.recordFailure()
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
