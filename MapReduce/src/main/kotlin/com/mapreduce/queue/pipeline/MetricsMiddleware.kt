package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.TimeUnit

/**
 * Metrics middleware (order 20). Records per-handler execution
 * duration and result counters.
 */
@ApplicationScoped
class MetricsMiddleware(private val meterRegistry: MeterRegistry) : Middleware {

    override val order: Int = 20

    override suspend fun invoke(
        context: TaskContext,
        next: suspend (TaskContext) -> TaskResult,
    ): TaskResult {
        val startNanos = System.nanoTime()
        val result = try {
            next(context)
        } catch (e: Exception) {
            recordMetrics(context, "exception", startNanos)
            throw e
        }

        val resultLabel = when (result) {
            is TaskResult.Success -> "success"
            is TaskResult.Retry -> "retry"
            is TaskResult.Failure -> "failure"
            is TaskResult.DeadLetter -> "dead_letter"
        }

        recordMetrics(context, resultLabel, startNanos)
        return result
    }

    private fun recordMetrics(context: TaskContext, resultLabel: String, startNanos: Long) {
        val durationNanos = System.nanoTime() - startNanos

        meterRegistry.timer(
            "taskqueue.handler.duration",
            "handler", context.handler,
            "queue", context.queue,
            "result", resultLabel,
        ).record(durationNanos, TimeUnit.NANOSECONDS)

        meterRegistry.counter(
            "taskqueue.handler.executions",
            "handler", context.handler,
            "result", resultLabel,
        ).increment()
    }
}
