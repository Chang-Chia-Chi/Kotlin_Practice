package com.mapreduce.queue.pipeline

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
        context: TaskExecutionContext,
        next: suspend (TaskExecutionContext) -> TaskResult,
    ): TaskResult {
        val startNanos = System.nanoTime()
        val result = next(context)
        val durationNanos = System.nanoTime() - startNanos

        val resultLabel = when (result) {
            is TaskResult.Success -> "success"
            is TaskResult.Retry -> "retry"
            is TaskResult.Failure -> "failure"
            is TaskResult.DeadLetter -> "dead_letter"
        }

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

        return result
    }
}
