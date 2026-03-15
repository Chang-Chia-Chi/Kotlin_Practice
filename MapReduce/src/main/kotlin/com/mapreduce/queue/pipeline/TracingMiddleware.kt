package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskResult
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import jakarta.enterprise.context.ApplicationScoped

/**
 * Creates an OpenTelemetry span per task execution (order 20).
 *
 * Inside metrics so that the metrics timer includes tracing overhead
 * (negligible, but consistent). Outside circuit breaker so that
 * rejected tasks still get a span (useful for debugging breaker behavior).
 *
 * When no OTel SDK is configured, the injected [Tracer] is a no-op —
 * spans are created but discarded, with zero overhead.
 */
@ApplicationScoped
class TracingMiddleware(
    private val tracer: Tracer,
) : HandlerMiddleware {

    override val order: Int = 20

    override suspend fun invoke(
        context: TaskExecutionContext,
        next: suspend (TaskExecutionContext) -> TaskResult,
    ): TaskResult {
        val span = tracer.spanBuilder("task.execute ${context.handler}")
            .setAttribute("task.id", context.taskId)
            .setAttribute("task.handler", context.handler)
            .setAttribute("task.queue", context.queue)
            .setAttribute("task.retryCount", context.retryCount.toLong())
            .apply { context.groupId?.let { setAttribute("task.groupId", it) } }
            .startSpan()

        return try {
            val result = next(context)

            when (result) {
                is TaskResult.Success ->
                    span.setStatus(StatusCode.OK)
                is TaskResult.Retry ->
                    span.setStatus(StatusCode.OK, "retry: ${result.reason}")
                is TaskResult.Failure ->
                    span.setStatus(StatusCode.ERROR, result.message)
                is TaskResult.DeadLetter ->
                    span.setStatus(StatusCode.ERROR, "dead-letter: ${result.reason}")
            }

            result
        } catch (e: Exception) {
            span.setStatus(StatusCode.ERROR, e.message ?: "Unknown error")
            span.recordException(e)
            throw e
        } finally {
            span.end()
        }
    }
}
