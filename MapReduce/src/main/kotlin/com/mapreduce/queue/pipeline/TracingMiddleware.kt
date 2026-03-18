package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import jakarta.enterprise.context.ApplicationScoped

/**
 * Outermost middleware (order 10). Opens an OTel span around the
 * handler execution and sets status based on the result.
 */
@ApplicationScoped
class TracingMiddleware(private val tracer: Tracer) : Middleware {

    override val order: Int = 10

    override suspend fun invoke(
        context: TaskContext,
        next: suspend (TaskContext) -> TaskResult,
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
                is TaskResult.Success -> span.setStatus(StatusCode.OK)
                is TaskResult.Retry -> span.setStatus(StatusCode.OK, "retry: ${result.reason}")
                is TaskResult.Failure -> span.setStatus(StatusCode.ERROR, result.message)
                is TaskResult.DeadLetter -> span.setStatus(StatusCode.ERROR, "dead-letter: ${result.reason}")
            }
            result
        } catch (e: Exception) {
            span.recordException(e)
            span.setStatus(StatusCode.ERROR, e.message ?: "unknown")
            throw e
        } finally {
            span.end()
        }
    }
}
