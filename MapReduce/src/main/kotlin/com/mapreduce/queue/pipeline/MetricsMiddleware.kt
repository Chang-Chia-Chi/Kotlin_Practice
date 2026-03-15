package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskResult
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Outermost middleware (order 10). Captures end-to-end timing, throughput,
 * in-flight count, and exception breakdown for every handler invocation.
 *
 * Metrics emitted:
 * - `taskqueue.handler.duration` — latency distribution (timer)
 * - `taskqueue.handler.executions` — throughput by result (counter)
 * - `taskqueue.handler.inflight` — currently executing tasks (gauge)
 * - `taskqueue.handler.exceptions` — exception type breakdown (counter)
 */
@ApplicationScoped
class MetricsMiddleware(
    private val meterRegistry: MeterRegistry,
) : HandlerMiddleware {

    override val order: Int = 10

    private val log = Logger.getLogger(MetricsMiddleware::class.java)

    /** Per-handler in-flight gauge backing values. */
    private val inflightGauges = ConcurrentHashMap<String, AtomicInteger>()

    override suspend fun invoke(
        context: TaskExecutionContext,
        next: suspend (TaskExecutionContext) -> TaskResult,
    ): TaskResult {
        val inflight = inflightGauges.computeIfAbsent(context.handler) { handler ->
            AtomicInteger(0).also { gauge ->
                meterRegistry.gauge(
                    "taskqueue.handler.inflight",
                    listOf(Tag.of("handler", handler)),
                    gauge,
                ) { it.toDouble() }
            }
        }

        inflight.incrementAndGet()
        val startNanos = System.nanoTime()

        try {
            val result = next(context)
            val durationNanos = System.nanoTime() - startNanos
            val resultLabel = resultLabel(result)

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
        } catch (e: Exception) {
            val durationNanos = System.nanoTime() - startNanos

            meterRegistry.timer(
                "taskqueue.handler.duration",
                "handler", context.handler,
                "queue", context.queue,
                "result", "error",
            ).record(durationNanos, TimeUnit.NANOSECONDS)

            meterRegistry.counter(
                "taskqueue.handler.executions",
                "handler", context.handler,
                "result", "error",
            ).increment()

            meterRegistry.counter(
                "taskqueue.handler.exceptions",
                "handler", context.handler,
                "exception_class", e.javaClass.simpleName,
            ).increment()

            throw e
        } finally {
            inflight.decrementAndGet()
        }
    }

    private fun resultLabel(result: TaskResult): String = when (result) {
        is TaskResult.Success -> "success"
        is TaskResult.Retry -> "retry"
        is TaskResult.Failure -> "failure"
        is TaskResult.DeadLetter -> "dead_letter"
    }
}
