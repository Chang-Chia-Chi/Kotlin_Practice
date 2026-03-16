package com.mapreduce.queue.pipeline

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import org.jboss.logging.Logger
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

/**
 * Enforces a hard execution deadline per handler (order 40).
 *
 * Resolves the timeout from a [HandlerTimeout] annotation on the handler
 * class, falling back to the global default (`mapreduce.pipeline.default-timeout`).
 *
 * Wraps the remaining chain in [withTimeout]. If the deadline expires,
 * the handler's coroutine is cancelled and the middleware returns a result
 * based on shutdown state:
 * - Normal operation: `Failure("Handler timed out after ...")`
 * - During shutdown: `Retry(delay=0, consumeRetry=false)` — re-enqueue
 *   for another pod without penalty.
 */
@ApplicationScoped
class TimeoutMiddleware(
    private val config: FrameworkConfig,
    private val handlerRegistry: HandlerRegistry,
) : Middleware {

    private val log = Logger.getLogger(TimeoutMiddleware::class.java)

    override val order: Int = 40

    /** Cached timeout millis per handler name. */
    private val timeouts = ConcurrentHashMap<String, Long>()

    override suspend fun invoke(
        context: TaskExecutionContext,
        next: suspend (TaskExecutionContext) -> TaskResult,
    ): TaskResult {
        val timeoutMs = resolveTimeout(context.handler)

        return try {
            withTimeout(timeoutMs) {
                next(context)
            }
        } catch (e: TimeoutCancellationException) {
            if (context.taskContext.isShuttingDown) {
                log.infof(
                    "Handler '%s' timed out during shutdown — re-enqueuing task %s",
                    context.handler, context.taskId,
                )
                TaskResult.Retry(
                    delay = Duration.ZERO,
                    reason = "Timeout during shutdown — re-enqueue for another pod",
                    consumeRetry = false,
                )
            } else {
                log.warnf(
                    "Handler '%s' timed out after %dms for task %s",
                    context.handler, timeoutMs, context.taskId,
                )
                TaskResult.Failure("Handler '${context.handler}' timed out after ${timeoutMs}ms")
            }
        }
    }

    private fun resolveTimeout(handlerName: String): Long =
        timeouts.computeIfAbsent(handlerName) { name ->
            val handler = handlerRegistry.resolve(name)
            val annotation = handler?.let {
                it::class.java.getAnnotation(HandlerTimeout::class.java)
            }
            if (annotation != null) {
                annotation.unit.toMillis(annotation.value)
            } else {
                config.pipeline().defaultTimeout().toMillis()
            }
        }
}
