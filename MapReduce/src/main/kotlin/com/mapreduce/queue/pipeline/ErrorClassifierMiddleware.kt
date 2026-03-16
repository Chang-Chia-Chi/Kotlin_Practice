package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CancellationException
import org.jboss.logging.Logger
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.sql.SQLTransientException
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min
import kotlin.random.Random

/**
 * Innermost middleware (order 50). Catches exceptions from the handler
 * and classifies them into structured [TaskResult] values.
 *
 * Classification categories:
 * - **Transient**: retryable with exponential backoff + jitter (e.g., network errors)
 * - **Permanent**: dead-letter immediately, skip remaining retries (e.g., bad input)
 * - **Unknown**: normal failure — follows the standard retry/dead-letter cycle
 *
 * Handlers can customize classification via [TransientExceptions] and
 * [PermanentExceptions] annotations on their implementation class.
 *
 * Important: [CancellationException] is always re-thrown, never classified.
 * This preserves coroutine cancellation semantics for [TimeoutMiddleware].
 */
@ApplicationScoped
class ErrorClassifierMiddleware(
    private val handlerRegistry: HandlerRegistry,
) {

    private val log = Logger.getLogger(ErrorClassifierMiddleware::class.java)

    /** Cached per-handler classification config. */
    private val classificationCache = ConcurrentHashMap<String, ClassificationConfig>()

    suspend fun invoke(
        context: TaskExecutionContext,
        next: suspend (TaskExecutionContext) -> TaskResult,
    ): TaskResult =
        try {
            next(context)
        } catch (e: CancellationException) {
            throw e // never classify coroutine cancellation
        } catch (e: Exception) {
            classify(e, context)
        }

    private fun classify(e: Exception, context: TaskExecutionContext): TaskResult {
        val config = classificationCache.computeIfAbsent(context.handler) {
            buildClassificationConfig(it)
        }

        return when {
            config.isPermanent(e) -> {
                log.infof(
                    "Permanent error for handler '%s' task %s: %s — dead-lettering",
                    context.handler, context.taskId, e.javaClass.simpleName,
                )
                TaskResult.DeadLetter("Permanent error: ${e.javaClass.simpleName}: ${e.message}")
            }
            config.isTransient(e) -> {
                val backoff = computeBackoff(context.retryCount)
                log.infof(
                    "Transient error for handler '%s' task %s: %s — retry with %dms backoff",
                    context.handler, context.taskId, e.javaClass.simpleName, backoff.toMillis(),
                )
                TaskResult.Retry(
                    delay = backoff,
                    reason = "Transient error: ${e.javaClass.simpleName}: ${e.message}",
                )
            }
            else -> {
                log.warnf(
                    e, "Unknown error for handler '%s' task %s — standard failure",
                    context.handler, context.taskId,
                )
                TaskResult.Failure("${e.javaClass.simpleName}: ${e.message}")
            }
        }
    }

    /**
     * Exponential backoff with ±25% jitter.
     * Formula: `min(baseMs × 2^retryCount, maxMs) ± 25% jitter`
     */
    private fun computeBackoff(retryCount: Int): Duration {
        val baseMs = 1_000L
        val maxMs = 60_000L
        val rawMs = min(baseMs * (1L shl retryCount.coerceAtMost(20)), maxMs)
        val jitterRange = (rawMs * 0.25).toLong()
        val jitter = if (jitterRange > 0) Random.nextLong(-jitterRange, jitterRange) else 0
        return Duration.ofMillis((rawMs + jitter).coerceAtLeast(100))
    }

    private fun buildClassificationConfig(handlerName: String): ClassificationConfig {
        val handler = handlerRegistry.resolve(handlerName)
            ?: return ClassificationConfig(emptySet(), emptySet())

        val transientAnnotation = handler::class.java.getAnnotation(TransientExceptions::class.java)
        val permanentAnnotation = handler::class.java.getAnnotation(PermanentExceptions::class.java)

        val customTransient = transientAnnotation?.value?.map { it.java }?.toSet() ?: emptySet()
        val customPermanent = permanentAnnotation?.value?.map { it.java }?.toSet() ?: emptySet()

        return ClassificationConfig(customTransient, customPermanent)
    }

    /** Per-handler classification rules. */
    private class ClassificationConfig(
        private val customTransient: Set<Class<out Throwable>>,
        private val customPermanent: Set<Class<out Throwable>>,
    ) {
        companion object {
            /** Default transient exception types. */
            private val DEFAULT_TRANSIENT: Set<Class<out Throwable>> = setOf(
                SQLTransientException::class.java,
                ConnectException::class.java,
                SocketTimeoutException::class.java,
            )

            /** Default permanent exception types. */
            private val DEFAULT_PERMANENT: Set<Class<out Throwable>> = setOf(
                IllegalArgumentException::class.java,
                NullPointerException::class.java,
            )
        }

        fun isTransient(e: Exception): Boolean =
            matchesAny(e, customTransient) || (customTransient.isEmpty() && matchesAny(e, DEFAULT_TRANSIENT))

        fun isPermanent(e: Exception): Boolean =
            matchesAny(e, customPermanent) || (customPermanent.isEmpty() && matchesAny(e, DEFAULT_PERMANENT))

        private fun matchesAny(e: Exception, types: Set<Class<out Throwable>>): Boolean =
            types.any { it.isAssignableFrom(e.javaClass) }
    }
}
