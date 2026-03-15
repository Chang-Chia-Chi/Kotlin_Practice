package com.mapreduce.queue.pipeline

import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

/**
 * Per-handler timeout override. Applied to [com.mapreduce.queue.spi.TaskHandler]
 * implementation classes.
 *
 * Example: `@HandlerTimeout(30, TimeUnit.SECONDS)` for a 30-second timeout.
 * Handlers without this annotation use the global default (2 minutes).
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class HandlerTimeout(
    val value: Long,
    val unit: TimeUnit = TimeUnit.SECONDS,
)

/**
 * Per-handler circuit breaker configuration. Applied to [com.mapreduce.queue.spi.TaskHandler]
 * implementation classes.
 *
 * Handlers without this annotation have no per-handler circuit breaker —
 * the middleware passes through transparently.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class HandlerCircuitBreaker(
    val failureRateThreshold: Double = 50.0,
    val slidingWindowSize: Int = 20,
    val waitDurationSeconds: Long = 30,
    val permittedCallsInHalfOpen: Int = 5,
)

/**
 * Declares exception types as transient (retryable with exponential backoff).
 * Overrides the default classification for the annotated handler.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class TransientExceptions(val value: Array<KClass<out Throwable>>)

/**
 * Declares exception types as permanent (dead-letter immediately).
 * Overrides the default classification for the annotated handler.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class PermanentExceptions(val value: Array<KClass<out Throwable>>)
