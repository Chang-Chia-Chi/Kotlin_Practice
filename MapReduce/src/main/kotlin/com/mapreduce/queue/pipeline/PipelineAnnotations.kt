package com.mapreduce.queue.pipeline

import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

/**
 * Per-handler timeout override. Handlers without this annotation
 * use the global default (`mapreduce.pipeline.default-timeout`).
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class HandlerTimeout(
    val value: Long,
    val unit: TimeUnit = TimeUnit.SECONDS,
)

/** Declares exception types as transient (retryable with exponential backoff). */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class TransientExceptions(val value: Array<KClass<out Throwable>>)

/** Declares exception types as permanent (dead-letter immediately). */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class PermanentExceptions(val value: Array<KClass<out Throwable>>)
