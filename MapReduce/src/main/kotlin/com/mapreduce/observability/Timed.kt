package com.mapreduce.observability

import jakarta.interceptor.InterceptorBinding

/**
 * Interceptor binding that records execution duration and success/failure counters.
 * Apply to any method to automatically track metrics without polluting business logic.
 */
@InterceptorBinding
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Timed(
    val value: String = "",
    val extraTags: Array<String> = []
)
