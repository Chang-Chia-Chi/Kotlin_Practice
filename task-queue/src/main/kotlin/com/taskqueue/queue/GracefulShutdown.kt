package com.taskqueue.queue

import jakarta.enterprise.util.Nonbinding
import jakarta.interceptor.InterceptorBinding

/**
 * Interceptor binding that adds graceful-shutdown semantics to any `@Scheduled` method.
 *
 * When applied, the interceptor:
 * 1. **Skips** the method body once a [io.quarkus.runtime.ShutdownEvent] has been observed.
 * 2. **Tracks** in-flight invocations with an atomic counter.
 * 3. **Drains** on shutdown: waits up to [timeoutSeconds] for the counter to reach zero.
 *
 * Usage:
 * ```kotlin
 * @GracefulShutdown(timeoutSeconds = 25)
 * @Scheduled(every = "5s")
 * fun poll() { /* ... */ }
 * ```
 */
@InterceptorBinding
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class GracefulShutdown(
    /** Maximum seconds to wait for in-flight invocations to finish during shutdown. */
    @get:Nonbinding val timeoutSeconds: Long = 25,
)
