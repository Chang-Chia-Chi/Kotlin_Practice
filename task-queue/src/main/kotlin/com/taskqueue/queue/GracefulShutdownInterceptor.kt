package com.taskqueue.queue

import io.quarkus.runtime.ShutdownEvent
import jakarta.annotation.Priority
import jakarta.enterprise.event.Observes
import jakarta.interceptor.AroundInvoke
import jakarta.interceptor.Interceptor
import jakarta.interceptor.InvocationContext
import org.jboss.logging.Logger
import java.util.concurrent.atomic.AtomicInteger

/**
 * CDI interceptor that implements the graceful-shutdown pattern for any method
 * annotated with [@GracefulShutdown].
 *
 * Once a [ShutdownEvent] is observed:
 * - New invocations are **skipped** (return `null` immediately).
 * - The interceptor **waits** for any in-flight invocations to finish, up to the
 *   `timeoutSeconds` declared on the annotation.
 */
@Interceptor
@GracefulShutdown
@Priority(Interceptor.Priority.PLATFORM_BEFORE)
class GracefulShutdownInterceptor {

    private val log = Logger.getLogger(GracefulShutdownInterceptor::class.java)

    @Volatile
    private var shuttingDown = false

    private val inFlightCount = AtomicInteger(0)

    @Volatile
    private var timeoutSeconds: Long = 25

    fun onShutdown(@Observes event: ShutdownEvent) {
        log.info("Shutdown signal received — draining in-flight invocations")
        shuttingDown = true

        val deadline = System.currentTimeMillis() + timeoutSeconds * 1000
        while (inFlightCount.get() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(250)
        }

        val remaining = inFlightCount.get()
        if (remaining > 0) {
            log.warnf("Shutdown timeout reached with %d invocation(s) still in-flight", remaining)
        } else {
            log.info("All in-flight invocations drained — clean shutdown")
        }
    }

    @AroundInvoke
    fun intercept(ctx: InvocationContext): Any? {
        if (shuttingDown) return null

        cacheTimeout(ctx)

        inFlightCount.incrementAndGet()
        try {
            return ctx.proceed()
        } finally {
            inFlightCount.decrementAndGet()
        }
    }

    private fun cacheTimeout(ctx: InvocationContext) {
        val annotation = ctx.method.getAnnotation(GracefulShutdown::class.java)
            ?: ctx.target?.javaClass?.getAnnotation(GracefulShutdown::class.java)
        if (annotation != null) {
            timeoutSeconds = annotation.timeoutSeconds
        }
    }

    /** Visible for testing. */
    internal fun isShuttingDown(): Boolean = shuttingDown

    /** Visible for testing. */
    internal fun getInFlightCount(): Int = inFlightCount.get()

    /** Visible for testing — allows setting timeout without annotation resolution. */
    internal fun setTimeoutSeconds(seconds: Long) {
        timeoutSeconds = seconds
    }
}
