package com.mapreduce.observability

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.Priority
import jakarta.interceptor.AroundInvoke
import jakarta.interceptor.Interceptor
import jakarta.interceptor.InvocationContext
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED

/**
 * Coroutine-aware interceptor that records execution duration and success/failure counters.
 *
 * For regular (non-suspend) methods, timing is straightforward.
 * For suspend functions, the interceptor wraps the [Continuation] parameter
 * so that metrics are recorded when the coroutine actually completes — not
 * at the first suspension point.
 */
@Timed
@Interceptor
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 10)
class TimedInterceptor(private val meterRegistry: MeterRegistry) {

    @AroundInvoke
    fun intercept(ctx: InvocationContext): Any? {
        val annotation = ctx.method.getAnnotation(Timed::class.java)
            ?: ctx.target.javaClass.getAnnotation(Timed::class.java)

        val metricName = annotation?.value?.ifEmpty { null }
            ?: "${ctx.target.javaClass.simpleName}.${ctx.method.name}"

        val tags = buildList {
            val extras = annotation?.extraTags ?: emptyArray()
            var i = 0
            while (i + 1 < extras.size) {
                add(extras[i] to extras[i + 1])
                i += 2
            }
        }

        val params = ctx.parameters
        if (params.isNotEmpty() && params.last() is Continuation<*>) {
            return interceptSuspend(ctx, metricName, tags)
        }

        return interceptBlocking(ctx, metricName, tags)
    }

    private fun interceptBlocking(
        ctx: InvocationContext,
        metricName: String,
        tags: List<Pair<String, String>>,
    ): Any? {
        val timer = Timer.builder("$metricName.duration")
            .apply { tags.forEach { (k, v) -> tag(k, v) } }
            .register(meterRegistry)

        val sample = Timer.start(meterRegistry)
        return try {
            val result = ctx.proceed()
            meterRegistry.counter("$metricName.success").increment()
            result
        } catch (e: Throwable) {
            meterRegistry.counter("$metricName.failure").increment()
            throw e
        } finally {
            sample.stop(timer)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun interceptSuspend(
        ctx: InvocationContext,
        metricName: String,
        tags: List<Pair<String, String>>,
    ): Any? {
        val params = ctx.parameters
        val originalContinuation = params.last() as Continuation<Any?>

        val timer = Timer.builder("$metricName.duration")
            .apply { tags.forEach { (k, v) -> tag(k, v) } }
            .register(meterRegistry)
        val sample = Timer.start(meterRegistry)

        val wrappedContinuation = object : Continuation<Any?> {
            override val context = originalContinuation.context
            override fun resumeWith(result: Result<Any?>) {
                if (result.isSuccess) {
                    meterRegistry.counter("$metricName.success").increment()
                } else {
                    meterRegistry.counter("$metricName.failure").increment()
                }
                sample.stop(timer)
                originalContinuation.resumeWith(result)
            }
        }

        val newParams = params.copyOf()
        newParams[newParams.size - 1] = wrappedContinuation
        ctx.parameters = newParams

        return try {
            val result = ctx.proceed()
            if (result !== COROUTINE_SUSPENDED) {
                meterRegistry.counter("$metricName.success").increment()
                sample.stop(timer)
            }
            result
        } catch (e: Throwable) {
            meterRegistry.counter("$metricName.failure").increment()
            sample.stop(timer)
            throw e
        }
    }
}
