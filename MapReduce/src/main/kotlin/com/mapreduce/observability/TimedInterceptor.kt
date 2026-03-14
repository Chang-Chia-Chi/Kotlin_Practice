package com.mapreduce.observability

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.Priority
import jakarta.interceptor.AroundInvoke
import jakarta.interceptor.Interceptor
import jakarta.interceptor.InvocationContext

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
}
