package com.workflow.benchmark

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

class MetricsSupport private constructor(
    val registry: MeterRegistry,
) {
    companion object {
        fun create(enabled: Boolean): MetricsSupport {
            val registry = if (enabled) SimpleMeterRegistry() else CompositeMeterRegistry()
            return MetricsSupport(registry)
        }
    }

    fun printSummary() {
        println("\n=== Micrometer Metrics Summary ===")
        registry.meters
            .sortedBy { it.id.name }
            .forEach { meter ->
                when (meter) {
                    is Timer -> {
                        val snap = meter.takeSnapshot()
                        println("  ${meter.id.name}: count=${snap.count()} mean=${"%.2f".format(snap.mean() * 1000)}ms max=${"%.2f".format(snap.max() * 1000)}ms")
                    }
                    else -> println("  ${meter.id.name}: ${meter.measure().joinToString { "${it.statistic}=${it.value}" }}")
                }
            }
        println()
    }

    fun stop() {
        // No-op — SimpleMeterRegistry has no server to stop
    }
}
