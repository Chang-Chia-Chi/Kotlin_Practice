package com.workflow.queryexporter.core

import com.workflow.queryexporter.config.MetricConfig
import com.workflow.queryexporter.config.MetricType
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class MetricWriter(private val registry: MeterRegistry) {

    private data class MeterKey(val metricName: String, val tags: List<Tag>)

    private val gaugeValues = ConcurrentHashMap<MeterKey, AtomicReference<Double>>()
    private val registeredMeters = ConcurrentHashMap<MeterKey, Meter>()

    fun write(metric: MetricConfig, rows: List<Map<String, Any?>>) {
        when (metric.type) {
            MetricType.GAUGE -> writeGauge(metric, rows)
            else -> throw UnsupportedOperationException("MetricType.${metric.type} not yet implemented")
        }
    }

    private fun writeGauge(metric: MetricConfig, rows: List<Map<String, Any?>>) {
        val seenKeys = mutableSetOf<MeterKey>()

        for (row in rows) {
            val value = toDouble(row[metric.valueColumn])
            val tags = metric.tagColumns.map { col -> Tag.of(col, row[col]?.toString() ?: "") }
            val key = MeterKey(metric.name, tags)
            seenKeys += key

            val ref = gaugeValues.computeIfAbsent(key) { k ->
                val atomicRef = AtomicReference(0.0)
                val gauge = Gauge.builder(k.metricName, atomicRef) { it.get() }
                    .tags(k.tags)
                    .register(registry)
                registeredMeters[k] = gauge
                atomicRef
            }
            ref.set(value)
        }

        // Stale tag cleanup: zero out any cached keys for this metric name not in current batch
        gaugeValues.keys
            .filter { it.metricName == metric.name && it !in seenKeys }
            .forEach { staleKey -> gaugeValues[staleKey]?.set(0.0) }
    }

    fun close() {
        registeredMeters.values.forEach { meter -> registry.remove(meter) }
        registeredMeters.clear()
        gaugeValues.clear()
    }

    private fun toDouble(value: Any?): Double = when (value) {
        null -> 0.0
        is Number -> value.toDouble()
        is String -> value.toDoubleOrNull() ?: 0.0
        else -> 0.0
    }
}
