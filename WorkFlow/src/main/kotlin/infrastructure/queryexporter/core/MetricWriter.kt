package com.workflow.infrastructure.queryexporter.core

import com.workflow.infrastructure.queryexporter.config.MetricConfig
import com.workflow.infrastructure.queryexporter.config.MetricType
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class MetricWriter(private val registry: MeterRegistry) {

    private data class MeterKey(val metricName: String, val tags: List<Tag>)

    private val gaugeValues = ConcurrentHashMap<MeterKey, AtomicReference<Double>>()
    private val counterValues = ConcurrentHashMap<MeterKey, AtomicReference<Double>>()
    private val registeredMeters = ConcurrentHashMap<MeterKey, Meter>()

    fun write(metric: MetricConfig, rows: List<Map<String, Any?>>) {
        when (metric.type) {
            MetricType.GAUGE -> writeGauge(metric, rows)
            MetricType.COUNTER -> writeCounter(metric, rows)
            MetricType.HISTOGRAM -> writeHistogram(metric, rows)
            MetricType.SUMMARY -> writeSummary(metric, rows)
            MetricType.ENUM -> writeEnum(metric, rows)
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

    private fun writeCounter(metric: MetricConfig, rows: List<Map<String, Any?>>) {
        for (row in rows) {
            val value = toDouble(row[metric.valueColumn])
            val tags = metric.tagColumns.map { col -> Tag.of(col, row[col]?.toString() ?: "") }
            val key = MeterKey(metric.name, tags)

            val ref = counterValues.computeIfAbsent(key) { k ->
                val atomicRef = AtomicReference(0.0)
                val counter = FunctionCounter.builder(k.metricName, atomicRef) { it.get() }
                    .tags(k.tags)
                    .register(registry)
                registeredMeters[k] = counter
                atomicRef
            }
            // Absolute/monotonic: last row wins for same tag combo
            ref.set(value)
        }
        // No stale tag cleanup for counters — holds last seen value to avoid counter reset
    }

    private fun writeHistogram(metric: MetricConfig, rows: List<Map<String, Any?>>) {
        for (row in rows) {
            val value = toDouble(row[metric.valueColumn])
            val tags = metric.tagColumns.map { col -> Tag.of(col, row[col]?.toString() ?: "") }
            val key = MeterKey(metric.name, tags)

            val summary = registeredMeters.computeIfAbsent(key) { k ->
                DistributionSummary.builder(k.metricName)
                    .tags(k.tags)
                    .serviceLevelObjectives(*metric.buckets.toDoubleArray())
                    .register(registry)
            } as DistributionSummary
            summary.record(value)
        }
    }

    private fun writeSummary(metric: MetricConfig, rows: List<Map<String, Any?>>) {
        for (row in rows) {
            val value = toDouble(row[metric.valueColumn])
            val tags = metric.tagColumns.map { col -> Tag.of(col, row[col]?.toString() ?: "") }
            val key = MeterKey(metric.name, tags)

            val summary = registeredMeters.computeIfAbsent(key) { k ->
                DistributionSummary.builder(k.metricName)
                    .tags(k.tags)
                    .publishPercentiles(0.5, 0.9, 0.95, 0.99)
                    .register(registry)
            } as DistributionSummary
            summary.record(value)
        }
    }

    private fun writeEnum(metric: MetricConfig, rows: List<Map<String, Any?>>) {
        val seenKeys = mutableSetOf<MeterKey>()

        for (row in rows) {
            // valueColumn is used as tag key for ENUM: each state becomes a separate gauge
            val currentState = row[metric.valueColumn]?.toString()
            val baseTags = metric.tagColumns.map { col -> Tag.of(col, row[col]?.toString() ?: "") }

            for (state in metric.states) {
                val tags = baseTags + Tag.of(metric.valueColumn, state)
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
                ref.set(if (state == currentState) 1.0 else 0.0)
            }
        }

        // Stale tag cleanup: zero out absent tag combos, same as GAUGE
        gaugeValues.keys
            .filter { it.metricName == metric.name && it !in seenKeys }
            .forEach { staleKey -> gaugeValues[staleKey]?.set(0.0) }
    }

    fun close() {
        registeredMeters.values.forEach { meter -> registry.remove(meter) }
        registeredMeters.clear()
        gaugeValues.clear()
        counterValues.clear()
    }

    private fun toDouble(value: Any?): Double = when (value) {
        null -> 0.0
        is Number -> value.toDouble()
        is String -> value.toDoubleOrNull() ?: 0.0
        else -> 0.0
    }
}
