package com.exporter.metrics

import com.exporter.config.MetricType
import com.exporter.config.ResolvedMetric
import io.micrometer.core.instrument.*
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Thread-safe metric state registry.
 *
 * Decouples the "push" side (query execution writing values) from the
 * "pull" side (Prometheus scraping current state via Micrometer).
 *
 * Design choices:
 * - GAUGE: Uses an AtomicReference<Double> behind a Micrometer Gauge so
 *   the latest value is always returned on scrape.
 * - COUNTER: Uses a Micrometer Counter. On each query cycle we compute
 *   delta = newValue - lastSeen and add the delta. This correctly handles
 *   monotonically increasing source counters.
 * - HISTOGRAM/SUMMARY: Uses DistributionSummary. Every row value is
 *   recorded (not set), preserving distribution semantics.
 * - ENUM: Modeled as multiple gauges (one per state), valued 0 or 1.
 */
@ApplicationScoped
class MetricStateRegistry(
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(MetricStateRegistry::class.java)

    // Keyed by (metricName + sorted tags) → holder
    private val gaugeHolders = ConcurrentHashMap<String, AtomicReference<Double>>()
    private val counterLastValues = ConcurrentHashMap<String, AtomicReference<Double>>()
    private val counters = ConcurrentHashMap<String, Counter>()
    private val summaries = ConcurrentHashMap<String, DistributionSummary>()
    private val enumGauges = ConcurrentHashMap<String, AtomicReference<Double>>()

    /**
     * Updates metric state from a single result row.
     *
     * @param metric resolved metric definition
     * @param value the numeric value extracted from valueColumn
     * @param tags label key-value pairs extracted from tagColumns
     */
    fun update(metric: ResolvedMetric, value: Double, tags: Map<String, String>) {
        when (metric.type) {
            MetricType.GAUGE -> updateGauge(metric.name, value, tags)
            MetricType.COUNTER -> updateCounter(metric.name, value, tags)
            MetricType.HISTOGRAM -> recordDistribution(metric, value, tags)
            MetricType.SUMMARY -> recordDistribution(metric, value, tags)
            MetricType.ENUM -> throw IllegalArgumentException(
                "ENUM metrics must use updateEnumByState(), not update(). Metric: ${metric.name}"
            )
        }
    }

    private fun updateGauge(name: String, value: Double, tags: Map<String, String>) {
        val meterTags = toTags(tags)
        val key = compositeKey(name, tags)

        val holder = gaugeHolders.computeIfAbsent(key) { k ->
            val ref = AtomicReference(0.0)
            Gauge.builder(name) { ref.get() }
                .tags(meterTags)
                .register(meterRegistry)
            log.debugf("Registered gauge: %s %s", name, tags)
            ref
        }
        holder.set(value)
    }

    private fun updateCounter(name: String, value: Double, tags: Map<String, String>) {
        val key = compositeKey(name, tags)
        val meterTags = toTags(tags)

        val counter = counters.computeIfAbsent(key) {
            log.debugf("Registered counter: %s %s", name, tags)
            Counter.builder(name)
                .tags(meterTags)
                .register(meterRegistry)
        }

        val lastRef = counterLastValues.computeIfAbsent(key) {
            AtomicReference(0.0)
        }

        // Compute delta: source counter is monotonic, we track the increment.
        val last = lastRef.getAndSet(value)
        val delta = value - last
        if (delta > 0) {
            counter.increment(delta)
        } else if (delta < 0) {
            // Counter reset detected (e.g., DB restart). Start fresh from current value.
            log.debugf("Counter reset detected for %s: %f -> %f", name, last, value)
            counter.increment(value)
        }
        // delta == 0 → no change, no increment
    }

    private fun recordDistribution(metric: ResolvedMetric, value: Double, tags: Map<String, String>) {
        val key = compositeKey(metric.name, tags)
        val meterTags = toTags(tags)

        val summary = summaries.computeIfAbsent(key) {
            val builder = DistributionSummary.builder(metric.name)
                .tags(meterTags)

            if (metric.type == MetricType.HISTOGRAM && metric.buckets.isNotEmpty()) {
                builder.serviceLevelObjectives(*metric.buckets.toDoubleArray())
                builder.publishPercentileHistogram(false)
            }
            if (metric.type == MetricType.SUMMARY) {
                builder.publishPercentiles(0.5, 0.9, 0.95, 0.99)
            }

            log.debugf("Registered %s: %s %s", metric.type, metric.name, tags)
            builder.register(meterRegistry)
        }

        summary.record(value)
    }

    /**
     * Updates an ENUM metric by state name.
     * Sets the active state gauge to 1 and all others to 0.
     */
    fun updateEnumByState(metric: ResolvedMetric, activeState: String, tags: Map<String, String>) {
        for (state in metric.states) {
            val enumTags = tags + ("state" to state)
            val key = compositeKey(metric.name, enumTags)
            val meterTags = toTags(enumTags)

            val holder = enumGauges.computeIfAbsent(key) {
                val ref = AtomicReference(0.0)
                Gauge.builder(metric.name) { ref.get() }
                    .tags(meterTags)
                    .register(meterRegistry)
                ref
            }
            holder.set(if (state == activeState) 1.0 else 0.0)
        }
    }

    /** Clears all tracked state. Primarily for testing. */
    fun clear() {
        gaugeHolders.clear()
        counterLastValues.clear()
        counters.clear()
        summaries.clear()
        enumGauges.clear()
    }

    private fun compositeKey(name: String, tags: Map<String, String>): String {
        val tagStr = tags.entries.sortedBy { it.key }.joinToString(",") { "${it.key}=${it.value}" }
        return "$name{$tagStr}"
    }

    private fun toTags(tags: Map<String, String>): Tags {
        return Tags.of(tags.map { Tag.of(it.key, it.value) })
    }
}
