package com.mapreduce.exporter

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Thread-safe bridge between SQL query results and Micrometer meters.
 *
 * The write path (query coroutine) and the read path (Prometheus scrape) share
 * only lock-free state — [AtomicReference] for gauges/counters, [DistributionSummary]
 * internals for histograms/summaries.
 *
 * Meters are registered lazily on first observation and cached by (name, tags).
 * A configurable cardinality ceiling prevents unbounded label explosion.
 */
@ApplicationScoped
class MetricBridge(
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(MetricBridge::class.java)

    // Per-type caches: metricName → (tags → handle)
    private val gauges = ConcurrentHashMap<String, ConcurrentHashMap<Tags, AtomicReference<Double>>>()
    private val counters = ConcurrentHashMap<String, ConcurrentHashMap<Tags, AtomicReference<Double>>>()
    private val histograms = ConcurrentHashMap<String, ConcurrentHashMap<Tags, DistributionSummary>>()
    private val summaries = ConcurrentHashMap<String, ConcurrentHashMap<Tags, DistributionSummary>>()
    private val enums = ConcurrentHashMap<String, ConcurrentHashMap<Tags, Map<String, AtomicReference<Double>>>>()

    // Cardinality overflow counter (per metric name)
    private val overflowCounts = ConcurrentHashMap<String, AtomicLong>()

    @Volatile
    var cardinalityLimit: Int = 1000

    // ── Meta-metric for cardinality overflow ──
    private val overflowCounter = ConcurrentHashMap<String, AtomicLong>()

    /**
     * Dispatch a value update to the correct type-specific handler.
     * For ENUM, [value] must be a String (the current state name).
     * For all other types, [value] must be a Double.
     */
    fun update(metric: ResolvedMetric, tags: Tags, value: Any) {
        when (metric.type) {
            MetricType.GAUGE -> updateGauge(metric.name, tags, value as Double)
            MetricType.COUNTER -> updateCounter(metric.name, tags, value as Double)
            MetricType.HISTOGRAM -> updateHistogram(metric.name, tags, value as Double, metric.buckets)
            MetricType.SUMMARY -> updateSummary(metric.name, tags, value as Double)
            MetricType.ENUM -> updateEnum(metric.name, tags, value as String, metric.states)
        }
    }

    /**
     * GAUGE: last-write-wins via AtomicReference.
     */
    private fun updateGauge(name: String, tags: Tags, value: Double) {
        val byTags = gauges.computeIfAbsent(name) { ConcurrentHashMap() }
        if (exceedsCardinality(name, byTags, tags)) return

        val ref = byTags.computeIfAbsent(tags) { t ->
            val holder = AtomicReference(0.0)
            Gauge.builder(name, holder) { it.get() }
                .tags(t)
                .register(meterRegistry)
            holder
        }
        ref.set(value)
    }

    /**
     * COUNTER (monotonic from SQL): FunctionCounter exposes cumulative value.
     * Prometheus computes rate() — no need for delta computation.
     */
    private fun updateCounter(name: String, tags: Tags, value: Double) {
        val byTags = counters.computeIfAbsent(name) { ConcurrentHashMap() }
        if (exceedsCardinality(name, byTags, tags)) return

        val ref = byTags.computeIfAbsent(tags) { t ->
            val holder = AtomicReference(0.0)
            FunctionCounter.builder(name, holder) { it.get() }
                .tags(t)
                .register(meterRegistry)
            holder
        }
        ref.set(value)
    }

    /**
     * HISTOGRAM: records raw observations into DistributionSummary with SLO buckets.
     * Cumulative and append-only — correct only when SQL returns raw events.
     */
    private fun updateHistogram(name: String, tags: Tags, value: Double, buckets: List<Double>) {
        val byTags = histograms.computeIfAbsent(name) { ConcurrentHashMap() }
        if (exceedsCardinality(name, byTags, tags)) return

        val summary = byTags.computeIfAbsent(tags) { t ->
            DistributionSummary.builder(name)
                .serviceLevelObjectives(*buckets.toDoubleArray())
                .tags(t)
                .register(meterRegistry)
        }
        summary.record(value)
    }

    /**
     * SUMMARY: client-side quantile calculation via DistributionSummary with percentiles.
     * Same "raw observations only" constraint as histogram.
     */
    private fun updateSummary(name: String, tags: Tags, value: Double) {
        val byTags = summaries.computeIfAbsent(name) { ConcurrentHashMap() }
        if (exceedsCardinality(name, byTags, tags)) return

        val summary = byTags.computeIfAbsent(tags) { t ->
            DistributionSummary.builder(name)
                .publishPercentiles(0.5, 0.9, 0.95, 0.99)
                .tags(t)
                .register(meterRegistry)
        }
        summary.record(value)
    }

    /**
     * ENUM (state set): N gauges where exactly one is 1, rest are 0.
     * Update order: set new=1 FIRST, then set old=0 (brief "both 1" is safer than "both 0").
     */
    private fun updateEnum(name: String, baseTags: Tags, currentState: String, states: List<String>) {
        val byTags = enums.computeIfAbsent(name) { ConcurrentHashMap() }
        if (exceedsCardinality(name, byTags, baseTags)) return

        val stateRefs = byTags.computeIfAbsent(baseTags) { t ->
            states.associateWith { state ->
                val holder = AtomicReference(0.0)
                Gauge.builder(name, holder) { it.get() }
                    .tag(name, state)
                    .tags(t)
                    .register(meterRegistry)
                holder
            }
        }

        // Set the matching state to 1 FIRST (prefer brief "both 1" over "both 0")
        stateRefs[currentState]?.set(1.0)

        // Then set all non-matching states to 0
        for ((state, ref) in stateRefs) {
            if (state != currentState) {
                ref.set(0.0)
            }
        }

        if (currentState !in stateRefs) {
            log.warnf("ENUM metric '%s': SQL returned state '%s' not in declared states %s",
                name, currentState, states)
        }
    }

    /**
     * Cardinality guard: rejects new tag combinations beyond the ceiling.
     * Logs error and increments overflow meta-metric.
     */
    private fun <V> exceedsCardinality(
        metricName: String,
        cache: ConcurrentHashMap<Tags, V>,
        tags: Tags,
    ): Boolean {
        if (cache.size < cardinalityLimit || cache.containsKey(tags)) return false

        val count = overflowCounts.computeIfAbsent(metricName) { AtomicLong(0) }
        if (count.incrementAndGet() <= 3) {
            // Log first few overflows per metric, then suppress to avoid log flooding
            log.errorf("Cardinality overflow for metric '%s': %d unique tag combinations exceeds limit %d. Skipping tags: %s",
                metricName, cache.size, cardinalityLimit, tags)
        }

        // Increment the meta-metric counter
        meterRegistry.counter(
            "query_exporter_label_cardinality_overflow_total",
            "metric", metricName
        ).increment()

        return true
    }
}
