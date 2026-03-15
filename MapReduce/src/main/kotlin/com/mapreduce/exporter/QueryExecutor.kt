package com.mapreduce.exporter

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.withContext
import org.jdbi.v3.core.Jdbi
import org.jboss.logging.Logger
import java.util.concurrent.atomic.AtomicReference

/**
 * Executes a single [ResolvedQuery] against JDBI, transforms rows into
 * Micrometer metric updates, and records meta-metrics for self-observability.
 *
 * Each execution follows four phases:
 * 1. **I/O** — Run SQL via JDBI on the IO dispatcher
 * 2. **Transform** — Extract valueColumn / tagColumns from each row
 * 3. **Update** — Push values into [MetricBridge]
 * 4. **Meta** — Record success/failure, duration, and row count
 */
class QueryExecutor(
    private val metricBridge: MetricBridge,
    private val meterRegistry: MeterRegistry,
    private val ioDispatcher: kotlinx.coroutines.CoroutineDispatcher,
) {

    private val log = Logger.getLogger(QueryExecutor::class.java)

    // Meta-metric holders: queryName → AtomicReference
    private val lastRunSuccess = HashMap<String, AtomicReference<Double>>()
    private val lastRunTimestamp = HashMap<String, AtomicReference<Double>>()

    /**
     * Register meta-metrics for a set of queries. Called once at startup.
     */
    fun registerMetaMetrics(queries: List<ResolvedQuery>) {
        for (query in queries) {
            val successRef = AtomicReference(0.0)
            lastRunSuccess[query.name] = successRef
            Gauge.builder("query_exporter_query_last_run_success", successRef) { it.get() }
                .tag("query", query.name)
                .register(meterRegistry)

            val timestampRef = AtomicReference(0.0)
            lastRunTimestamp[query.name] = timestampRef
            Gauge.builder("query_exporter_query_last_run_timestamp_seconds", timestampRef) { it.get() }
                .tag("query", query.name)
                .register(meterRegistry)
        }
    }

    /**
     * Execute a query and update all associated metrics.
     * Never throws — exceptions are caught, logged, and reflected in meta-metrics.
     */
    suspend fun execute(query: ResolvedQuery, jdbi: Jdbi) {
        val sample = Timer.start(meterRegistry)
        var success = false
        var rowCount = 0L

        try {
            // Phase 1: I/O — execute SQL on the bounded IO dispatcher
            val rows = withContext(ioDispatcher) {
                jdbi.withHandle<List<Map<String, Any>>, Exception> { handle ->
                    handle.createQuery(query.sql)
                        .mapToMap()
                        .list()
                }
            }

            rowCount = rows.size.toLong()

            // Phase 2 + 3: Transform and Update — extract columns, push to metric bridge
            for (row in rows) {
                for (metric in query.metrics) {
                    try {
                        processRow(row, metric)
                    } catch (e: Exception) {
                        log.warnf(e, "[%s] Error processing row for metric '%s': %s",
                            query.name, metric.name, row)
                    }
                }
            }

            success = true
            if (rows.isEmpty()) {
                log.debugf("[%s] Query returned 0 rows", query.name)
            }
        } catch (e: Exception) {
            log.errorf(e, "[%s] Query execution failed (datasource=%s)", query.name, query.datasource)
        } finally {
            // Phase 4: Meta-metrics — always update regardless of success/failure
            lastRunSuccess[query.name]?.set(if (success) 1.0 else 0.0)
            lastRunTimestamp[query.name]?.set(System.currentTimeMillis() / 1000.0)

            sample.stop(
                Timer.builder("query_exporter_query_duration_seconds")
                    .tag("query", query.name)
                    .tag("datasource", query.datasource)
                    .register(meterRegistry)
            )

            meterRegistry.counter(
                "query_exporter_query_rows_total",
                "query", query.name
            ).increment(rowCount.toDouble())
        }
    }

    /**
     * Extract value and tags from a single result row and push to the metric bridge.
     * Column lookup is case-insensitive to handle Oracle (UPPER) vs Postgres (lower).
     */
    private fun processRow(row: Map<String, Any>, metric: ResolvedMetric) {
        // Build case-insensitive column index
        val ciRow = CaseInsensitiveMap(row)

        // Build Micrometer tags from tagColumns
        val tagPairs = metric.tagColumns.flatMap { col ->
            val tagValue = ciRow[col]?.toString() ?: ""
            listOf(col, tagValue)
        }
        val tags = if (tagPairs.isEmpty()) Tags.empty() else Tags.of(*tagPairs.toTypedArray())

        // Extract value and dispatch to MetricBridge
        val rawValue = ciRow[metric.valueColumn]
            ?: throw IllegalArgumentException("Column '${metric.valueColumn}' not found in row. Available: ${row.keys}")

        if (metric.type == MetricType.ENUM) {
            metricBridge.update(metric, tags, rawValue.toString())
        } else {
            val numericValue = coerceToDouble(rawValue, metric.name, metric.valueColumn)
            metricBridge.update(metric, tags, numericValue)
        }
    }

    private fun coerceToDouble(value: Any, metricName: String, column: String): Double {
        return when (value) {
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull()
                ?: throw IllegalArgumentException(
                    "Metric '$metricName': column '$column' value '$value' is not numeric"
                )
            else -> throw IllegalArgumentException(
                "Metric '$metricName': column '$column' has unsupported type ${value::class.simpleName}"
            )
        }
    }

    /**
     * Case-insensitive wrapper around a Map for cross-database column name compatibility.
     * Oracle returns UPPER_CASE, PostgreSQL returns lower_case, MySQL preserves alias case.
     */
    private class CaseInsensitiveMap(private val delegate: Map<String, Any>) {
        private val lowerKeyMap: Map<String, Any> by lazy {
            delegate.entries.associate { (k, v) -> k.lowercase() to v }
        }

        operator fun get(key: String): Any? =
            delegate[key] ?: lowerKeyMap[key.lowercase()]
    }

}
