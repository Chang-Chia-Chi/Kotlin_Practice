package com.exporter.engine

import com.exporter.config.MetricType
import com.exporter.config.ResolvedMetric
import org.jboss.logging.Logger

/**
 * Extracts numeric values and label tags from raw query result rows.
 *
 * Handles type coercion from database types (Integer, Long, BigDecimal,
 * String-encoded numbers) to Double for Micrometer metrics.
 */
object RowProcessor {

    private val log = Logger.getLogger(RowProcessor::class.java)

    /**
     * Extracts the metric value from a result row.
     * Returns null if the column is missing or not coercible to a number.
     */
    fun extractValue(row: Map<String, Any?>, metric: ResolvedMetric): Double? {
        val raw = findColumn(row, metric.valueColumn)
        if (raw == null) {
            log.warnf("Column '%s' is null or missing in row for metric '%s'",
                metric.valueColumn, metric.name)
            return null
        }
        return coerceToDouble(raw, metric.valueColumn, metric.name)
    }

    /**
     * For ENUM metrics, extracts the state string directly from the value column.
     */
    fun extractEnumState(row: Map<String, Any?>, metric: ResolvedMetric): String? {
        val raw = findColumn(row, metric.valueColumn)
        return raw?.toString()
    }

    /**
     * Extracts tag (label) values from a result row.
     * Missing columns result in "unknown" to prevent metric explosion from null labels.
     */
    fun extractTags(row: Map<String, Any?>, metric: ResolvedMetric): Map<String, String> {
        return metric.tagColumns.associateWith { col ->
            findColumn(row, col)?.toString() ?: "unknown"
        }
    }

    /**
     * Case-insensitive column lookup. Database drivers may return column names
     * in different cases depending on the DB engine.
     */
    private fun findColumn(row: Map<String, Any?>, column: String): Any? {
        // Fast path: exact match (handles null values correctly)
        if (row.containsKey(column)) return row[column]
        // Slow path: case-insensitive
        return row.entries.firstOrNull { it.key.equals(column, ignoreCase = true) }?.value
    }

    private fun coerceToDouble(value: Any, column: String, metricName: String): Double? {
        return when (value) {
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull().also {
                if (it == null) {
                    log.warnf("Cannot coerce string '%s' from column '%s' to number for metric '%s'",
                        value, column, metricName)
                }
            }
            else -> {
                log.warnf("Unsupported type %s for column '%s' in metric '%s'",
                    value.javaClass.simpleName, column, metricName)
                null
            }
        }
    }
}
