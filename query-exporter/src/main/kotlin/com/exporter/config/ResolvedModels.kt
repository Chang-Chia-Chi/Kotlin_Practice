package com.exporter.config

import java.time.Duration

/**
 * Validated, resolved query definition ready for scheduling.
 * Created after validation passes — these are guaranteed correct.
 */
data class ResolvedQuery(
    val name: String,
    val sql: String,
    val datasource: String,
    val schedule: ResolvedSchedule,
    val metrics: List<ResolvedMetric>,
)

/**
 * Exactly one of [interval] or [cron] is non-null (enforced by validation).
 */
data class ResolvedSchedule(
    val interval: Duration?,
    val cron: String?,
) {
    init {
        require((interval != null) xor (cron != null)) {
            "Exactly one of interval or cron must be set"
        }
    }
}

/**
 * Validated metric definition with type-specific constraints already checked.
 */
data class ResolvedMetric(
    val name: String,
    val type: MetricType,
    val valueColumn: String,
    val tagColumns: List<String>,
    val buckets: List<Double>,
    val states: List<String>,
)
