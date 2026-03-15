package com.mapreduce.exporter

/**
 * Plain data classes extracted from [ExporterConfig] CDI proxies.
 * Safe to use with equals/hashCode/toString and across threads.
 */
data class ResolvedQuery(
    val name: String,
    val sql: String,
    val datasource: String,
    val intervalSeconds: Long?,
    val cron: String?,
    val metrics: List<ResolvedMetric>,
)

data class ResolvedMetric(
    val name: String,
    val type: MetricType,
    val valueColumn: String,
    val tagColumns: List<String>,
    val buckets: List<Double>,
    val states: List<String>,
)
