package com.workflow.infrastructure.queryexporter.config

data class QueryConfig(
    val sql: String,
    val datasource: String,
    val schedule: ScheduleConfig,
    val metrics: List<MetricConfig>,
)
