package com.workflow.infrastructure.queryexporter.config

data class MetricConfig(
    val name: String,
    val type: MetricType,
    val valueColumn: String,
    val tagColumns: List<String> = emptyList(),
    val buckets: List<Double> = emptyList(),
    val states: List<String> = emptyList(),
)
