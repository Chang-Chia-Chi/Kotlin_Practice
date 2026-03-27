package com.workflow.queryexporter.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.InputStream
import java.time.Duration

data class ExporterConfig(
    val queries: Map<String, QueryConfig>,
) {
    companion object {
        private val mapper: ObjectMapper =
            ObjectMapper(YAMLFactory())
                .registerModule(KotlinModule.Builder().build())
                .registerModule(JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        fun load(input: InputStream): ExporterConfig = mapper.readValue(input, ExporterConfig::class.java)
    }
}

data class QueryConfig(
    val sql: String,
    val datasource: String,
    val schedule: ScheduleConfig,
    val metrics: List<MetricConfig>,
)

data class ScheduleConfig(
    val interval: Duration? = null,
    val cron: String? = null,
)

data class MetricConfig(
    val name: String,
    val type: MetricType,
    val valueColumn: String,
    val tagColumns: List<String> = emptyList(),
    val buckets: List<Double> = emptyList(),
    val states: List<String> = emptyList(),
)

enum class MetricType {
    GAUGE,
    COUNTER,
    HISTOGRAM,
    SUMMARY,
    ENUM,
}

class ExporterConfigException(
    message: String,
) : IllegalArgumentException(message)

object ExporterConfigValidator {
    fun validate(config: ExporterConfig) {
        val violations = mutableListOf<String>()
        val allMetricNames = mutableSetOf<String>()

        config.queries.forEach { (queryName, queryConfig) ->
            // Non-blank SQL
            if (queryConfig.sql.isBlank()) {
                violations += "Query '$queryName': sql must not be blank"
            }

            // Non-empty metrics
            if (queryConfig.metrics.isEmpty()) {
                violations += "Query '$queryName': metrics must not be empty"
            }

            // Schedule: exactly one of interval/cron
            val schedule = queryConfig.schedule
            if (schedule.interval == null && schedule.cron == null) {
                violations += "Query '$queryName': schedule must specify either interval or cron"
            }
            if (schedule.interval != null && schedule.cron != null) {
                violations += "Query '$queryName': schedule must specify only one of interval or cron, not both"
            }

            // Positive interval
            if (schedule.interval != null && !schedule.interval.isPositive()) {
                violations += "Query '$queryName': schedule.interval must be positive"
            }

            queryConfig.metrics.forEach { metric ->
                // Non-blank metric name
                if (metric.name.isBlank()) {
                    violations += "Query '$queryName': metric name must not be blank"
                }

                // Non-blank valueColumn
                if (metric.valueColumn.isBlank()) {
                    violations += "Query '$queryName': metric '${metric.name}' valueColumn must not be blank"
                }

                // Unique metric names across entire config
                if (!allMetricNames.add(metric.name)) {
                    violations += "Query '$queryName': duplicate metric name '${metric.name}'"
                }

                // HISTOGRAM requires buckets
                if (metric.type == MetricType.HISTOGRAM && metric.buckets.isEmpty()) {
                    violations += "Query '$queryName': metric '${metric.name}' of type HISTOGRAM requires buckets"
                }

                // ENUM requires states
                if (metric.type == MetricType.ENUM && metric.states.isEmpty()) {
                    violations += "Query '$queryName': metric '${metric.name}' of type ENUM requires states"
                }
            }
        }

        if (violations.isNotEmpty()) {
            throw ExporterConfigException(violations.joinToString("; "))
        }
    }
}
