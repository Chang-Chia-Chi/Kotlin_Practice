package com.workflow.infrastructure.queryexporter.config

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
