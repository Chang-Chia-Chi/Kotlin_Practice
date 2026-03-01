package com.exporter.validation

import com.exporter.config.*
import com.exporter.db.DataSourceResolver
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Duration

/**
 * Validates the entire [ExporterConfig] at startup.
 *
 * Collects ALL validation errors before failing, so operators see every problem
 * in a single log message rather than fixing them one at a time.
 *
 * This class is deliberately stateless and side-effect-free (pure validation).
 * It converts raw config interfaces into [ResolvedQuery] domain models on success.
 */
@ApplicationScoped
class ConfigValidator(
    private val dataSourceResolver: DataSourceResolver,
) {

    private val log = Logger.getLogger(ConfigValidator::class.java)

    /**
     * Validates config and returns resolved queries.
     * @throws ConfigValidationException if any rule fails.
     */
    fun validate(config: ExporterConfig): List<ResolvedQuery> {
        val errors = mutableListOf<String>()
        val resolved = mutableListOf<ResolvedQuery>()

        val queries = config.queries()
        if (queries.isEmpty()) {
            errors.add("No queries defined in configuration.")
        }

        for ((queryName, queryConfig) in queries) {
            validateQuery(queryName, queryConfig, errors)?.let { resolved.add(it) }
        }

        if (errors.isNotEmpty()) {
            throw ConfigValidationException(errors)
        }

        log.infof("Configuration validated: %d queries, %d total metrics",
            resolved.size, resolved.sumOf { it.metrics.size })
        return resolved
    }

    private fun validateQuery(
        name: String,
        query: ExporterConfig.QueryConfig,
        errors: MutableList<String>,
    ): ResolvedQuery? {
        var valid = true

        // Rule: SQL must not be empty
        if (query.sql().isBlank()) {
            errors.add("Query '$name' has empty SQL.")
            valid = false
        }

        // Rule: Datasource must be resolvable
        if (dataSourceResolver.resolve(query.datasource()) == null) {
            errors.add("Query '$name': datasource '${query.datasource()}' not found in Quarkus registry.")
            valid = false
        }

        // Rule: Schedule must be exactly one of interval OR cron
        val schedule = validateSchedule(name, query.schedule(), errors)
        if (schedule == null) valid = false

        // Rule: At least one metric
        if (query.metrics().isEmpty()) {
            errors.add("Query '$name' has no metrics defined.")
            valid = false
        }

        // Validate each metric
        val resolvedMetrics = query.metrics().mapNotNull { metricConfig ->
            validateMetric(name, metricConfig, errors)
        }

        if (resolvedMetrics.size != query.metrics().size) {
            valid = false
        }

        return if (valid && schedule != null) {
            ResolvedQuery(
                name = name,
                sql = query.sql(),
                datasource = query.datasource(),
                schedule = schedule,
                metrics = resolvedMetrics,
            )
        } else {
            null
        }
    }

    internal fun validateSchedule(
        queryName: String,
        schedule: ExporterConfig.ScheduleConfig,
        errors: MutableList<String>,
    ): ResolvedSchedule? {
        val hasInterval = schedule.interval().isPresent && schedule.interval().get().isNotBlank()
        val hasCron = schedule.cron().isPresent && schedule.cron().get().isNotBlank()

        return when {
            hasInterval && hasCron -> {
                errors.add("Query '$queryName' has ambiguous schedule: both interval and cron are set.")
                null
            }
            !hasInterval && !hasCron -> {
                errors.add("Query '$queryName' has no schedule: set either interval or cron.")
                null
            }
            hasInterval -> {
                val raw = schedule.interval().get()
                val duration = parseDuration(raw)
                if (duration == null || duration.isZero || duration.isNegative) {
                    errors.add("Query '$queryName' has invalid interval: '$raw'.")
                    null
                } else {
                    ResolvedSchedule(interval = duration, cron = null)
                }
            }
            else -> {
                ResolvedSchedule(interval = null, cron = schedule.cron().get())
            }
        }
    }

    internal fun validateMetric(
        queryName: String,
        metric: ExporterConfig.MetricConfig,
        errors: MutableList<String>,
    ): ResolvedMetric? {
        var valid = true
        val metricName = metric.name()
        val tagColumns = metric.tagColumns().orElse(emptyList())

        // Rule: metric name must not be blank
        if (metricName.isBlank()) {
            errors.add("Query '$queryName' has a metric with empty name.")
            valid = false
        } else if (!PROMETHEUS_NAME_REGEX.matches(metricName)) {
            errors.add(
                "Query '$queryName', metric '$metricName': " +
                    "name must match Prometheus format [a-zA-Z_:][a-zA-Z0-9_:]*."
            )
            valid = false
        }

        // Rule: valueColumn must not be blank
        if (metric.valueColumn().isBlank()) {
            errors.add("Query '$queryName', metric '$metricName': valueColumn is empty.")
            valid = false
        }

        // Rule: valueColumn must not appear in tagColumns
        if (tagColumns.contains(metric.valueColumn())) {
            errors.add(
                "Query '$queryName', metric '$metricName': " +
                    "column '${metric.valueColumn()}' cannot be both value and tag."
            )
            valid = false
        }

        // Rule: HISTOGRAM requires non-empty buckets
        if (metric.type() == MetricType.HISTOGRAM) {
            val buckets = metric.buckets().orElse(emptyList())
            if (buckets.isEmpty()) {
                errors.add("Query '$queryName', histogram metric '$metricName' missing buckets.")
                valid = false
            }
        }

        // Rule: ENUM requires non-empty states
        if (metric.type() == MetricType.ENUM) {
            val states = metric.states().orElse(emptyList())
            if (states.isEmpty()) {
                errors.add("Query '$queryName', enum metric '$metricName' missing states.")
                valid = false
            }
        }

        return if (valid) {
            ResolvedMetric(
                name = metricName,
                type = metric.type(),
                valueColumn = metric.valueColumn(),
                tagColumns = tagColumns,
                buckets = metric.buckets().orElse(emptyList()),
                states = metric.states().orElse(emptyList()),
            )
        } else {
            null
        }
    }

    companion object {
        /** Prometheus metric name must match this pattern. */
        private val PROMETHEUS_NAME_REGEX = Regex("^[a-zA-Z_:][a-zA-Z0-9_:]*$")
        /**
         * Parses duration strings like "5s", "1m", "500ms", "2h".
         * Falls back to ISO-8601 Duration parsing.
         */
        fun parseDuration(raw: String): Duration? {
            return try {
                val trimmed = raw.trim().lowercase()
                when {
                    trimmed.endsWith("ms") -> Duration.ofMillis(trimmed.dropLast(2).toLong())
                    trimmed.endsWith("s") -> Duration.ofSeconds(trimmed.dropLast(1).toLong())
                    trimmed.endsWith("m") -> Duration.ofMinutes(trimmed.dropLast(1).toLong())
                    trimmed.endsWith("h") -> Duration.ofHours(trimmed.dropLast(1).toLong())
                    else -> Duration.parse(trimmed) // ISO-8601 fallback
                }
            } catch (e: Exception) {
                null
            }
        }
    }
}
