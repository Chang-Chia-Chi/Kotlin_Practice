package com.exporter.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import io.smallrye.config.WithName
import java.util.Optional

/**
 * Root configuration mapping for the query exporter.
 *
 * Maps from `exporter.queries.*` in application.yml.
 * Quarkus SmallRye Config automatically binds nested YAML to these interfaces.
 */
@ConfigMapping(prefix = "exporter")
interface ExporterConfig {

    /** Named query definitions keyed by logical name. */
    fun queries(): Map<String, QueryConfig>

    interface QueryConfig {
        /** Raw SQL to execute. */
        fun sql(): String

        /** Logical datasource name referencing a Quarkus-managed Agroal datasource. */
        fun datasource(): String

        /** Schedule configuration — exactly one of interval or cron must be set. */
        fun schedule(): ScheduleConfig

        /** Metrics to produce from query results. */
        fun metrics(): List<MetricConfig>
    }

    interface ScheduleConfig {
        /** Fixed-rate interval, e.g. "5s", "1m". Mutually exclusive with cron. */
        fun interval(): Optional<String>

        /** Cron expression. Mutually exclusive with interval. */
        fun cron(): Optional<String>
    }

    interface MetricConfig {
        /** Prometheus metric name (must follow naming conventions). */
        fun name(): String

        /** Metric type. */
        fun type(): MetricType

        /** Column name whose value populates the metric. */
        @WithName("value-column")
        fun valueColumn(): String

        /** Column names used as Prometheus labels. */
        @WithName("tag-columns")
        @WithDefault("")
        fun tagColumns(): Optional<List<String>>

        /** Histogram bucket boundaries. Required when type=HISTOGRAM. */
        @WithDefault("")
        fun buckets(): Optional<List<Double>>

        /** Enum state names. Required when type=ENUM. */
        @WithDefault("")
        fun states(): Optional<List<String>>
    }
}
