package com.mapreduce.exporter

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import io.smallrye.config.WithName
import java.time.Duration
import java.util.Optional

/**
 * Config-driven SQL-to-Prometheus metric exporter.
 *
 * Example YAML:
 * ```yaml
 * query-exporter:
 *   cardinality-limit: 1000
 *   io-parallelism: 8
 *   queries:
 *     active-sessions:
 *       sql: "SELECT region, COUNT(*) AS cnt FROM sessions WHERE active = 1 GROUP BY region"
 *       datasource: "<default>"
 *       schedule:
 *         interval: "30S"
 *       metrics:
 *         - name: "app_active_sessions"
 *           type: GAUGE
 *           value-column: "cnt"
 *           tag-columns:
 *             - "region"
 * ```
 */
@ConfigMapping(prefix = "query-exporter")
interface ExporterConfig {

    fun queries(): Map<String, QueryConfig>

    @WithName("cardinality-limit")
    @WithDefault("1000")
    fun cardinalityLimit(): Int

    @WithName("io-parallelism")
    @WithDefault("8")
    fun ioParallelism(): Int

    interface QueryConfig {
        fun sql(): String
        fun datasource(): String
        fun schedule(): ScheduleConfig
        fun metrics(): List<MetricConfig>
    }

    interface ScheduleConfig {
        fun interval(): Optional<Duration>
        fun cron(): Optional<String>
    }

    interface MetricConfig {
        fun name(): String
        fun type(): MetricType

        @WithName("value-column")
        fun valueColumn(): String

        @WithName("tag-columns")
        fun tagColumns(): Optional<List<String>>

        fun buckets(): Optional<List<Double>>
        fun states(): Optional<List<String>>
    }
}
