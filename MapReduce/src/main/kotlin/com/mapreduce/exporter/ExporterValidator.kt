package com.mapreduce.exporter

import io.agroal.api.AgroalDataSource
import io.quarkus.arc.Arc
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Default
import org.jboss.logging.Logger

/**
 * Validates all [ExporterConfig] at startup and converts CDI proxy objects
 * into plain [ResolvedQuery] data classes.
 *
 * Accumulates every violation and throws a single [StartupException] listing
 * all problems — operators see all config errors in one restart.
 */
@ApplicationScoped
class ExporterValidator {

    private val log = Logger.getLogger(ExporterValidator::class.java)

    private val prometheusNameRegex = Regex("^[a-zA-Z_:][a-zA-Z0-9_:]*$")
    private val prometheusLabelRegex = Regex("^[a-zA-Z_][a-zA-Z0-9_]*$")

    fun validateAndResolve(config: ExporterConfig): List<ResolvedQuery> {
        val errors = mutableListOf<String>()
        val allMetricNames = mutableSetOf<String>()
        val resolved = mutableListOf<ResolvedQuery>()

        for ((queryName, queryConfig) in config.queries()) {
            // Rule 1: Datasource reference resolves to a live CDI bean
            val dsName = queryConfig.datasource()
            if (!datasourceExists(dsName)) {
                errors.add("[$queryName] Datasource '$dsName' not found in CDI container")
            }

            // Rule 2: Schedule is XOR (exactly one of interval / cron)
            val hasInterval = queryConfig.schedule().interval().isPresent
            val hasCron = queryConfig.schedule().cron().isPresent
            if (hasInterval && hasCron) {
                errors.add("[$queryName] Both 'interval' and 'cron' specified — must be exactly one")
            } else if (!hasInterval && !hasCron) {
                errors.add("[$queryName] Neither 'interval' nor 'cron' specified — must be exactly one")
            }

            // Rule 3: SQL string is non-blank
            if (queryConfig.sql().isBlank()) {
                errors.add("[$queryName] SQL is blank")
            }

            val resolvedMetrics = mutableListOf<ResolvedMetric>()
            for ((idx, metricConfig) in queryConfig.metrics().withIndex()) {
                val mName = metricConfig.name()
                val mType = metricConfig.type()
                val valueCol = metricConfig.valueColumn()
                val tagCols = metricConfig.tagColumns().orElse(emptyList())
                val buckets = metricConfig.buckets().orElse(emptyList())
                val states = metricConfig.states().orElse(emptyList())

                // Rule 4: valueColumn not in tagColumns
                if (valueCol in tagCols) {
                    errors.add("[$queryName] metric[$idx] '$mName': valueColumn '$valueCol' is also in tagColumns")
                }

                // Rule 5: HISTOGRAM requires non-empty buckets
                if (mType == MetricType.HISTOGRAM && buckets.isEmpty()) {
                    errors.add("[$queryName] metric[$idx] '$mName': HISTOGRAM requires non-empty 'buckets'")
                }

                // Rule 6: ENUM requires non-empty states
                if (mType == MetricType.ENUM && states.isEmpty()) {
                    errors.add("[$queryName] metric[$idx] '$mName': ENUM requires non-empty 'states'")
                }

                // Rule 7: Valid Prometheus metric name
                if (!prometheusNameRegex.matches(mName)) {
                    errors.add("[$queryName] metric[$idx] '$mName': invalid Prometheus metric name (must match [a-zA-Z_:][a-zA-Z0-9_:]*)")
                }

                // Rule 7b: Tag column names must be valid Prometheus label names
                for (col in tagCols) {
                    if (!prometheusLabelRegex.matches(col)) {
                        errors.add("[$queryName] metric[$idx] '$mName': tag column '$col' is not a valid Prometheus label name")
                    }
                }

                // Rule 8: No duplicate metric names across entire config
                if (!allMetricNames.add(mName)) {
                    errors.add("[$queryName] metric[$idx] '$mName': duplicate metric name across config")
                }

                resolvedMetrics.add(
                    ResolvedMetric(
                        name = mName,
                        type = mType,
                        valueColumn = valueCol,
                        tagColumns = tagCols,
                        buckets = buckets,
                        states = states,
                    )
                )
            }

            resolved.add(
                ResolvedQuery(
                    name = queryName,
                    sql = queryConfig.sql(),
                    datasource = dsName,
                    intervalSeconds = if (hasInterval) queryConfig.schedule().interval().get().toSeconds() else null,
                    cron = if (hasCron) queryConfig.schedule().cron().get() else null,
                    metrics = resolvedMetrics,
                )
            )
        }

        if (errors.isNotEmpty()) {
            throw StartupException(
                "Query exporter config validation failed (${errors.size} error(s)):\n" +
                    errors.joinToString("\n") { "  - $it" }
            )
        }

        log.infof("Query exporter config validated: %d queries, %d metrics",
            resolved.size, resolved.sumOf { it.metrics.size })
        return resolved
    }

    private fun datasourceExists(name: String): Boolean {
        return try {
            val container = Arc.container() ?: return false
            if (name == "<default>") {
                container.select(AgroalDataSource::class.java, Default.Literal.INSTANCE).isResolvable
            } else {
                findNamedDataSource(name) != null
            }
        } catch (e: Exception) {
            log.debugf(e, "Datasource lookup failed for '%s'", name)
            false
        }
    }

    companion object {
        /**
         * Resolve a named Quarkus datasource via CDI bean qualifier inspection.
         *
         * Kotlin cannot extend Java annotation types via [AnnotationLiteral], so we
         * iterate over [AgroalDataSource] beans and match the `@DataSource("name")`
         * qualifier value via reflection.
         */
        fun findNamedDataSource(name: String): AgroalDataSource? {
            val container = Arc.container() ?: return null
            for (handle in container.select(AgroalDataSource::class.java).handles()) {
                for (qualifier in handle.bean.qualifiers) {
                    if (isMatchingDataSourceQualifier(qualifier, name)) {
                        return handle.get()
                    }
                }
            }
            return null
        }

        private fun isMatchingDataSourceQualifier(qualifier: Annotation, name: String): Boolean {
            val annotationClass = qualifier.annotationClass.java
            if (annotationClass.name != "io.quarkus.agroal.DataSource") return false
            return try {
                val valueMethod = annotationClass.getMethod("value")
                valueMethod.invoke(qualifier) as String == name
            } catch (e: Exception) {
                false
            }
        }
    }
}
