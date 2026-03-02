package com.exporter.engine

import com.exporter.config.MetricType
import com.exporter.config.ResolvedMetric
import com.exporter.config.ResolvedQuery
import com.exporter.db.QueryExecutor
import com.exporter.metrics.MetricStateRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.jboss.logging.Logger
import kotlin.time.Duration.Companion.seconds

/**
 * Executes a single query cycle: fetch rows, extract values, update metrics.
 *
 * Designed as a suspend function for coroutine integration.
 * I/O (JDBI query) runs on Dispatchers.IO; row processing runs on the caller's dispatcher.
 */
class QueryJob(
    private val query: ResolvedQuery,
    private val queryExecutor: QueryExecutor,
    private val metricRegistry: MetricStateRegistry,
    private val timeoutSeconds: Long = DEFAULT_TIMEOUT_SECONDS,
) {

    companion object {
        const val DEFAULT_TIMEOUT_SECONDS = 60L
    }

    private val log = Logger.getLogger(QueryJob::class.java)

    /**
     * Executes the query and updates all associated metrics.
     * Returns the number of rows processed.
     */
    suspend fun execute(): Int {
        val rows = fetchRows()
        if (rows.isEmpty()) {
            log.debugf("Query '%s' returned 0 rows", query.name)
            return 0
        }
        processRows(rows)
        return rows.size
    }

    private suspend fun fetchRows(): List<Map<String, Any?>> {
        return withContext(Dispatchers.IO) {
            try {
                withTimeout(timeoutSeconds.seconds) {
                    queryExecutor.execute(query.datasource, query.sql)
                }
            } catch (e: Exception) {
                log.errorf(e, "Query '%s' failed against datasource '%s'",
                    query.name, query.datasource)
                emptyList()
            }
        }
    }

    private fun processRows(rows: List<Map<String, Any?>>) {
        for (row in rows) {
            for (metric in query.metrics) {
                try {
                    processRow(row, metric)
                } catch (e: Exception) {
                    log.errorf(e, "Error processing row for metric '%s' in query '%s'",
                        metric.name, query.name)
                }
            }
        }
    }

    private fun processRow(row: Map<String, Any?>, metric: ResolvedMetric) {
        val tags = RowProcessor.extractTags(row, metric)

        if (metric.type == MetricType.ENUM) {
            val state = RowProcessor.extractEnumState(row, metric)
            if (state != null) {
                metricRegistry.updateEnumByState(metric, state, tags)
            }
            return
        }

        val value = RowProcessor.extractValue(row, metric) ?: return
        metricRegistry.update(metric, value, tags)
    }
}
