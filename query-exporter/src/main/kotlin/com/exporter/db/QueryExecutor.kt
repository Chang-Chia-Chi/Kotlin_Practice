package com.exporter.db

import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.SqlStatements
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

/**
 * Executes raw SQL queries against named datasources via JDBI.
 *
 * Returns schema-agnostic results as List<Map<String, Any?>> so the
 * metric extraction layer can dynamically pull columns by name.
 *
 * JDBI instances are cached per datasource to amortize plugin setup cost.
 */
@ApplicationScoped
class QueryExecutor(
    private val dataSourceResolver: DataSourceResolver,
    private val queryTimeoutSeconds: Int = DEFAULT_QUERY_TIMEOUT_SECONDS,
) {

    companion object {
        const val DEFAULT_QUERY_TIMEOUT_SECONDS = 30
    }

    private val log = Logger.getLogger(QueryExecutor::class.java)
    private val jdbiCache = ConcurrentHashMap<String, Jdbi>()

    /**
     * Executes the given SQL against the named datasource.
     *
     * @param datasourceName logical datasource name
     * @param sql raw SQL to execute
     * @return list of rows, each row a column-name → value map
     * @throws IllegalStateException if datasource is not found
     */
    fun execute(datasourceName: String, sql: String): List<Map<String, Any?>> {
        val jdbi = getOrCreateJdbi(datasourceName)

        return jdbi.withHandle<List<Map<String, Any?>>, Exception> { handle ->
            handle.getConfig(SqlStatements::class.java).queryTimeout = queryTimeoutSeconds
            handle.createQuery(sql)
                .mapToMap()
                .list()
        }
    }

    private fun getOrCreateJdbi(name: String): Jdbi {
        return jdbiCache.computeIfAbsent(name) { dsName ->
            val ds = dataSourceResolver.resolve(dsName)
                ?: throw IllegalStateException("DataSource '$dsName' not found in registry.")
            createJdbi(ds)
        }
    }

    /** Visible for testing — override to customize JDBI plugins. */
    internal fun createJdbi(dataSource: DataSource): Jdbi {
        return Jdbi.create(dataSource).apply {
            // Register column mappers for common types if needed.
            // Default JDBI handles String, Number, Timestamp, etc.
        }
    }

    /** Clears the JDBI cache. For testing. */
    fun clearCache() {
        jdbiCache.clear()
    }
}
