package com.mapreduce.testinfra

import org.h2.jdbcx.JdbcDataSource
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.sqlobject.kotlin.KotlinSqlObjectPlugin
import java.util.UUID

/**
 * Shared test infrastructure for H2 in-memory database integration tests.
 *
 * Each call to [createJdbi] creates an isolated in-memory database with
 * the full schema applied. Use [cleanTables] between tests to reset state.
 */
object H2TestSupport {

    fun createJdbi(): Jdbi {
        val dbName = "test_${UUID.randomUUID().toString().replace("-", "")}"
        val ds = JdbcDataSource().apply {
            setUrl("jdbc:h2:mem:$dbName;MODE=Oracle;DB_CLOSE_DELAY=-1")
            user = "sa"
            password = ""
        }

        val jdbi = Jdbi.create(ds).apply {
            installPlugin(KotlinPlugin())
            installPlugin(KotlinSqlObjectPlugin())
        }

        jdbi.useHandle<Exception> { h ->
            val schema = H2TestSupport::class.java.getResourceAsStream("/h2-schema.sql")!!
                .bufferedReader().readText()
            for (statement in schema.split(";").filter { it.isNotBlank() }) {
                h.execute(statement.trim())
            }
        }

        return jdbi
    }

    fun cleanTables(jdbi: Jdbi) {
        jdbi.useHandle<Exception> { h ->
            h.execute("DELETE FROM mr_output")
            h.execute("DELETE FROM task")
            h.execute("DELETE FROM mr_job")
        }
    }
}
