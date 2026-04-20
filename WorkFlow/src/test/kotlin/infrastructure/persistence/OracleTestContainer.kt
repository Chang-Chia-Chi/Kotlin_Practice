package com.workflow.infrastructure.persistence

import org.jdbi.v3.core.ConnectionFactory
import org.jdbi.v3.core.Jdbi
import org.testcontainers.oracle.OracleContainer
import java.sql.Connection
import java.sql.DriverManager

/**
 * Shared Oracle Free container for all engine tests.
 * Singleton object — one container per JVM / test run.
 *
 * The container is configured to run in Asia/Taipei (+08:00) so `SYSTIMESTAMP`
 * and TIMESTAMP column storage match production behavior (see [DB_ZONE]).
 * Each JDBC connection also pins its session time zone to +08:00 so the
 * `TIMESTAMP WITH TIME ZONE` → `TIMESTAMP` conversion is deterministic.
 */
object OracleTestContainer {

    val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:23-slim-faststart")
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass")
        .withEnv("TZ", "Asia/Taipei")
        .apply { start() }

    private val migrations = listOf(
        "db/migration/V1__create_workflow_tables.sql",
        "db/migration/V2__create_dispatch_tables.sql",
    )

    val jdbi: Jdbi by lazy {
        // Ensure Oracle JDBC driver is registered — Quarkus test classloader
        // isolation can cause DriverManager to lose the driver registration.
        Class.forName("oracle.jdbc.OracleDriver")
        val factory = ConnectionFactory {
            val conn: Connection = DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)
            conn.createStatement().use { it.execute("ALTER SESSION SET TIME_ZONE = '+08:00'") }
            conn
        }
        Jdbi.create(factory).also { db ->
            val loader = OracleTestContainer::class.java.classLoader
            db.useHandle<Exception> { handle ->
                migrations.forEach { path ->
                    handle.createScript(loader.getResource(path)!!.readText()).execute()
                }
            }
        }
    }
}
