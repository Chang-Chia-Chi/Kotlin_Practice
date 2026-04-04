package com.workflow.infrastructure.persistence

import org.jdbi.v3.core.Jdbi
import org.testcontainers.oracle.OracleContainer

/**
 * Shared Oracle Free container for all engine tests.
 * Singleton object — one container per JVM / test run.
 */
object OracleTestContainer {

    val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:23-slim-faststart")
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass")
        .apply { start() }

    private val migrations = listOf(
        "db/migration/V1__create_workflow_tables.sql",
        "db/migration/V2__create_dispatch_tables.sql",
    )

    val jdbi: Jdbi by lazy {
        // Ensure Oracle JDBC driver is registered — Quarkus test classloader
        // isolation can cause DriverManager to lose the driver registration.
        Class.forName("oracle.jdbc.OracleDriver")
        Jdbi.create(oracle.jdbcUrl, oracle.username, oracle.password).also { db ->
            val loader = OracleTestContainer::class.java.classLoader
            db.useHandle<Exception> { handle ->
                migrations.forEach { path ->
                    handle.createScript(loader.getResource(path)!!.readText()).execute()
                }
            }
        }
    }
}
