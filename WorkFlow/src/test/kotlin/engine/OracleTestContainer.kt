package com.workflow.engine

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

    val jdbi: Jdbi by lazy {
        // Ensure Oracle JDBC driver is registered — Quarkus test classloader
        // isolation can cause DriverManager to lose the driver registration.
        Class.forName("oracle.jdbc.OracleDriver")
        Jdbi.create(oracle.jdbcUrl, oracle.username, oracle.password).also { db ->
            val loader = OracleTestContainer::class.java.classLoader
            db.useHandle<Exception> { handle ->
                handle.createScript(loader.getResource("db/migration/V1__create_workflow_tables.sql")!!.readText()).execute()
                handle.createScript(loader.getResource("db/migration/V2__add_dead_letter.sql")!!.readText()).execute()
                handle.createScript(loader.getResource("db/migration/V3__add_not_before.sql")!!.readText()).execute()
                handle.createScript(loader.getResource("db/migration/V4__cancelled_and_timeout.sql")!!.readText()).execute()
                handle.createScript(loader.getResource("db/migration/V5__enqueued_at_and_indexes.sql")!!.readText()).execute()
                handle.createScript(loader.getResource("db/migration/V6__queue_name.sql")!!.readText()).execute()
                handle.createScript(loader.getResource("db/migration/V7__waiting_for_signal.sql")!!.readText()).execute()
                handle.createScript(loader.getResource("db/migration/V8__explicit_inputs.sql")!!.readText()).execute()
            }
        }
    }
}
