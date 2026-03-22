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
        Jdbi.create(oracle.jdbcUrl, oracle.username, oracle.password).also { db ->
            val sql = OracleTestContainer::class.java.classLoader
                .getResource("db/migration/V1__create_workflow_tables.sql")!!
                .readText()
            db.useHandle<Exception> { it.createScript(sql).execute() }
        }
    }
}
