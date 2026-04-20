package com.workflow.infrastructure.persistence

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager

class OracleTestResource : QuarkusTestResourceLifecycleManager {
    override fun start(): Map<String, String> {
        OracleTestContainer.jdbi  // triggers container start + V1+V2 migrations
        val oracle = OracleTestContainer.oracle
        return mapOf(
            "quarkus.datasource.jdbc.url" to oracle.jdbcUrl,
            "quarkus.datasource.username" to oracle.username,
            "quarkus.datasource.password" to oracle.password,
        )
    }

    override fun stop() {}
}
