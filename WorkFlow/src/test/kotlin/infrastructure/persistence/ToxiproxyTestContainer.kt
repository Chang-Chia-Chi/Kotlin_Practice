package com.workflow.infrastructure.persistence

import org.testcontainers.Testcontainers
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared Toxiproxy container for all stress tests.
 * Singleton object — one container per JVM / test run.
 *
 * Mirrors OracleTestContainer. Call [newOracleProxy] once per stress test class
 * in @BeforeAll to get an isolated proxy pointing at the shared Oracle container.
 * Do NOT stop the container in @AfterAll — it lives for the JVM lifetime.
 */
object ToxiproxyTestContainer {

    private val oraclePort = OracleTestContainer.oracle.getMappedPort(1521)

    val container: ToxiproxyContainer = run {
        Testcontainers.exposeHostPorts(oraclePort)
        ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0"))
            .apply { start() }
    }

    fun newOracleProxy(): ToxiproxyContainer.ContainerProxy =
        container.getProxy("host.testcontainers.internal", oraclePort)
}
