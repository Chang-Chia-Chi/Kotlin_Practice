package com.workflow.infrastructure.persistence

import org.testcontainers.Testcontainers
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared Toxiproxy container for all stress tests.
 * Singleton object — one container per JVM / test run.
 *
 * Mirrors OracleTestContainer. Call [sharedOracleProxy] from @BeforeAll to get the shared
 * proxy handle pointing at the Oracle container. Toxics are cleaned up in @AfterEach.
 * Do NOT stop the container in @AfterAll — it lives for the JVM lifetime.
 */
object ToxiproxyTestContainer {

    private val oraclePort = OracleTestContainer.oracle.getMappedPort(1521)

    val container: ToxiproxyContainer = run {
        Testcontainers.exposeHostPorts(oraclePort)
        ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0"))
            .apply { start() }
    }

    fun sharedOracleProxy(): ToxiproxyContainer.ContainerProxy =
        container.getProxy("host.testcontainers.internal", oraclePort)
}
