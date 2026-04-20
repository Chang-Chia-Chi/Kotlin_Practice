package com.mapreduce.leader

import com.mapreduce.config.FrameworkConfig
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Integration tests for [LeaderManager] using the fabric8 mock K8s API server.
 * Covers the Kubernetes code paths not exercised in dev-mode unit tests.
 */
@EnableKubernetesMockClient(crud = true)
class LeaderManagerK8sTest {

    lateinit var client: KubernetesClient

    private lateinit var config: FrameworkConfig
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var leaderManager: LeaderManager

    @BeforeEach
    fun setUp() {
        config = mock()

        val workerConfig = mock<FrameworkConfig.WorkerConfig>()
        whenever(config.worker()).thenReturn(workerConfig)
        whenever(workerConfig.id()).thenReturn("test-pod-1")

        val leaderCfg = mock<FrameworkConfig.LeaderElectionConfig>()
        whenever(config.leaderElection()).thenReturn(leaderCfg)
        whenever(leaderCfg.namespace()).thenReturn("default")
        whenever(leaderCfg.leaseName()).thenReturn("test-lease")
        whenever(leaderCfg.leaseDuration()).thenReturn(Duration.ofSeconds(15))
        whenever(leaderCfg.renewDeadline()).thenReturn(Duration.ofSeconds(10))
        whenever(leaderCfg.retryPeriod()).thenReturn(Duration.ofSeconds(2))

        val shutdownConfig = mock<FrameworkConfig.ShutdownConfig>()
        whenever(config.shutdown()).thenReturn(shutdownConfig)
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(5))

        meterRegistry = SimpleMeterRegistry()
        leaderManager = LeaderManager(config, client, meterRegistry)

        setEnv("KUBERNETES_SERVICE_HOST", "mock-k8s")
    }

    @AfterEach
    fun tearDown() {
        clearEnv("KUBERNETES_SERVICE_HOST")
        runBlocking {
            try {
                leaderManager.shutdown()
            } catch (_: Exception) {
            }
        }
    }

    // ── Election lifecycle ──────────────────────────────────────────

    @Test
    fun `onStart in K8s mode acquires leadership via lease`() {
        leaderManager.onStart(StartupEvent())

        await.atMost(30, TimeUnit.SECONDS).untilAsserted {
            assertTrue(leaderManager.isActive)
        }
        assertTrue(leaderManager.token >= 0)
    }

    @Test
    fun `shutdown cancels election and sets isActive false`() {
        leaderManager.onStart(StartupEvent())

        await.atMost(30, TimeUnit.SECONDS).untilAsserted {
            assertTrue(leaderManager.isActive)
        }

        runBlocking { leaderManager.shutdown() }

        assertFalse(leaderManager.isActive)
    }

    @Test
    fun `metrics register and reflect K8s leadership state`() {
        leaderManager.onStart(StartupEvent())

        await.atMost(30, TimeUnit.SECONDS).untilAsserted {
            assertTrue(leaderManager.isActive)
        }

        val leaderGauge = meterRegistry.find("leader_election_is_leader").gauge()
        assertNotNull(leaderGauge)
        assertEquals(1.0, leaderGauge!!.value())

        val epochGauge = meterRegistry.find("leader_election_epoch").gauge()
        assertNotNull(epochGauge)
        assertTrue(epochGauge!!.value() >= 0.0)
    }

    // ── readLeaseTransitions via mock server ────────────────────────

    @Test
    fun `readLeaseTransitions reads transitions count from mock server`() {
        val lease = LeaseBuilder()
            .withNewMetadata().withName("test-lease").withNamespace("default").endMetadata()
            .withNewSpec().withLeaseTransitions(5).withHolderIdentity("other-pod").endSpec()
            .build()
        client.resource(lease).create()

        val result = invokeReadLeaseTransitions(config.leaderElection())

        assertEquals(5L, result)
    }

    @Test
    fun `readLeaseTransitions returns 0 when lease does not exist on server`() {
        val result = invokeReadLeaseTransitions(config.leaderElection())

        assertEquals(0L, result)
    }

    // ── releaseLeaseExplicitly K8s path ─────────────────────────────

    @Test
    fun `releaseLeaseExplicitly clears holder identity on existing lease`() {
        val lease = LeaseBuilder()
            .withNewMetadata().withName("test-lease").withNamespace("default").endMetadata()
            .withNewSpec().withHolderIdentity("test-pod-1").withLeaseTransitions(1).endSpec()
            .build()
        client.resource(lease).create()

        invokeOnAcquire("test-pod-1", config.leaderElection())
        assertTrue(leaderManager.isActive)

        invokeReleaseLeaseExplicitly()

        assertFalse(leaderManager.isActive)
        val updated = client.leases().inNamespace("default").withName("test-lease").get()
        assertNull(updated.spec.holderIdentity)
    }

    @Test
    fun `releaseLeaseExplicitly handles missing lease gracefully`() {
        // Set active — readLeaseTransitions returns 0 since no lease exists
        invokeOnAcquire("test-pod-1", config.leaderElection())
        assertTrue(leaderManager.isActive)

        // No lease on server — get() returns null, skips patch
        invokeReleaseLeaseExplicitly()

        assertFalse(leaderManager.isActive)
    }

    @Test
    fun `releaseLeaseExplicitly handles exception gracefully`() {
        val brokenClient = mock<KubernetesClient>()
        whenever(brokenClient.leases()).thenThrow(RuntimeException("connection refused"))

        val brokenManager = LeaderManager(config, brokenClient, meterRegistry)

        // onAcquire uses readLeaseTransitions which catches the error and falls back
        invokeOnAcquire("test-pod-1", config.leaderElection(), target = brokenManager)
        assertTrue(brokenManager.isActive)

        // releaseLeaseExplicitly catches the exception from brokenClient.leases()
        invokeReleaseLeaseExplicitly(brokenManager)

        assertFalse(brokenManager.isActive)
    }

    // ── electionLoop retry paths ─────────────────────────────────────

    @Test
    fun `electionLoop retries after runElection throws`() {
        val failingClient = mock<KubernetesClient>()
        whenever(failingClient.leaderElector()).thenThrow(RuntimeException("API unavailable"))

        val fastConfig = createFastRetryConfig()
        val mgr = LeaderManager(fastConfig, failingClient, SimpleMeterRegistry())
        mgr.onStart(StartupEvent())

        // Wait for at least 2 retry cycles — proves error path + delay + loop-back
        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            verify(failingClient, atLeast(2)).leaderElector()
        }

        assertFalse(mgr.isActive)
        runBlocking { mgr.shutdown() }
    }

    @Test
    fun `electionLoop retries after runElection returns normally`() {
        // Deep stubs: leaderElector().withConfig(any()).build().run() all return mocks;
        // run() is void so it returns immediately, simulating a normal election exit.
        val stubbedClient = mock<KubernetesClient>(defaultAnswer = RETURNS_DEEP_STUBS)

        val fastConfig = createFastRetryConfig()
        val mgr = LeaderManager(fastConfig, stubbedClient, SimpleMeterRegistry())
        mgr.onStart(StartupEvent())

        // Wait for at least 2 cycles — proves normal-return path + "will retry" + delay + loop-back
        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            verify(stubbedClient, atLeast(2)).leaderElector()
        }

        runBlocking { mgr.shutdown() }
    }

    private fun createFastRetryConfig(): FrameworkConfig {
        val cfg = mock<FrameworkConfig>()
        val workerCfg = mock<FrameworkConfig.WorkerConfig>()
        whenever(cfg.worker()).thenReturn(workerCfg)
        whenever(workerCfg.id()).thenReturn("test-pod-1")

        val leaderCfg = mock<FrameworkConfig.LeaderElectionConfig>()
        whenever(cfg.leaderElection()).thenReturn(leaderCfg)
        whenever(leaderCfg.namespace()).thenReturn("default")
        whenever(leaderCfg.leaseName()).thenReturn("test-lease")
        whenever(leaderCfg.leaseDuration()).thenReturn(Duration.ofSeconds(15))
        whenever(leaderCfg.renewDeadline()).thenReturn(Duration.ofSeconds(10))
        whenever(leaderCfg.retryPeriod()).thenReturn(Duration.ofMillis(100))

        val shutdownCfg = mock<FrameworkConfig.ShutdownConfig>()
        whenever(cfg.shutdown()).thenReturn(shutdownCfg)
        whenever(shutdownCfg.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(5))

        return cfg
    }

    // ── Reflection helpers ──────────────────────────────────────────

    private fun invokeReadLeaseTransitions(leaderCfg: FrameworkConfig.LeaderElectionConfig): Long {
        val method = LeaderManager::class.java.getDeclaredMethod(
            "readLeaseTransitions", FrameworkConfig.LeaderElectionConfig::class.java,
        )
        method.isAccessible = true
        return method.invoke(leaderManager, leaderCfg) as Long
    }

    private fun invokeOnAcquire(
        identity: String,
        leaderCfg: FrameworkConfig.LeaderElectionConfig,
        target: LeaderManager = leaderManager,
    ) {
        val method = LeaderManager::class.java.getDeclaredMethod(
            "onAcquire", String::class.java, FrameworkConfig.LeaderElectionConfig::class.java,
        )
        method.isAccessible = true
        method.invoke(target, identity, leaderCfg)
    }

    private fun invokeReleaseLeaseExplicitly(target: LeaderManager = leaderManager) {
        val method = LeaderManager::class.java.getDeclaredMethod("releaseLeaseExplicitly")
        method.isAccessible = true
        method.invoke(target)
    }

    companion object {
        private fun setEnv(key: String, value: String) {
            val clazz = Class.forName("java.lang.ProcessEnvironment")
            try {
                // Windows: case-insensitive TreeMap queried by System.getenv(String)
                val field = clazz.getDeclaredField("theCaseInsensitiveEnvironment")
                field.isAccessible = true
                @Suppress("UNCHECKED_CAST")
                (field.get(null) as MutableMap<String, String>)[key] = value
            } catch (_: NoSuchFieldException) {
                // Linux/Mac: backing HashMap behind the unmodifiable view
                val field = clazz.getDeclaredField("theEnvironment")
                field.isAccessible = true
                @Suppress("UNCHECKED_CAST")
                (field.get(null) as MutableMap<String, String>)[key] = value
            }
        }

        private fun clearEnv(key: String) {
            val clazz = Class.forName("java.lang.ProcessEnvironment")
            try {
                val field = clazz.getDeclaredField("theCaseInsensitiveEnvironment")
                field.isAccessible = true
                @Suppress("UNCHECKED_CAST")
                (field.get(null) as MutableMap<String, String>).remove(key)
            } catch (_: NoSuchFieldException) {
                val field = clazz.getDeclaredField("theEnvironment")
                field.isAccessible = true
                @Suppress("UNCHECKED_CAST")
                (field.get(null) as MutableMap<String, String>).remove(key)
            }
        }
    }
}
