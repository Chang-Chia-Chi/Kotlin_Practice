package com.mapreduce.leader

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.shutdown.ShutdownParticipant
import io.fabric8.kubernetes.api.model.coordination.v1.Lease
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseSpec
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.MixedOperation
import io.fabric8.kubernetes.client.dsl.Resource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration

class LeaderManagerTest {

    private lateinit var config: FrameworkConfig
    private lateinit var kubernetesClient: KubernetesClient
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var leaderManager: LeaderManager

    @BeforeEach
    fun setUp() {
        config = mock()
        val shutdownConfig = mock<FrameworkConfig.ShutdownConfig>()
        whenever(config.shutdown()).thenReturn(shutdownConfig)
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(5))

        kubernetesClient = mock()
        meterRegistry = SimpleMeterRegistry()
        leaderManager = LeaderManager(config, kubernetesClient, meterRegistry)
    }

    // ── Initial state ─────────────────────────────────────────────

    @Nested
    inner class InitialState {

        @Test
        fun `isActive is false before onStart`() {
            assertFalse(leaderManager.isActive)
        }

        @Test
        fun `token is 0 before onStart`() {
            assertEquals(0L, leaderManager.token)
        }

        @Test
        fun `lastHeartbeat is set before onStart`() {
            assertNotNull(leaderManager.lastHeartbeat)
        }
    }

    // ── Dev mode startup ──────────────────────────────────────────

    @Nested
    inner class DevModeStartup {

        @Test
        fun `onStart sets isActive to true without Kubernetes`() {
            leaderManager.onStart(StartupEvent())

            assertTrue(leaderManager.isActive)
        }

        @Test
        fun `onStart sets token to 1 as synthetic epoch`() {
            leaderManager.onStart(StartupEvent())

            assertEquals(1L, leaderManager.token)
        }

        @Test
        fun `onStart registers metric gauges`() {
            leaderManager.onStart(StartupEvent())

            val leaderGauge = meterRegistry.find("leader_election_is_leader").gauge()
            assertNotNull(leaderGauge)

            val epochGauge = meterRegistry.find("leader_election_epoch").gauge()
            assertNotNull(epochGauge)
        }
    }

    // ── Metrics ───────────────────────────────────────────────────

    @Nested
    inner class Metrics {

        @Test
        fun `leader gauge shows 1 when active`() {
            leaderManager.onStart(StartupEvent())

            val gauge = meterRegistry.find("leader_election_is_leader").gauge()
            assertNotNull(gauge)
            assertEquals(1.0, gauge!!.value())
        }

        @Test
        fun `epoch gauge shows current epoch`() {
            leaderManager.onStart(StartupEvent())

            val gauge = meterRegistry.find("leader_election_epoch").gauge()
            assertNotNull(gauge)
            assertEquals(1.0, gauge!!.value())
        }

        @Test
        fun `leader gauge shows 0 when not active`() {
            leaderManager.onStart(StartupEvent())

            val onLose = LeaderManager::class.java.getDeclaredMethod("onLose")
            onLose.isAccessible = true
            onLose.invoke(leaderManager)

            val gauge = meterRegistry.find("leader_election_is_leader").gauge()
            assertNotNull(gauge)
            assertEquals(0.0, gauge!!.value())
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────

    @Nested
    inner class Shutdown {

        @Test
        fun `shutdown sets isActive to false`() = runBlocking {
            leaderManager.onStart(StartupEvent())
            assertTrue(leaderManager.isActive)

            leaderManager.shutdown()

            assertFalse(leaderManager.isActive)
        }

        @Test
        fun `shutdownOrder is 0`() {
            assertEquals(0, leaderManager.shutdownOrder)
        }

        @Test
        fun `shutdownTimeout comes from config`() {
            assertEquals(Duration.ofSeconds(5), leaderManager.shutdownTimeout)
        }
    }

    // ── ShutdownParticipant interface ─────────────────────────────

    @Test
    fun `implements ShutdownParticipant`() {
        assertTrue(leaderManager is ShutdownParticipant)
    }

    // ── K8s callbacks via reflection ──────────────────────────────

    @Nested
    inner class K8sCallbacks {

        private fun mockLeaderConfig(): FrameworkConfig.LeaderElectionConfig {
            val leaderCfg = mock<FrameworkConfig.LeaderElectionConfig>()
            whenever(leaderCfg.namespace()).thenReturn("default")
            whenever(leaderCfg.leaseName()).thenReturn("test-lease")
            whenever(leaderCfg.leaseDuration()).thenReturn(Duration.ofSeconds(15))
            whenever(leaderCfg.renewDeadline()).thenReturn(Duration.ofSeconds(10))
            whenever(leaderCfg.retryPeriod()).thenReturn(Duration.ofSeconds(2))
            return leaderCfg
        }

        @Suppress("UNCHECKED_CAST")
        private fun mockLeaseChain(transitions: Int) {
            val leasesOp = mock<MixedOperation<Lease, *, *>>()
            val namespaceable = mock<MixedOperation<Lease, *, *>>()
            val resource = mock<Resource<Lease>>()
            val lease = mock<Lease>()
            val spec = mock<LeaseSpec>()

            whenever(kubernetesClient.leases()).thenReturn(leasesOp as MixedOperation<Lease, io.fabric8.kubernetes.api.model.coordination.v1.LeaseList, Resource<Lease>>)
            whenever(leasesOp.inNamespace(any())).thenReturn(namespaceable as MixedOperation<Lease, io.fabric8.kubernetes.api.model.coordination.v1.LeaseList, Resource<Lease>>)
            whenever(namespaceable.withName(any())).thenReturn(resource)
            whenever(resource.get()).thenReturn(lease)
            whenever(lease.spec).thenReturn(spec)
            whenever(spec.leaseTransitions).thenReturn(transitions)
        }

        private fun invokeOnAcquire(identity: String, leaderCfg: FrameworkConfig.LeaderElectionConfig) {
            val method = LeaderManager::class.java.getDeclaredMethod(
                "onAcquire", String::class.java, FrameworkConfig.LeaderElectionConfig::class.java,
            )
            method.isAccessible = true
            method.invoke(leaderManager, identity, leaderCfg)
        }

        private fun invokeOnLose() {
            val method = LeaderManager::class.java.getDeclaredMethod("onLose")
            method.isAccessible = true
            method.invoke(leaderManager)
        }

        private fun invokeReadLeaseTransitions(leaderCfg: FrameworkConfig.LeaderElectionConfig): Long {
            val method = LeaderManager::class.java.getDeclaredMethod(
                "readLeaseTransitions", FrameworkConfig.LeaderElectionConfig::class.java,
            )
            method.isAccessible = true
            return method.invoke(leaderManager, leaderCfg) as Long
        }

        @Test
        fun `onAcquire sets isActive and epoch from lease transitions`() {
            val leaderCfg = mockLeaderConfig()
            mockLeaseChain(transitions = 7)

            invokeOnAcquire("test-pod", leaderCfg)

            assertTrue(leaderManager.isActive)
            assertEquals(7L, leaderManager.token)
        }

        @Test
        fun `onLose sets isActive to false`() {
            // First make it active
            leaderManager.onStart(StartupEvent())
            assertTrue(leaderManager.isActive)

            invokeOnLose()

            assertFalse(leaderManager.isActive)
        }

        @Test
        fun `readLeaseTransitions returns lease transitions count`() {
            val leaderCfg = mockLeaderConfig()
            mockLeaseChain(transitions = 42)

            val epoch = invokeReadLeaseTransitions(leaderCfg)

            assertEquals(42L, epoch)
        }

        @Test
        fun `readLeaseTransitions falls back to local increment on error`() {
            val leaderCfg = mockLeaderConfig()
            whenever(kubernetesClient.leases()).thenThrow(RuntimeException("K8s API unavailable"))

            // Set initial epoch to 5
            leaderManager.onStart(StartupEvent()) // sets epoch=1 in dev mode

            val epoch = invokeReadLeaseTransitions(leaderCfg)

            assertEquals(2L, epoch) // _epoch.value (1) + 1
        }

        @Test
        fun `readLeaseTransitions returns 0 when lease spec is null`() {
            val leaderCfg = mockLeaderConfig()

            @Suppress("UNCHECKED_CAST")
            val leasesOp = mock<MixedOperation<Lease, *, *>>()
            val namespaceable = mock<MixedOperation<Lease, *, *>>()
            val resource = mock<Resource<Lease>>()
            val lease = mock<Lease>()

            whenever(kubernetesClient.leases()).thenReturn(leasesOp as MixedOperation<Lease, io.fabric8.kubernetes.api.model.coordination.v1.LeaseList, Resource<Lease>>)
            whenever(leasesOp.inNamespace(any())).thenReturn(namespaceable as MixedOperation<Lease, io.fabric8.kubernetes.api.model.coordination.v1.LeaseList, Resource<Lease>>)
            whenever(namespaceable.withName(any())).thenReturn(resource)
            whenever(resource.get()).thenReturn(lease)
            whenever(lease.spec).thenReturn(null)

            val epoch = invokeReadLeaseTransitions(leaderCfg)

            assertEquals(0L, epoch)
        }

        @Test
        fun `readLeaseTransitions returns 0 when lease is null`() {
            val leaderCfg = mockLeaderConfig()

            @Suppress("UNCHECKED_CAST")
            val leasesOp = mock<MixedOperation<Lease, *, *>>()
            val namespaceable = mock<MixedOperation<Lease, *, *>>()
            val resource = mock<Resource<Lease>>()

            whenever(kubernetesClient.leases()).thenReturn(leasesOp as MixedOperation<Lease, io.fabric8.kubernetes.api.model.coordination.v1.LeaseList, Resource<Lease>>)
            whenever(leasesOp.inNamespace(any())).thenReturn(namespaceable as MixedOperation<Lease, io.fabric8.kubernetes.api.model.coordination.v1.LeaseList, Resource<Lease>>)
            whenever(namespaceable.withName(any())).thenReturn(resource)
            whenever(resource.get()).thenReturn(null)

            val epoch = invokeReadLeaseTransitions(leaderCfg)

            assertEquals(0L, epoch)
        }
    }

    // ── releaseLeaseExplicitly via reflection ─────────────────────

    @Nested
    inner class ReleaseLeaseExplicitly {

        private fun invokeReleaseLeaseExplicitly() {
            val method = LeaderManager::class.java.getDeclaredMethod("releaseLeaseExplicitly")
            method.isAccessible = true
            method.invoke(leaderManager)
        }

        @Test
        fun `in dev mode sets isActive to false`() {
            leaderManager.onStart(StartupEvent())
            assertTrue(leaderManager.isActive)

            invokeReleaseLeaseExplicitly()

            assertFalse(leaderManager.isActive)
        }
    }
}
