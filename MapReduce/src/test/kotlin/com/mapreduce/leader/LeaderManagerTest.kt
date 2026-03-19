package com.mapreduce.leader

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.shutdown.ShutdownParticipant
import io.fabric8.kubernetes.client.KubernetesClient
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
}
