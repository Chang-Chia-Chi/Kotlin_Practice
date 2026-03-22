package com.workflow.leader

import com.workflow.config.FrameworkConfig
import io.fabric8.kubernetes.client.KubernetesClient
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.test.runTest
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LeaderManagerTest {

    private val fixedInstant = Instant.parse("2024-01-01T00:00:00Z")
    private val fixedClock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

    private val shutdownConfig = mock<FrameworkConfig.ShutdownConfig>()
    private val leaderElectionConfig = mock<FrameworkConfig.LeaderElectionConfig>()
    private val config = mock<FrameworkConfig>().also {
        whenever(it.shutdown()).thenReturn(shutdownConfig)
        whenever(it.leaderElection()).thenReturn(leaderElectionConfig)
    }

    private val kubernetesClient = mock<KubernetesClient>()
    private val meterRegistry = SimpleMeterRegistry()
    private val startupEvent = mock<StartupEvent>()

    private fun createManager(
        detector: KubernetesDetector = KubernetesDetector { false },
        scope: CoroutineScope = CoroutineScope(SupervisorJob()),
    ): LeaderManager {
        val manager = LeaderManager(config, kubernetesClient, meterRegistry, detector)
        manager.clock = fixedClock
        manager.scope = scope
        return manager
    }

    // -- A. Non-Kubernetes Fallback -------------------------------------------

    @Test
    fun `onStart sets isActive true and token 1 when not in kubernetes`() {
        val manager = createManager(detector = KubernetesDetector { false })

        manager.onStart(startupEvent)

        assertTrue(manager.isActive)
        assertEquals(1L, manager.token)
    }

    @Test
    fun `onStart registers metrics when not in kubernetes`() {
        val manager = createManager(detector = KubernetesDetector { false })

        manager.onStart(startupEvent)

        assertNotNull(meterRegistry.find("leader_election_is_leader").gauge())
        assertNotNull(meterRegistry.find("leader_election_epoch").gauge())
    }

    @Test
    fun `onStart does not interact with kubernetesClient when not in kubernetes`() {
        val manager = createManager(detector = KubernetesDetector { false })

        manager.onStart(startupEvent)

        verifyNoInteractions(kubernetesClient)
    }

    // -- B. State Transitions -------------------------------------------------

    @Test
    fun `initial state before onStart is inactive with zero token`() {
        val manager = createManager()

        assertFalse(manager.isActive)
        assertEquals(0L, manager.token)
    }

    @Test
    fun `shutdown sets isActive false regardless of prior state`() = runTest {
        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { false },
            scope = CoroutineScope(managerJob),
        )

        manager.onStart(startupEvent)
        assertTrue(manager.isActive)

        manager.shutdown()
        assertFalse(manager.isActive)
    }

    // -- C. Shutdown ----------------------------------------------------------

    @Test
    fun `shutdown cancels coroutine scope`() = runTest {
        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { false },
            scope = CoroutineScope(managerJob),
        )

        manager.onStart(startupEvent)
        manager.shutdown()

        assertTrue(managerJob.isCancelled)
    }

    @Test
    fun `shutdown in non-k8s mode does not call kubernetesClient for lease release`() = runTest {
        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { false },
            scope = CoroutineScope(managerJob),
        )

        manager.onStart(startupEvent)
        manager.shutdown()

        verifyNoInteractions(kubernetesClient)
    }

    @Test
    fun `shutdownOrder is 1`() {
        val manager = createManager()
        assertEquals(1, manager.shutdownOrder)
    }

    @Test
    fun `shutdownTimeout delegates to config`() {
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(42))
        val manager = createManager()

        assertEquals(Duration.ofSeconds(42), manager.shutdownTimeout)
    }

    // -- D. Clock Determinism -------------------------------------------------

    @Test
    fun `lastHeartbeat uses injected clock`() {
        val manager = createManager()
        assertEquals(fixedInstant, manager.lastHeartbeat)
    }

    // -- E. Metrics -----------------------------------------------------------

    @Test
    fun `onStart registers leader_election_is_leader gauge`() {
        val manager = createManager(detector = KubernetesDetector { false })
        manager.onStart(startupEvent)

        assertNotNull(meterRegistry.find("leader_election_is_leader").gauge())
    }

    @Test
    fun `onStart registers leader_election_epoch gauge`() {
        val manager = createManager(detector = KubernetesDetector { false })
        manager.onStart(startupEvent)

        assertNotNull(meterRegistry.find("leader_election_epoch").gauge())
    }

    @Test
    fun `is_leader gauge returns 1 when active and 0 when not`() = runTest {
        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { false },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        val gauge = meterRegistry.find("leader_election_is_leader").gauge()!!
        assertEquals(1.0, gauge.value())

        manager.shutdown()
        assertEquals(0.0, gauge.value())
    }
}
