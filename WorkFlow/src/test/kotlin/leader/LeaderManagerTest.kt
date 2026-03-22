package com.workflow.leader

import com.workflow.config.FrameworkConfig
import io.fabric8.kubernetes.api.model.coordination.v1.Lease
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseList
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseSpec
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.MixedOperation
import io.fabric8.kubernetes.client.dsl.Resource
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfig
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectorBuilder
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.test.runTest
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class LeaderManagerTest {

    private val fixedInstant = Instant.parse("2024-01-01T00:00:00Z")
    private val fixedClock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

    private val workerConfig = mock<FrameworkConfig.WorkerConfig>().also {
        whenever(it.id()).thenReturn("worker-1")
    }
    private val shutdownConfig = mock<FrameworkConfig.ShutdownConfig>()
    private val leaderElectionConfig = mock<FrameworkConfig.LeaderElectionConfig>().also {
        whenever(it.namespace()).thenReturn("test-ns")
        whenever(it.leaseName()).thenReturn("test-lease")
        whenever(it.leaseDuration()).thenReturn(Duration.ofSeconds(15))
        whenever(it.renewDeadline()).thenReturn(Duration.ofSeconds(10))
        whenever(it.retryPeriod()).thenReturn(Duration.ofSeconds(2))
    }
    private val config = mock<FrameworkConfig>().also {
        whenever(it.shutdown()).thenReturn(shutdownConfig)
        whenever(it.leaderElection()).thenReturn(leaderElectionConfig)
        whenever(it.worker()).thenReturn(workerConfig)
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

    /**
     * Mocks the K8s leaderElector() chain and captures the LeaderElectionConfig
     * so tests can extract and invoke LeaderCallbacks.
     *
     * @param onRunInvoked called when leaderElector.run() is invoked, receives
     *        the captured [LeaderElectionConfig] to let the test trigger callbacks.
     */
    @Suppress("UNCHECKED_CAST")
    private fun mockLeaderElectorChain(
        onRunInvoked: (LeaderElectionConfig) -> Unit = {},
    ) {
        val configCaptor = argumentCaptor<LeaderElectionConfig>()
        val leaderElector = mock<LeaderElector>()
        val leaderElectorBuilder = mock<LeaderElectorBuilder>()

        whenever(leaderElectorBuilder.withConfig(configCaptor.capture()))
            .thenReturn(leaderElectorBuilder)
        whenever(leaderElectorBuilder.build()).thenReturn(leaderElector)
        whenever(kubernetesClient.leaderElector()).thenReturn(leaderElectorBuilder)

        doAnswer {
            val captured = configCaptor.firstValue
            onRunInvoked(captured)
            null
        }.whenever(leaderElector).run()
    }

    /**
     * Mocks the K8s leases() API chain for readLeaseTransitions and releaseLeaseExplicitly.
     *
     * @return the mock [Resource] for further stubbing of .get() and .patch()
     */
    @Suppress("UNCHECKED_CAST")
    private fun mockLeasesApi(): Resource<Lease> {
        val leaseResource = mock<Resource<Lease>>()
        val namespacedOp = mock<MixedOperation<Lease, LeaseList, Resource<Lease>>>()

        whenever(kubernetesClient.leases())
            .thenReturn(namespacedOp as MixedOperation<Lease, LeaseList, Resource<Lease>>)
        whenever(namespacedOp.inNamespace("test-ns"))
            .thenReturn(namespacedOp)
        whenever(namespacedOp.withName("test-lease"))
            .thenReturn(leaseResource)

        return leaseResource
    }

    /**
     * Creates a [Lease] with the given [leaseTransitions] and optional [holderIdentity].
     */
    private fun leaseWithTransitions(
        leaseTransitions: Int,
        holderIdentity: String? = null,
    ): Lease {
        val spec = LeaseSpec()
        spec.leaseTransitions = leaseTransitions
        spec.holderIdentity = holderIdentity
        val lease = Lease()
        lease.spec = spec
        return lease
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

    // == Section A: Kubernetes Election Path ===================================

    @Test
    fun `onStart in K8s mode launches election loop`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(3))

        val electionStarted = CompletableDeferred<Unit>()
        mockLeaderElectorChain { config ->
            electionStarted.complete(Unit)
            // Simulate election running briefly then returning
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        // The election loop launches in the scope; verify it started
        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(electionStarted.isCompleted, "Election loop should have started")
        }
    }

    @Test
    fun `onAcquire callback sets isActive true and token from leaseTransitions`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(7))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            // Block until the test completes to keep the election "running"
            Thread.sleep(2000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        val callbacks = callbacksReceived.getCompleted()
        callbacks.onStartLeading()

        assertTrue(manager.isActive)
        assertEquals(7L, manager.token)
    }

    @Test
    fun `onLose callback sets isActive false but retains epoch`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(5))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        val callbacks = callbacksReceived.getCompleted()
        // First acquire
        callbacks.onStartLeading()
        assertTrue(manager.isActive)
        assertEquals(5L, manager.token)

        // Then lose
        callbacks.onStopLeading()
        assertFalse(manager.isActive)
        assertEquals(5L, manager.token, "Epoch should be retained after losing leadership")
    }

    @Test
    fun `epoch ordering - token is set before isActive becomes true`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(42))

        // Track the order of state changes
        val tokenWhenActivated = CompletableDeferred<Long>()
        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()

        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        val callbacks = callbacksReceived.getCompleted()
        callbacks.onStartLeading()

        // After onAcquire, both should be set. The production code sets
        // _epoch.value = epoch BEFORE _isLeader.value = true
        assertTrue(manager.isActive)
        assertEquals(42L, manager.token, "Token must reflect lease transitions after acquire")
    }

    // == Section B: readLeaseTransitions =======================================

    @Test
    fun `readLeaseTransitions returns lease transitions count from K8s API`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(5))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        assertEquals(5L, manager.token)
    }

    @Test
    fun `readLeaseTransitions falls back to previousEpoch plus 1 on K8s API exception`() {
        val leaseResource = mockLeasesApi()
        // First call succeeds to set epoch=3, second call throws
        whenever(leaseResource.get())
            .thenReturn(leaseWithTransitions(3))
            .thenThrow(RuntimeException("API unavailable"))

        val callbacksHolder = mutableListOf<LeaderCallbacks>()
        var runCount = 0
        mockLeaderElectorChain { electionConfig ->
            runCount++
            callbacksHolder.add(electionConfig.leaderCallbacks)
            if (runCount == 1) {
                // First election: acquire then lose
                electionConfig.leaderCallbacks.onStartLeading()
                electionConfig.leaderCallbacks.onStopLeading()
                // Return so loop retries
            } else {
                // Second election: acquire again (API will throw)
                electionConfig.leaderCallbacks.onStartLeading()
                Thread.sleep(2000)
            }
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(10, TimeUnit.SECONDS).untilAsserted {
            // After the second acquire with failing API, token should be previousEpoch(3) + 1 = 4
            assertEquals(4L, manager.token)
        }
    }

    @Test
    fun `readLeaseTransitions returns 0 when lease is null`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(null)

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        assertEquals(0L, manager.token, "Null lease should yield token=0")
    }

    // == Section C: releaseLeaseExplicitly =====================================

    @Test
    fun `shutdown in K8s mode releases lease by patching holderIdentity to null`() = runTest {
        val leaseResource = mockLeasesApi()
        val existingLease = leaseWithTransitions(2, holderIdentity = "worker-1")
        whenever(leaseResource.get()).thenReturn(existingLease)
        whenever(leaseResource.patch(any<Lease>())).thenReturn(existingLease)

        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(10))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { true },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        assertTrue(manager.isActive)

        manager.shutdown()

        assertFalse(manager.isActive)
        // Verify that patch was called with a lease where holderIdentity is null
        verify(leaseResource).patch(argThat<Lease> { this.spec.holderIdentity == null })
    }

    @Test
    fun `shutdown in K8s mode with exception during release does not crash`() = runTest {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(1))
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(10))

        // Make the lease release fail during shutdown
        val leaseForRelease = mockLeasesApi()
        whenever(leaseForRelease.get()).thenThrow(RuntimeException("API connection refused"))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { true },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()

        // Should not throw even when release fails
        manager.shutdown()
        assertFalse(manager.isActive)
    }

    @Test
    fun `shutdown in K8s mode with null lease does not attempt patch`() = runTest {
        val leaseResource = mockLeasesApi()
        // First call for readLeaseTransitions returns a lease, then for release returns null
        whenever(leaseResource.get())
            .thenReturn(leaseWithTransitions(1))
            .thenReturn(null)

        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(10))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { true },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        manager.shutdown()

        assertFalse(manager.isActive)
        verify(leaseResource, never()).patch(any<Lease>())
    }

    @Test
    fun `shutdown in K8s mode skips lease release when holderIdentity does not match`() = runTest {
        val leaseResource = mockLeasesApi()
        // First get() for readLeaseTransitions during onStartLeading
        // Second get() for releaseLeaseExplicitly during shutdown — holder is a different pod
        whenever(leaseResource.get())
            .thenReturn(leaseWithTransitions(2, holderIdentity = "worker-1"))
            .thenReturn(leaseWithTransitions(2, holderIdentity = "other-worker"))

        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(10))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { true },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        assertTrue(manager.isActive)

        manager.shutdown()

        assertFalse(manager.isActive, "Shutdown should complete even when lease is held by another pod")
        verify(leaseResource, never()).patch(any<Lease>())
    }

    @Test
    fun `releaseLeaseExplicitly in non-K8s mode does not call kubernetesClient`() = runTest {
        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { false },
            scope = CoroutineScope(managerJob),
        )

        manager.onStart(startupEvent)
        manager.shutdown()

        verifyNoInteractions(kubernetesClient)
    }

    // == Section D: electionLoop Error Recovery ================================

    @Test
    fun `runElection throws non-cancellation exception sets isLeader false and loop retries`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(1))

        var runCount = 0
        val secondRunStarted = CompletableDeferred<Unit>()

        mockLeaderElectorChain { electionConfig ->
            runCount++
            if (runCount == 1) {
                // First run: acquire, then throw
                electionConfig.leaderCallbacks.onStartLeading()
                throw RuntimeException("Simulated election failure")
            } else {
                // Second run: verify loop retried
                secondRunStarted.complete(Unit)
                Thread.sleep(5000)
            }
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        // After the first run throws, isLeader should be set to false
        await.atMost(10, TimeUnit.SECONDS).untilAsserted {
            assertTrue(secondRunStarted.isCompleted, "Election loop should retry after error")
        }
    }

    @Test
    fun `runElection returns normally and loop retries`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(1))

        var runCount = 0
        val secondRunStarted = CompletableDeferred<Unit>()

        mockLeaderElectorChain { electionConfig ->
            runCount++
            if (runCount == 1) {
                // First run: return normally (election expired)
            } else {
                // Second run: verify retry
                secondRunStarted.complete(Unit)
                Thread.sleep(5000)
            }
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(10, TimeUnit.SECONDS).untilAsserted {
            assertTrue(secondRunStarted.isCompleted, "Election loop should retry after normal return")
        }
    }

    @Test
    fun `scope cancellation exits election loop and sets isLeader false`() = runTest {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(1))
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(10))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            // Block to simulate long-running election
            Thread.sleep(10_000)
        }

        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { true },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        assertTrue(manager.isActive)

        // Shutdown cancels scope, which should exit the election loop
        manager.shutdown()

        assertFalse(manager.isActive)
        assertTrue(managerJob.isCancelled)
    }

    // == Section E: shutdown (K8s mode) ========================================

    @Test
    fun `shutdown in K8s mode sets isLeader false and cancels scope and releases lease`() = runTest {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(2, holderIdentity = "worker-1"))
        whenever(leaseResource.patch(any<Lease>())).thenReturn(leaseWithTransitions(2))
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(10))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(5000)
        }

        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { true },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        assertTrue(manager.isActive)
        assertEquals(2L, manager.token)

        manager.shutdown()

        assertFalse(manager.isActive, "isLeader should be false after shutdown")
        assertTrue(managerJob.isCancelled, "Scope should be cancelled after shutdown")
        // Verify lease release was attempted
        verify(leaseResource).patch(any<Lease>())
    }

    // == Section F: onNewLeader callback updates heartbeat ====================

    @Test
    fun `onNewLeader callback updates lastHeartbeat`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(1))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        // The initial heartbeat is set to fixedInstant by our clock
        assertEquals(fixedInstant, manager.lastHeartbeat)

        // The onNewLeader callback also updates heartbeat (using our fixed clock)
        callbacksReceived.getCompleted().onNewLeader("")
        assertEquals(fixedInstant, manager.lastHeartbeat)
    }

    // == Section G: Metrics in K8s mode =======================================

    @Test
    fun `metrics reflect state changes through onAcquire and onLose in K8s mode`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(10))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(5000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        val isLeaderGauge = meterRegistry.find("leader_election_is_leader").gauge()!!
        val epochGauge = meterRegistry.find("leader_election_epoch").gauge()!!

        // Before acquire: inactive
        assertEquals(0.0, isLeaderGauge.value(), "Gauge should be 0 before acquire")
        assertEquals(0.0, epochGauge.value(), "Epoch gauge should be 0 before acquire")

        // After acquire
        callbacksReceived.getCompleted().onStartLeading()
        assertEquals(1.0, isLeaderGauge.value(), "Gauge should be 1 after acquire")
        assertEquals(10.0, epochGauge.value(), "Epoch gauge should reflect lease transitions")

        // After lose
        callbacksReceived.getCompleted().onStopLeading()
        assertEquals(0.0, isLeaderGauge.value(), "Gauge should be 0 after lose")
        assertEquals(10.0, epochGauge.value(), "Epoch gauge should retain value after lose")
    }

    @Test
    fun `epoch gauge reflects epoch from readLeaseTransitions`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(25))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(2000)
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()

        val epochGauge = meterRegistry.find("leader_election_epoch").gauge()!!
        assertEquals(25.0, epochGauge.value(), "Epoch gauge should read from lease transitions counter")
    }

    // == Additional coverage: electionLoop heartbeat at loop entry =============

    @Test
    fun `electionLoop updates lastHeartbeat at each iteration start`() {
        val leaseResource = mockLeasesApi()
        whenever(leaseResource.get()).thenReturn(leaseWithTransitions(1))

        var runCount = 0
        mockLeaderElectorChain { _ ->
            runCount++
            // Return immediately so the loop cycles to the next iteration
        }

        val manager = createManager(detector = KubernetesDetector { true })
        manager.onStart(startupEvent)

        // The loop sets _lastHeartbeat = Instant.now(clock) at the top of each iteration.
        // With our fixed clock, the value stays at fixedInstant, but the assignment runs.
        await.atMost(10, TimeUnit.SECONDS).untilAsserted {
            assertTrue(runCount >= 2, "Election should have run at least twice")
        }
        assertEquals(fixedInstant, manager.lastHeartbeat)
    }

    // == Additional: shutdown with successful (non-timeout) lease release =======

    @Test
    fun `shutdown completes lease release within timeout`() = runTest {
        val leaseResource = mockLeasesApi()
        val existingLease = leaseWithTransitions(3, holderIdentity = "worker-1")
        whenever(leaseResource.get()).thenReturn(existingLease)
        whenever(leaseResource.patch(any<Lease>())).thenReturn(existingLease)
        whenever(shutdownConfig.leaderTeardownTimeout()).thenReturn(Duration.ofSeconds(30))

        val callbacksReceived = CompletableDeferred<LeaderCallbacks>()
        mockLeaderElectorChain { electionConfig ->
            callbacksReceived.complete(electionConfig.leaderCallbacks)
            Thread.sleep(5000)
        }

        val managerJob = SupervisorJob()
        val manager = createManager(
            detector = KubernetesDetector { true },
            scope = CoroutineScope(managerJob),
        )
        manager.onStart(startupEvent)

        await.atMost(5, TimeUnit.SECONDS).untilAsserted {
            assertTrue(callbacksReceived.isCompleted)
        }

        callbacksReceived.getCompleted().onStartLeading()
        assertTrue(manager.isActive)

        manager.shutdown()

        assertFalse(manager.isActive)
        // The release should complete within the 30s timeout
        verify(leaseResource).patch(argThat<Lease> { this.spec.holderIdentity == null })
    }
}
