package com.workflow.worker

import com.workflow.config.FrameworkConfig
import com.workflow.leader.KubernetesDetector
import io.fabric8.kubernetes.api.model.EndpointAddress
import io.fabric8.kubernetes.api.model.EndpointSubset
import io.fabric8.kubernetes.api.model.Endpoints
import io.fabric8.kubernetes.api.model.EndpointsList
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watch
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import io.fabric8.kubernetes.client.dsl.MixedOperation
import io.fabric8.kubernetes.client.dsl.Resource
import io.quarkus.runtime.StartupEvent
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PeerRegistryTest {

    private lateinit var client: KubernetesClient
    private lateinit var config: FrameworkConfig
    private lateinit var detector: KubernetesDetector
    private lateinit var workerConfig: FrameworkConfig.WorkerConfig
    private lateinit var leaderElectionConfig: FrameworkConfig.LeaderElectionConfig
    private lateinit var registry: PeerRegistry

    private val watchCaptor = argumentCaptor<Watcher<Endpoints>>()

    @Suppress("UNCHECKED_CAST")
    @BeforeEach
    fun setup() {
        client = mock()
        config = mock()
        detector = mock()
        workerConfig = mock()
        leaderElectionConfig = mock()

        whenever(config.worker()).thenReturn(workerConfig)
        whenever(config.leaderElection()).thenReturn(leaderElectionConfig)
        whenever(workerConfig.podIp()).thenReturn("10.0.0.1")
        whenever(leaderElectionConfig.namespace()).thenReturn("default")
        whenever(config.serviceName()).thenReturn("workflow-engine")

        val endpointsOp = mock<MixedOperation<Endpoints, EndpointsList, Resource<Endpoints>>>()
        val namedResource = mock<Resource<Endpoints>>()
        val watch = mock<Watch>()

        whenever(client.endpoints()).thenReturn(endpointsOp)
        whenever(endpointsOp.inNamespace("default")).thenReturn(endpointsOp)
        whenever(endpointsOp.withName("workflow-engine")).thenReturn(namedResource)
        whenever(namedResource.watch(watchCaptor.capture())).thenReturn(watch)
        whenever(detector.isRunningInKubernetes()).thenReturn(true)

        registry = PeerRegistry(client, config, detector)
    }

    private fun startupEvent(): StartupEvent = mock()

    private fun buildEndpoints(vararg ips: String): Endpoints {
        val endpoints = Endpoints()
        val subset = EndpointSubset()
        subset.addresses = ips.map { ip ->
            val addr = EndpointAddress()
            addr.ip = ip
            addr
        }
        endpoints.subsets = listOf(subset)
        return endpoints
    }

    // ── A. start() registers a watcher ───────────────────────────────────

    @Test
    fun `start registers an Endpoints watcher`() {
        registry.start(startupEvent())

        verify(client.endpoints()
            .inNamespace("default")
            .withName("workflow-engine"))
            .watch(any<Watcher<Endpoints>>())
    }

    // ── B. Watch ADDED event populates peers ─────────────────────────────

    @Nested
    inner class WatchEvents {

        @Test
        fun `ADDED event populates peers`() {
            registry.start(startupEvent())
            val watcher = watchCaptor.firstValue

            watcher.eventReceived(Watcher.Action.ADDED, buildEndpoints("10.0.0.2", "10.0.0.3"))

            assertEquals(listOf("10.0.0.2", "10.0.0.3"), registry.peers())
        }

        @Test
        fun `MODIFIED event updates peers`() {
            registry.start(startupEvent())
            val watcher = watchCaptor.firstValue

            watcher.eventReceived(Watcher.Action.ADDED, buildEndpoints("10.0.0.2"))
            assertEquals(listOf("10.0.0.2"), registry.peers())

            watcher.eventReceived(Watcher.Action.MODIFIED, buildEndpoints("10.0.0.2", "10.0.0.4"))
            assertEquals(listOf("10.0.0.2", "10.0.0.4"), registry.peers())
        }

        @Test
        fun `DELETED event clears stale IPs`() {
            registry.start(startupEvent())
            val watcher = watchCaptor.firstValue

            watcher.eventReceived(Watcher.Action.ADDED, buildEndpoints("10.0.0.2", "10.0.0.3"))
            assertEquals(2, registry.peers().size)

            val emptyEndpoints = Endpoints()
            emptyEndpoints.subsets = emptyList()
            watcher.eventReceived(Watcher.Action.DELETED, emptyEndpoints)
            assertTrue(registry.peers().isEmpty(), "Peers should be empty after DELETED event")
        }
    }

    // ── C. Self-exclusion ────────────────────────────────────────────────

    @Nested
    inner class SelfExclusion {

        @Test
        fun `pod own IP is excluded from peers`() {
            registry.start(startupEvent())
            val watcher = watchCaptor.firstValue

            watcher.eventReceived(
                Watcher.Action.ADDED,
                buildEndpoints("10.0.0.1", "10.0.0.2", "10.0.0.3"),
            )

            assertFalse(registry.peers().contains("10.0.0.1"), "Own IP should be excluded")
            assertEquals(listOf("10.0.0.2", "10.0.0.3"), registry.peers())
        }

        @Test
        fun `all IPs equal to self results in empty peers`() {
            registry.start(startupEvent())
            val watcher = watchCaptor.firstValue

            watcher.eventReceived(Watcher.Action.ADDED, buildEndpoints("10.0.0.1"))

            assertTrue(registry.peers().isEmpty())
        }
    }

    // ── D. Initial state ─────────────────────────────────────────────────

    @Test
    fun `peers returns empty before start is called`() {
        assertTrue(registry.peers().isEmpty())
    }

    // ── E. onClose resilience ────────────────────────────────────────────

    @Nested
    inner class OnCloseResilience {

        @Test
        fun `onClose with cause does not throw and peers unchanged`() {
            registry.start(startupEvent())
            val watcher = watchCaptor.firstValue

            watcher.eventReceived(Watcher.Action.ADDED, buildEndpoints("10.0.0.2"))
            assertEquals(listOf("10.0.0.2"), registry.peers())

            watcher.onClose(WatcherException("connection lost"))

            assertEquals(listOf("10.0.0.2"), registry.peers(), "Peers should be unchanged after onClose with cause")
        }

        @Test
        fun `onClose with null cause does not throw and peers unchanged`() {
            registry.start(startupEvent())
            val watcher = watchCaptor.firstValue

            watcher.eventReceived(Watcher.Action.ADDED, buildEndpoints("10.0.0.2"))
            assertEquals(listOf("10.0.0.2"), registry.peers())

            watcher.onClose(null)

            assertEquals(listOf("10.0.0.2"), registry.peers(), "Peers should be unchanged after onClose with null")
        }
    }

    // ── F. start() exception resilience ──────────────────────────────────

    @Test
    fun `peers returns empty when client endpoints throws during start`() {
        whenever(client.endpoints()).thenThrow(RuntimeException("K8s API unavailable"))

        val failRegistry = PeerRegistry(client, config, detector)
        try {
            failRegistry.start(startupEvent())
        } catch (_: RuntimeException) {
            // start() may propagate the exception
        }

        assertTrue(failRegistry.peers().isEmpty(), "Peers should be empty when start fails")
    }
}
