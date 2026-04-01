package com.workflow.worker.adapter.http

import com.workflow.infrastructure.leader.LeaderElectionConfig
import com.workflow.infrastructure.leader.KubernetesDetector
import com.workflow.worker.config.WorkerLoopConfig
import com.workflow.worker.usecase.port.outbound.peer.PeerDiscovery
import io.fabric8.kubernetes.api.model.Endpoints
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watch
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import org.slf4j.LoggerFactory

/**
 * Maintains a live list of peer pod IPs via Kubernetes Endpoints Watch.
 *
 * The watch is registered on [StartupEvent] against the K8s Service
 * named by [WorkerLoopConfig.serviceName]. The pod's own IP
 * ([WorkerLoopConfig.podIp]) is excluded so broadcasts
 * are never sent to self.
 *
 * Outside Kubernetes (local dev, tests), [peers] returns an empty list
 * and all signaling stays in-process.
 */
@ApplicationScoped
class PeerRegistry(
    private val client: KubernetesClient,
    private val workerLoopConfig: WorkerLoopConfig,
    private val leaderElectionConfig: LeaderElectionConfig,
    private val detector: KubernetesDetector,
) : PeerDiscovery {
    private val log = LoggerFactory.getLogger(PeerRegistry::class.java)

    @Volatile
    private var _peers: List<String> = emptyList()

    private var watch: Watch? = null

    /** Current peer pod IPs, excluding this pod. */
    override fun peers(): List<String> = _peers

    fun start(@Observes ev: StartupEvent) {
        if (!detector.isRunningInKubernetes()) return

        val myIp = workerLoopConfig.podIp()

        try {
            watch = client.endpoints()
                .inNamespace(leaderElectionConfig.namespace())
                .withName(workerLoopConfig.serviceName())
                .watch(object : Watcher<Endpoints> {
                    override fun eventReceived(action: Watcher.Action, endpoints: Endpoints) {
                        _peers = (endpoints.subsets ?: emptyList())
                            .flatMap { subset -> (subset.addresses ?: emptyList()).map { it.ip } }
                            .filter { it != myIp }
                        log.debug("Peer list updated ({}): {}", action, _peers)
                    }

                    override fun onClose(cause: WatcherException?) {
                        if (cause != null) {
                            log.warn("Endpoints watch closed, Fabric8 will reconnect", cause)
                        }
                    }
                })
        } catch (e: Exception) {
            log.warn("Failed to start Endpoints watch: {}", e.message)
        }
    }

    @PreDestroy
    fun stop() {
        watch?.close()
    }
}
