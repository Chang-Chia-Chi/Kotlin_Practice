package com.mapreduce.leader

import com.mapreduce.config.FrameworkConfig
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.jboss.logging.Logger
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Kubernetes Lease-based leader election.
 *
 * In a K8s environment, exactly one pod acquires the lease and becomes the leader.
 * Outside K8s (dev mode), the pod defaults to the leader role.
 */
@ApplicationScoped
class LeaderElection(
    private val config: FrameworkConfig,
    private val kubernetesClient: KubernetesClient,
) {

    private val log = Logger.getLogger(LeaderElection::class.java)
    private val _isLeader = AtomicBoolean(false)
    private val _fenceToken = AtomicReference(UUID.randomUUID().toString())
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    val isLeader: Boolean get() = _isLeader.get()
    val fenceToken: String get() = _fenceToken.get()

    fun onStart(@Observes ev: StartupEvent) {
        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            log.info("Not running in Kubernetes — assuming leader role")
            _isLeader.set(true)
            return
        }
        startElection()
    }

    fun onStop(@Observes ev: ShutdownEvent) {
        _isLeader.set(false)
        scope.cancel()
    }

    private fun startElection() {
        val identity = config.worker().id()
        val namespace = kubernetesClient.configuration.namespace ?: "default"

        val lock = LeaseLock(namespace, "mapreduce-leader", identity)
        val electionConfig = LeaderElectionConfigBuilder()
            .withName("mapreduce-leader")
            .withLock(lock)
            .withLeaseDuration(Duration.ofSeconds(15))
            .withRenewDeadline(Duration.ofSeconds(10))
            .withRetryPeriod(Duration.ofSeconds(2))
            .withLeaderCallbacks(LeaderCallbacks(
                {
                    log.infof("Acquired leadership (identity=%s)", identity)
                    _fenceToken.set(UUID.randomUUID().toString())
                    _isLeader.set(true)
                },
                {
                    log.info("Lost leadership")
                    _isLeader.set(false)
                },
                { newLeader -> log.debugf("Leader changed: %s", newLeader) },
            ))
            .build()

        scope.launch {
            try {
                kubernetesClient.leaderElector()
                    .withConfig(electionConfig)
                    .build()
                    .run()
            } catch (e: Exception) {
                log.warnf("Leader election failed (%s) — assuming leader role", e.message)
                _isLeader.set(true)
            }
        }
    }
}
