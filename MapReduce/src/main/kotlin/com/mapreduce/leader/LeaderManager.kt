package com.mapreduce.leader

import com.mapreduce.config.FrameworkConfig
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import org.jboss.logging.Logger
import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Kubernetes Lease-based leader election with fencing epoch extraction.
 */
@ApplicationScoped
class LeaderManager(
    private val config: FrameworkConfig,
    private val kubernetesClient: KubernetesClient,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(LeaderManager::class.java)

    private val _isLeader = AtomicBoolean(false)
    private val _epoch = AtomicLong(0)
    private val _lastHeartbeat = AtomicReference(Instant.now())
    private val _acquiredAt = AtomicReference<Instant?>(null)
    private val _renewedAt = AtomicReference<Instant?>(null)
    private var executor: ExecutorService? = null

    val isActive: Boolean get() = _isLeader.get()
    val token: Long get() = _epoch.get()
    val lastHeartbeat: Instant get() = _lastHeartbeat.get()
    val acquiredAt: Instant? get() = _acquiredAt.get()
    val renewedAt: Instant? get() = _renewedAt.get()

    fun onStart(@Observes ev: StartupEvent) {
        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            log.info("Not running in Kubernetes — assuming leader role with synthetic epoch")
            _epoch.set(1)
            _isLeader.set(true)
            _acquiredAt.set(Instant.now())
            registerMetrics()
            return
        }

        executor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "leader-election").apply { isDaemon = true }
        }
        executor!!.submit { electionLoop() }
        registerMetrics()
    }

    fun onStop(@Observes ev: ShutdownEvent) {
        log.info("Shutting down leader election")
        _isLeader.set(false)
        executor?.shutdownNow()
    }

    fun releaseLeaseExplicitly() {
        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            log.info("Not in Kubernetes — skipping explicit lease release")
            _isLeader.set(false)
            return
        }
        try {
            val leaderCfg = config.leaderElection()
            val leaseApi = kubernetesClient.leases().inNamespace(leaderCfg.namespace())
            val lease = leaseApi.withName(leaderCfg.leaseName()).get()
            if (lease != null) {
                lease.spec.holderIdentity = null
                lease.spec.acquireTime = null
                leaseApi.withName(leaderCfg.leaseName()).patch(lease)
                log.info("Lease released explicitly — new leader can acquire immediately")
            }
        } catch (e: Exception) {
            log.warnf(e, "Failed to release lease explicitly — new leader will acquire after lease expiry")
        }
        _isLeader.set(false)
    }

    private fun electionLoop() {
        val identity = config.worker().id()
        val leaderCfg = config.leaderElection()
        val namespace = leaderCfg.namespace()
        val leaseName = leaderCfg.leaseName()
        val retryPeriodMs = leaderCfg.retryPeriod().toMillis()

        while (!Thread.currentThread().isInterrupted) {
            _lastHeartbeat.set(Instant.now())
            try {
                val lock = LeaseLock(namespace, leaseName, identity)
                val electionConfig = LeaderElectionConfigBuilder()
                    .withName(leaseName)
                    .withLock(lock)
                    .withLeaseDuration(leaderCfg.leaseDuration())
                    .withRenewDeadline(leaderCfg.renewDeadline())
                    .withRetryPeriod(leaderCfg.retryPeriod())
                    .withLeaderCallbacks(
                        LeaderCallbacks(
                            { onAcquire(identity) },
                            { onLose() },
                            { newLeader -> onNewLeader(newLeader, identity) },
                        ),
                    )
                    .build()

                log.debugf("Entering leader election (identity=%s, lease=%s/%s)", identity, namespace, leaseName)
                kubernetesClient.leaderElector()
                    .withConfig(electionConfig)
                    .build()
                    .run()

                log.infof("Leader election run() returned — will retry in %dms", retryPeriodMs)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            } catch (e: Exception) {
                log.errorf(e, "Leader election error — retrying in %dms", retryPeriodMs)
                _isLeader.set(false)
            }

            try {
                Thread.sleep(retryPeriodMs)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }

        log.info("Leader election thread exiting")
        _isLeader.set(false)
    }

    private fun onAcquire(identity: String) {
        log.infof("Acquired leadership (identity=%s)", identity)
        refreshEpoch()
        _isLeader.set(true)
        _acquiredAt.set(Instant.now())
    }

    private fun onLose() {
        log.info("Lost leadership")
        _isLeader.set(false)
    }

    private fun onNewLeader(newLeader: String, identity: String) {
        _lastHeartbeat.set(Instant.now())
        log.debugf("Leader changed: %s", newLeader)
        if (newLeader == identity && _isLeader.get()) {
            refreshEpoch()
        }
    }

    private fun refreshEpoch() {
        val newEpoch = _epoch.incrementAndGet()
        _renewedAt.set(Instant.now())
        log.infof("Fencing epoch incremented: %d", newEpoch)
    }

    private fun registerMetrics() {
        meterRegistry.gauge("leader_election_is_leader", this) { if (isActive) 1.0 else 0.0 }
        meterRegistry.gauge("leader_election_epoch", this) { token.toDouble() }
    }
}
