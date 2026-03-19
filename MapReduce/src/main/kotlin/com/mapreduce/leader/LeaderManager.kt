package com.mapreduce.leader

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.shutdown.ShutdownParticipant
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant

/**
 * Kubernetes Lease-based leader election with fencing epoch.
 *
 * The fencing epoch is sourced from the K8s lease's `leaseTransitions` counter,
 * which is globally monotonic and survives pod restarts. This eliminates the
 * correctness bug where a local counter would reset to 0 on restart, potentially
 * lower than epochs already written to the database.
 *
 * State is exposed via [isActive], [token], and [lastHeartbeat] — all backed
 * by [MutableStateFlow] for thread-safe, lock-free reads.
 */
@ApplicationScoped
class LeaderManager(
    private val config: FrameworkConfig,
    private val kubernetesClient: KubernetesClient,
    private val meterRegistry: MeterRegistry,
) : ShutdownParticipant {
    private val log = Logger.getLogger(LeaderManager::class.java)

    private val _isLeader = MutableStateFlow(false)
    private val _epoch = MutableStateFlow(0L)
    private val _lastHeartbeat = MutableStateFlow(Instant.now())
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    val isActive: Boolean get() = _isLeader.value
    val token: Long get() = _epoch.value
    val lastHeartbeat: Instant get() = _lastHeartbeat.value

    fun onStart(
        @Observes ev: StartupEvent,
    ) {
        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            log.info("Not running in Kubernetes — assuming leader role with synthetic epoch")
            _epoch.value = 1
            _isLeader.value = true
            registerMetrics()
            return
        }

        scope.launch { electionLoop() }
        registerMetrics()
    }

    override val shutdownOrder: Int = 0
    override val shutdownTimeout: Duration get() = config.shutdown().leaderTeardownTimeout()

    override suspend fun shutdown() {
        log.info("Shutting down leader election")
        _isLeader.value = false
        scope.coroutineContext.job.cancelAndJoin()
        releaseLeaseExplicitly()
    }

    private fun releaseLeaseExplicitly() {
        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            log.info("Not in Kubernetes — skipping explicit lease release")
            _isLeader.value = false
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
        _isLeader.value = false
    }

    // ── Election loop ──────────────────────────────────────────────────

    private suspend fun electionLoop() {
        val identity = config.worker().id()
        val leaderCfg = config.leaderElection()
        val retryPeriodMs = leaderCfg.retryPeriod().toMillis()

        try {
            while (true) {
                _lastHeartbeat.value = Instant.now()
                try {
                    runElection(identity, leaderCfg)
                    log.infof("Leader election run() returned — will retry in %dms", retryPeriodMs)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.errorf(e, "Leader election error — retrying in %dms", retryPeriodMs)
                    _isLeader.value = false
                }
                delay(retryPeriodMs)
            }
        } finally {
            log.info("Leader election coroutine exiting")
            _isLeader.value = false
        }
    }

    private suspend fun runElection(
        identity: String,
        leaderCfg: FrameworkConfig.LeaderElectionConfig,
    ) {
        val namespace = leaderCfg.namespace()
        val leaseName = leaderCfg.leaseName()
        val lock = LeaseLock(namespace, leaseName, identity)

        val electionConfig =
            LeaderElectionConfigBuilder()
                .withName(leaseName)
                .withLock(lock)
                .withLeaseDuration(leaderCfg.leaseDuration())
                .withRenewDeadline(leaderCfg.renewDeadline())
                .withRetryPeriod(leaderCfg.retryPeriod())
                .withLeaderCallbacks(
                    LeaderCallbacks(
                        { onAcquire(identity, leaderCfg) },
                        { onLose() },
                        { _lastHeartbeat.value = Instant.now() },
                    ),
                ).build()

        log.debugf("Entering leader election (identity=%s, lease=%s/%s)", identity, namespace, leaseName)
        runInterruptible {
            kubernetesClient
                .leaderElector()
                .withConfig(electionConfig)
                .build()
                .run()
        }
    }

    // ── Callbacks ──────────────────────────────────────────────────────

    private fun onAcquire(
        identity: String,
        leaderCfg: FrameworkConfig.LeaderElectionConfig,
    ) {
        val epoch = readLeaseTransitions(leaderCfg)
        _epoch.value = epoch
        _isLeader.value = true
        log.infof("Acquired leadership (identity=%s, epoch=%d)", identity, epoch)
    }

    private fun onLose() {
        _isLeader.value = false
        log.info("Lost leadership")
    }

    // ── Fencing epoch from K8s ─────────────────────────────────────────

    /**
     * Read the lease's `leaseTransitions` counter from Kubernetes.
     *
     * `leaseTransitions` is incremented by the leader election client each time
     * a new holder acquires the lease. It is persisted in etcd, so it survives
     * pod restarts and provides a globally monotonic fencing epoch.
     *
     * Falls back to local increment if the API call fails (should not happen
     * since we just acquired the lease, but defense in depth).
     */
    private fun readLeaseTransitions(leaderCfg: FrameworkConfig.LeaderElectionConfig): Long =
        try {
            val lease =
                kubernetesClient
                    .leases()
                    .inNamespace(leaderCfg.namespace())
                    .withName(leaderCfg.leaseName())
                    .get()
            val transitions = lease?.spec?.leaseTransitions ?: 0
            transitions.toLong()
        } catch (e: Exception) {
            log.warnf(e, "Failed to read lease transitions — falling back to local increment")
            _epoch.value + 1
        }

    // ── Metrics ────────────────────────────────────────────────────────

    private fun registerMetrics() {
        meterRegistry.gauge("leader_election_is_leader", this) { if (isActive) 1.0 else 0.0 }
        meterRegistry.gauge("leader_election_epoch", this) { token.toDouble() }
    }
}
