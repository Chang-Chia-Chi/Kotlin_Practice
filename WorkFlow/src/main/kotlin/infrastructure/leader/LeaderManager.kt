package com.workflow.infrastructure.leader

import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.infrastructure.shutdown.ShutdownParticipant
import com.workflow.worker.config.WorkerLoopConfig
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
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import org.jboss.logging.Logger
import java.time.Clock
import java.time.Duration
import java.time.Instant

@ApplicationScoped
class LeaderManager(
    private val workerLoopConfig: WorkerLoopConfig,
    private val leaderElectionConfig: LeaderElectionConfig,
    private val shutdownConfig: ShutdownConfig,
    private val kubernetesClient: KubernetesClient,
    private val meterRegistry: MeterRegistry,
    private val kubernetesDetector: KubernetesDetector,
) : LeaderElection, ShutdownParticipant {
    private val log = Logger.getLogger(LeaderManager::class.java)

    internal var clock: Clock = Clock.systemUTC()
    internal var scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val _isLeader = MutableStateFlow(false)
    private val _epoch = MutableStateFlow(0L)
    private val _lastHeartbeat by lazy(LazyThreadSafetyMode.NONE) { MutableStateFlow(Instant.now(clock)) }

    val leaderState: StateFlow<Boolean> get() = _isLeader.asStateFlow()

    override val isActive: Boolean get() = _isLeader.value
    override val token: Long get() = _epoch.value
    override val lastHeartbeat: Instant get() = _lastHeartbeat.value

    fun onStart(
        @Observes ev: StartupEvent,
    ) {
        if (!kubernetesDetector.isRunningInKubernetes()) {
            log.info("Not running in Kubernetes — assuming leader role with synthetic epoch")
            _epoch.value = 1
            _isLeader.value = true
            registerMetrics()
            return
        }

        scope.launch { electionLoop() }
        registerMetrics()
    }

    override val shutdownOrder: Int = 1
    override val shutdownTimeout: Duration get() = shutdownConfig.leaderTeardownTimeout()

    override suspend fun shutdown() {
        log.info("Shutting down leader election")
        _isLeader.value = false
        scope.coroutineContext.job.cancelAndJoin()
        withTimeoutOrNull(shutdownTimeout.toMillis()) {
            withContext(Dispatchers.IO) {
                releaseLeaseExplicitly()
            }
        } ?: log.warn("Lease release timed out — new leader will acquire after lease expiry")
    }

    private fun releaseLeaseExplicitly() {
        if (!kubernetesDetector.isRunningInKubernetes()) {
            log.info("Not in Kubernetes — skipping explicit lease release")
            _isLeader.value = false
            return
        }
        try {
            val leaseApi = kubernetesClient.leases().inNamespace(leaderElectionConfig.namespace())
            val lease = leaseApi.withName(leaderElectionConfig.leaseName()).get()
            if (lease != null) {
                val identity = workerLoopConfig.id()
                if (lease.spec.holderIdentity == identity) {
                    lease.spec.holderIdentity = null
                    lease.spec.acquireTime = null
                    leaseApi.withName(leaderElectionConfig.leaseName()).patch(lease)
                    log.info("Lease released explicitly — new leader can acquire immediately")
                } else {
                    log.debugf("Lease held by %s, not by this instance — skipping release", lease.spec.holderIdentity)
                }
            }
        } catch (e: Exception) {
            log.warnf(e, "Failed to release lease explicitly — new leader will acquire after lease expiry")
        }
    }

    private suspend fun electionLoop() {
        val identity = workerLoopConfig.id()
        val retryPeriodMs = leaderElectionConfig.retryPeriod().toMillis()

        try {
            while (true) {
                _lastHeartbeat.value = Instant.now(clock)
                try {
                    runElection(identity)
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

    private suspend fun runElection(identity: String) {
        val namespace = leaderElectionConfig.namespace()
        val leaseName = leaderElectionConfig.leaseName()
        val lock = LeaseLock(namespace, leaseName, identity)

        val electionConfig =
            LeaderElectionConfigBuilder()
                .withName(leaseName)
                .withLock(lock)
                .withLeaseDuration(leaderElectionConfig.leaseDuration())
                .withRenewDeadline(leaderElectionConfig.renewDeadline())
                .withRetryPeriod(leaderElectionConfig.retryPeriod())
                .withLeaderCallbacks(
                    LeaderCallbacks(
                        { onAcquire(identity) },
                        { onLose() },
                        { _lastHeartbeat.value = Instant.now(clock) },
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

    private fun onAcquire(identity: String) {
        val epoch = readLeaseTransitions()
        _epoch.value = epoch
        _isLeader.value = true
        log.infof("Acquired leadership (identity=%s, epoch=%d)", identity, epoch)
    }

    private fun onLose() {
        _isLeader.value = false
        log.info("Lost leadership")
    }

    private fun readLeaseTransitions(): Long =
        try {
            val lease =
                kubernetesClient
                    .leases()
                    .inNamespace(leaderElectionConfig.namespace())
                    .withName(leaderElectionConfig.leaseName())
                    .get()
            val transitions = lease?.spec?.leaseTransitions ?: 0
            transitions.toLong()
        } catch (e: Exception) {
            log.warnf(e, "Failed to read lease transitions — falling back to local increment")
            _epoch.value + 1
        }

    private fun registerMetrics() {
        meterRegistry.gauge("leader_election_is_leader", this) { if (isActive) 1.0 else 0.0 }
        meterRegistry.gauge("leader_election_epoch", this) { token.toDouble() }
        meterRegistry.gauge("leader_election_heartbeat_age_seconds", this) {
            Duration.between(lastHeartbeat, Instant.now(clock)).toSeconds().toDouble()
        }
    }
}
