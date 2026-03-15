package com.mapreduce.leader

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.LeadershipAcquired
import com.mapreduce.event.LeadershipLost
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
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
 *
 * Wraps fabric8's `LeaderElector` and adds the fencing token layer:
 * after each leadership transition, atomically increments a local monotonic
 * counter and exposes it as a strictly increasing [Long] epoch.
 *
 * Threading model:
 * - The election loop runs in a single dedicated daemon thread (not the Quarkus worker pool).
 * - `LeaderElector.run()` blocks until leadership is lost, then the restart loop
 *   sleeps for `retryPeriod` and re-enters the election.
 * - State is exposed via lock-free atomics — no synchronized blocks needed.
 *
 * In non-K8s environments (dev mode), the pod defaults to leader with
 * a synthetic epoch based on the current timestamp.
 */
@ApplicationScoped
class LeaderManager(
    private val config: FrameworkConfig,
    private val kubernetesClient: KubernetesClient,
    private val meterRegistry: MeterRegistry,
    private val leadershipAcquiredEvent: Event<LeadershipAcquired>,
    private val leadershipLostEvent: Event<LeadershipLost>,
) {

    private val log = Logger.getLogger(LeaderManager::class.java)

    private val _isLeader = AtomicBoolean(false)
    private val _epoch = AtomicLong(0)
    private val _lastHeartbeat = AtomicReference(Instant.now())
    private val _acquiredAt = AtomicReference<Instant?>(null)
    private val _renewedAt = AtomicReference<Instant?>(null)
    private var executor: ExecutorService? = null

    /** Whether this pod currently holds the leader lease. */
    val isActive: Boolean get() = _isLeader.get()

    /** The current fencing epoch (monotonically increasing counter). */
    val token: Long get() = _epoch.get()

    /** Last time the election loop heartbeated (for liveness probe). */
    val lastHeartbeat: Instant get() = _lastHeartbeat.get()

    /** When leadership was last acquired (null if never). */
    val acquiredAt: Instant? get() = _acquiredAt.get()

    /** When the epoch was last refreshed (null if never). */
    val renewedAt: Instant? get() = _renewedAt.get()

    fun onStart(@Observes ev: StartupEvent) {
        if (System.getenv("KUBERNETES_SERVICE_HOST") == null) {
            log.info("Not running in Kubernetes — assuming leader role with synthetic epoch")
            _epoch.set(1)
            _isLeader.set(true)
            _acquiredAt.set(Instant.now())
            registerMetrics()
            try {
                leadershipAcquiredEvent.fireAsync(LeadershipAcquired(
                    epoch = _epoch.get(),
                    podId = config.worker().id(),
                ))
            } catch (e: Exception) {
                log.warnf(e, "Failed to fire LeadershipAcquired event")
            }
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

    /**
     * Explicitly release the Kubernetes Lease so another pod can
     * acquire leadership immediately, without waiting for the lease
     * duration to expire.
     */
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

    /**
     * Blocking election loop that runs in the dedicated daemon thread.
     * After each `run()` return (leadership lost), sleeps for retryPeriod
     * and re-enters the election.
     */
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

                // run() returned — leadership lost
                log.infof("Leader election run() returned — will retry in %dms", retryPeriodMs)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            } catch (e: Exception) {
                log.errorf(e, "Leader election error — retrying in %dms", retryPeriodMs)
                _isLeader.set(false)
            }

            // Sleep before re-entering the election
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
        try {
            leadershipAcquiredEvent.fireAsync(LeadershipAcquired(
                epoch = _epoch.get(),
                podId = identity,
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire LeadershipAcquired event")
        }
    }

    private fun onLose() {
        val lastEpoch = _epoch.get()
        log.info("Lost leadership")
        _isLeader.set(false)
        try {
            leadershipLostEvent.fireAsync(LeadershipLost(
                lastEpoch = lastEpoch,
                podId = config.worker().id(),
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire LeadershipLost event")
        }
    }

    private fun onNewLeader(newLeader: String, identity: String) {
        _lastHeartbeat.set(Instant.now())
        log.debugf("Leader changed: %s", newLeader)
        // Refresh epoch when we're re-confirmed as leader (covers renewal bumps)
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
