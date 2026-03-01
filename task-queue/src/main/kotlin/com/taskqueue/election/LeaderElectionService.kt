package com.taskqueue.election

import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.event.Observes
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger
import java.time.Duration

/**
 * Kubernetes Lease-based leader election.
 *
 * Exposes [isLeader] as a reactive [StateFlow] — callers can check `.value` (polling)
 * or `.collect {}` (reactive). This powers the guard in [LeaderCronJobs].
 *
 * ### Failure Modes
 * - Pod loses API server connectivity → lease renewal fails → [isLeader] flips to false →
 *   housekeeping pauses on this pod, but task consumption continues (Oracle-only).
 * - Leader pod crashes → lease expires after [leaseDurationSeconds] → another pod acquires it.
 *
 * ### Identity
 * Each pod is identified by the HOSTNAME env var (set by K8s downward API).
 */
@Singleton
class LeaderElectionService(
    @ConfigProperty(name = "leader.election.lease-name", defaultValue = "app-leader")
    private val leaseName: String,
    @ConfigProperty(name = "leader.election.namespace", defaultValue = "default")
    private val namespace: String,
    @ConfigProperty(name = "leader.election.lease-duration-seconds", defaultValue = "15")
    private val leaseDurationSeconds: Long,
    @ConfigProperty(name = "leader.election.renew-deadline-seconds", defaultValue = "10")
    private val renewDeadlineSeconds: Long,
    @ConfigProperty(name = "leader.election.retry-period-seconds", defaultValue = "2")
    private val retryPeriodSeconds: Long,
) {

    private val log = Logger.getLogger(LeaderElectionService::class.java)

    private val _isLeader = MutableStateFlow(false)

    /** Reactive leader status. Thread-safe, non-blocking reads via `.value`. */
    val isLeader: StateFlow<Boolean> = _isLeader.asStateFlow()

    /** This pod's identity — the HOSTNAME env var set by K8s. */
    val identity: String = System.getenv("HOSTNAME") ?: "unknown-${ProcessHandle.current().pid()}"

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    fun onStart(@Observes event: StartupEvent) {
        log.infof("Starting leader election: lease=%s/%s, identity=%s", namespace, leaseName, identity)
        scope.launch { runElectionLoop() }
    }

    fun onStop(@Observes event: ShutdownEvent) {
        log.info("Shutting down leader election")
        _isLeader.value = false
        scope.cancel()
    }

    /**
     * Core election loop using the K8s Lease API.
     *
     * This method uses the `io.kubernetes:client-java-extended` LeaderElector API.
     * The callback-based API maps cleanly onto our StateFlow:
     *   onStartedLeading → _isLeader = true
     *   onStoppedLeading → _isLeader = false
     */
    private suspend fun runElectionLoop() {
        // while(true) instead of recursive tail call — suspend functions are NOT tail-call
        // optimized, so recursion would grow the stack on every leadership loss / error.
        while (true) {
            try {
                val config = io.kubernetes.client.openapi.Configuration.getDefaultApiClient()

                val lock = io.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock(
                    namespace, leaseName, identity
                )

                val electionConfig = io.kubernetes.client.extended.leaderelection.LeaderElectionConfig(
                    lock,
                    Duration.ofSeconds(leaseDurationSeconds),
                    Duration.ofSeconds(renewDeadlineSeconds),
                    Duration.ofSeconds(retryPeriodSeconds),
                )

                val elector = io.kubernetes.client.extended.leaderelection.LeaderElector(electionConfig)

                elector.run(
                    /* onStartedLeading = */ {
                        log.infof("Acquired leadership: %s", identity)
                        _isLeader.value = true
                    },
                    /* onStoppedLeading = */ {
                        log.infof("Lost leadership: %s", identity)
                        _isLeader.value = false
                    },
                )
            } catch (e: Exception) {
                log.errorf(e, "Leader election error — will retry")
                _isLeader.value = false
            }

            // Leadership was lost or an error occurred. Delay prevents tight retry loops.
            kotlinx.coroutines.delay(retryPeriodSeconds * 1000)
        }
    }
}
