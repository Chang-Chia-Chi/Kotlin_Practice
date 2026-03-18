package com.mapreduce.shutdown

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Central coordinator for the graceful shutdown protocol.
 *
 * This is the **single entry point** for shutdown — no other bean should
 * observe [ShutdownEvent] for lifecycle teardown. This eliminates CDI
 * observer ordering races.
 *
 * Phases:
 * - **Signal:** Set state to DRAINING, stop new claims, readiness → 503
 * - **Leader Teardown:** Await election loop completion, release Kubernetes Lease
 * - **Worker Drain:** Await in-flight tasks up to drainTimeout
 * - **Release:** Flip uncompleted CLAIMED tasks back to PENDING
 * - **Final:** Emit metrics, log summary, state → TERMINATED
 */
@ApplicationScoped
class ShutdownCoordinator(
    private val config: FrameworkConfig,
    private val leaderManager: LeaderManager,
    private val taskRepository: TaskRepository,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(ShutdownCoordinator::class.java)
    private val podId = config.worker().id()

    private val _state = AtomicReference(ShutdownState.RUNNING)
    private val _tasksCompletedDuringDrain = AtomicInteger(0)
    private val _tasksReleased = AtomicInteger(0)

    val state: ShutdownState get() = _state.get()
    val isShuttingDown: Boolean get() = _state.get() != ShutdownState.RUNNING

    @Volatile
    private var _drainDeadline: Instant? = null
    val drainDeadline: Instant? get() = _drainDeadline

    private val _inFlightTasks = AtomicInteger(0)
    val inFlightTasks: Int get() = _inFlightTasks.get()

    fun trackTaskStart() {
        _inFlightTasks.incrementAndGet()
    }

    fun trackTaskEnd() {
        _inFlightTasks.decrementAndGet()
    }

    fun recordDrainCompletion() {
        _tasksCompletedDuringDrain.incrementAndGet()
    }

    fun onShutdown(@Observes ev: ShutdownEvent) = runBlocking {
        val shutdownStart = Instant.now()
        val shutdownConfig = config.shutdown()
        val wasLeader = leaderManager.isActive

        // Signal — poll loops will see DRAINING and stop claiming
        val drainTimeout = shutdownConfig.drainTimeout()
        _drainDeadline = Instant.now().plus(drainTimeout)
        _state.set(ShutdownState.DRAINING)
        log.infof("Shutdown initiated. Drain deadline: %s. In-flight tasks: %d.",
            drainDeadline, inFlightTasks)

        // Leader Teardown — always run to clean up election scope
        phaseLeaderTeardown(shutdownConfig.leaderTeardownTimeout())

        // Worker Drain
        phaseWorkerDrain(drainTimeout, shutdownConfig.logInterval())

        // Release
        _state.set(ShutdownState.RELEASING)
        phaseRelease()

        // Final
        _state.set(ShutdownState.TERMINATED)
        val totalDuration = Duration.between(shutdownStart, Instant.now())

        meterRegistry.counter("taskqueue_shutdown_tasks_completed",
            "pod", podId).increment(_tasksCompletedDuringDrain.get().toDouble())
        meterRegistry.counter("taskqueue_shutdown_tasks_released",
            "pod", podId).increment(_tasksReleased.get().toDouble())
        meterRegistry.timer("taskqueue_shutdown_duration_seconds",
            "pod", podId, "was_leader", wasLeader.toString())
            .record(totalDuration.toMillis(), TimeUnit.MILLISECONDS)

        log.infof(
            "Shutdown complete: pod=%s, wasLeader=%s, drainDurationMs=%d, " +
                "tasksCompletedDuringDrain=%d, tasksReleased=%d",
            podId,
            wasLeader,
            totalDuration.toMillis(),
            _tasksCompletedDuringDrain.get(),
            _tasksReleased.get(),
        )
    }

    /**
     * Await leader election loop completion and release the K8s lease.
     * [LeaderManager.shutdown] uses `cancelAndJoin` — it returns only after
     * the election coroutine's finally block has run.
     */
    private suspend fun phaseLeaderTeardown(timeout: Duration) {
        log.info("Leader teardown — stopping election loop and releasing lease")
        try {
            withTimeoutOrNull(timeout.toMillis()) {
                leaderManager.shutdown()
            } ?: log.warn("Leader teardown timed out — proceeding to drain phase")
        } catch (e: Exception) {
            log.warnf(e, "Error during leader teardown")
        }
        log.info("Leader teardown done")
    }

    private suspend fun phaseWorkerDrain(drainTimeout: Duration, logInterval: Duration) {
        if (inFlightTasks == 0) {
            log.info("No in-flight tasks — skipping drain")
            return
        }

        log.infof("Draining %d in-flight task(s) (timeout=%ds)",
            inFlightTasks, drainTimeout.seconds)

        val drained = withTimeoutOrNull(drainTimeout.toMillis()) {
            while (inFlightTasks > 0) {
                delay(logInterval.toMillis())
                if (inFlightTasks > 0) {
                    log.infof("Draining: %d task(s) still in-flight.", inFlightTasks)
                }
            }
        }

        if (drained == null) {
            log.warnf("Drain timeout expired. %d task(s) still in-flight.", inFlightTasks)
            meterRegistry.counter("taskqueue_shutdown_drain_timeout_exceeded",
                "pod", podId).increment()
        } else {
            log.info("All tasks drained successfully")
        }
    }

    private fun phaseRelease() {
        log.info("Releasing uncompleted tasks back to PENDING")

        try {
            val released = taskRepository.releaseTasksByPod(podId)
            _tasksReleased.set(released)
            if (released > 0) {
                log.infof("Released %d task(s) back to PENDING", released)
            } else {
                log.info("No tasks to release")
            }
        } catch (e: Exception) {
            log.errorf(e, "Failed to release tasks — stale reaper will recover them")
        }
    }
}
