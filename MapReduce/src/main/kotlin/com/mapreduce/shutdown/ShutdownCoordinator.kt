package com.mapreduce.shutdown

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Central coordinator for the graceful shutdown protocol.
 *
 * Phases:
 * - **Signal:** Set state to DRAINING, stop new claims, readiness → 503
 * - **Leader Teardown:** Cancel leader orchestration loops, release Kubernetes Lease
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

    private val _state = AtomicReference(ShutdownState.RUNNING)
    private val _tasksCompletedDuringDrain = AtomicInteger(0)
    private val _tasksReleased = AtomicInteger(0)

    val state: ShutdownState get() = _state.get()
    val isShuttingDown: Boolean get() = _state.get() != ShutdownState.RUNNING

    @Volatile
    private var _drainDeadline: Instant? = null
    val drainDeadline: Instant? get() = _drainDeadline

    @Volatile
    private var bulkheadSemaphore: Semaphore? = null

    @Volatile
    private var _bulkheadSize: Int = 0
    val bulkheadSize: Int get() = _bulkheadSize

    fun registerBulkhead(semaphore: Semaphore, size: Int) {
        bulkheadSemaphore = semaphore
        _bulkheadSize = size
    }

    val inFlightTasks: Int
        get() {
            val sem = bulkheadSemaphore ?: return 0
            return bulkheadSize - sem.availablePermits()
        }

    fun recordDrainCompletion() {
        _tasksCompletedDuringDrain.incrementAndGet()
    }

    private val leaderScopeCallbacks = mutableListOf<() -> Unit>()

    @Synchronized
    fun registerLeaderScopeCallback(cancel: () -> Unit) {
        leaderScopeCallbacks.add(cancel)
    }

    fun registerMetrics() {
        meterRegistry.gauge("taskqueue_shutdown_state", this) { state.ordinal.toDouble() }
        meterRegistry.gauge("taskqueue_shutdown_inflight_tasks", this) { inFlightTasks.toDouble() }
    }

    fun onShutdown(@Observes ev: ShutdownEvent) {
        val shutdownStart = Instant.now()
        val shutdownConfig = config.shutdown()
        val wasLeader = leaderManager.isActive

        // Signal
        val drainTimeout = shutdownConfig.drainTimeout()
        _drainDeadline = Instant.now().plus(drainTimeout)
        _state.set(ShutdownState.DRAINING)
        log.infof("Shutdown initiated. Drain deadline: %s. In-flight tasks: %d.",
            drainDeadline, inFlightTasks)

        // Leader Teardown
        if (wasLeader) {
            phaseLeaderTeardown(shutdownConfig.leaderTeardownTimeout())
        }

        // Worker Drain
        phaseWorkerDrain(drainTimeout, shutdownConfig.logInterval())

        // Release
        _state.set(ShutdownState.RELEASING)
        phaseRelease()

        // Final
        _state.set(ShutdownState.TERMINATED)
        val totalDuration = Duration.between(shutdownStart, Instant.now())

        meterRegistry.counter("taskqueue_shutdown_tasks_completed",
            "pod", config.worker().id()).increment(_tasksCompletedDuringDrain.get().toDouble())
        meterRegistry.counter("taskqueue_shutdown_tasks_released",
            "pod", config.worker().id()).increment(_tasksReleased.get().toDouble())
        meterRegistry.timer("taskqueue_shutdown_duration_seconds",
            "pod", config.worker().id(), "was_leader", wasLeader.toString())
            .record(totalDuration.toMillis(), TimeUnit.MILLISECONDS)

        log.infof(
            "Shutdown complete: pod=%s, wasLeader=%s, drainDurationMs=%d, " +
                "tasksCompletedDuringDrain=%d, tasksReleased=%d",
            config.worker().id(),
            wasLeader,
            totalDuration.toMillis(),
            _tasksCompletedDuringDrain.get(),
            _tasksReleased.get(),
        )
    }

    private fun phaseLeaderTeardown(timeout: Duration) {
        log.info("Leader teardown — cancelling orchestration loops")

        synchronized(this) {
            for (cancel in leaderScopeCallbacks) {
                try {
                    cancel()
                } catch (e: Exception) {
                    log.warnf(e, "Error cancelling leader scope")
                }
            }
        }

        Thread.sleep(timeout.toMillis().coerceAtMost(5000))

        leaderManager.releaseLeaseExplicitly()
        log.info("Leader teardown done, lease released")
    }

    private fun phaseWorkerDrain(drainTimeout: Duration, logInterval: Duration) {
        val sem = bulkheadSemaphore
        if (sem == null || inFlightTasks == 0) {
            log.info("No in-flight tasks — skipping drain")
            return
        }

        log.infof("Draining %d in-flight task(s) (timeout=%ds)",
            inFlightTasks, drainTimeout.seconds)

        val deadline = Instant.now().plus(drainTimeout)
        var lastLog = Instant.now()

        while (inFlightTasks > 0) {
            val remaining = Duration.between(Instant.now(), deadline)
            if (remaining.isNegative) {
                log.warnf("Drain timeout expired. %d task(s) still in-flight.", inFlightTasks)
                meterRegistry.counter("taskqueue_shutdown_drain_timeout_exceeded",
                    "pod", config.worker().id()).increment()
                break
            }

            val sinceLastLog = Duration.between(lastLog, Instant.now())
            if (sinceLastLog >= logInterval) {
                log.infof("Draining: %d task(s) in-flight. %ds remaining.",
                    inFlightTasks, remaining.seconds)
                lastLog = Instant.now()
            }

            Thread.sleep(1000)
        }

        if (inFlightTasks == 0) {
            log.info("All tasks drained successfully")
        }
    }

    private fun phaseRelease() {
        val podId = config.worker().id()
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
