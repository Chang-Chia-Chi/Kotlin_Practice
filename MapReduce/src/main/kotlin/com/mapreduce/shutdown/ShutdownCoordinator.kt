package com.mapreduce.shutdown

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.ShutdownStateChanged
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import jakarta.enterprise.event.Observes
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Central coordinator for the four-phase graceful shutdown protocol.
 *
 * Observes the Quarkus [ShutdownEvent] and runs all phases in strict order:
 *
 * - **Phase 0 — Signal:** Set state to DRAINING, stop new claims, readiness → 503
 * - **Phase 1 — Leader Teardown:** Cancel leader orchestration loops, release Kubernetes Lease
 * - **Phase 2 — Worker Drain:** Await in-flight tasks up to [drainTimeout]
 * - **Phase 3 — Release:** Flip uncompleted CLAIMED tasks back to PENDING (no retry increment)
 * - **Phase 4 — Final:** Emit metrics, log summary, state → TERMINATED
 *
 * The coordinator exposes [state] as an [AtomicReference] that the worker loop checks
 * before every claim cycle.
 */
@ApplicationScoped
class ShutdownCoordinator(
    private val config: FrameworkConfig,
    private val leaderManager: LeaderManager,
    private val taskRepository: TaskRepository,
    private val meterRegistry: MeterRegistry,
    private val shutdownStateEvent: Event<ShutdownStateChanged>,
) {

    private val log = Logger.getLogger(ShutdownCoordinator::class.java)

    private val _state = AtomicReference(ShutdownState.RUNNING)
    private val _tasksCompletedDuringDrain = AtomicInteger(0)
    private val _tasksReleased = AtomicInteger(0)

    /** Current shutdown state — checked by the worker loop on every claim cycle. */
    val state: ShutdownState get() = _state.get()

    /** Whether the pod is shutting down (anything other than RUNNING). */
    val isShuttingDown: Boolean get() = _state.get() != ShutdownState.RUNNING

    /** Drain deadline (null until Phase 0). */
    @Volatile
    private var _drainDeadline: Instant? = null
    val drainDeadline: Instant? get() = _drainDeadline

    // ── Bulkhead integration ──────────────────────────────────────
    // Set by WorkerLoop at startup so the coordinator can track in-flight tasks.

    @Volatile
    private var bulkheadSemaphore: Semaphore? = null

    @Volatile
    private var _bulkheadSize: Int = 0

    /** Maximum bulkhead permits (concurrency limit). Exposed for utilization gauge. */
    val bulkheadSize: Int get() = _bulkheadSize

    /** Called by WorkerLoop to register its bulkhead for drain tracking. */
    fun registerBulkhead(semaphore: Semaphore, size: Int) {
        bulkheadSemaphore = semaphore
        _bulkheadSize = size
    }

    /** Number of tasks currently executing (bulkhead slots in use). */
    val inFlightTasks: Int
        get() {
            val sem = bulkheadSemaphore ?: return 0
            return bulkheadSize - sem.availablePermits()
        }

    /** Increment the counter of tasks that completed during the drain window. */
    fun recordDrainCompletion() {
        _tasksCompletedDuringDrain.incrementAndGet()
    }

    // ── Leader scope management ──────────────────────────────────
    // Leader orchestration loops register here so the coordinator can cancel them.

    private val leaderScopeCallbacks = mutableListOf<() -> Unit>()

    /**
     * Register a callback that cancels a leader orchestration scope.
     * Called by StaleTaskReaper, MapReduceOrchestrator, DagOrchestrator.
     */
    @Synchronized
    fun registerLeaderScopeCallback(cancel: () -> Unit) {
        leaderScopeCallbacks.add(cancel)
    }

    // ── Metrics ──────────────────────────────────────────────────

    fun registerMetrics() {
        meterRegistry.gauge("taskqueue_shutdown_state", this) { state.ordinal.toDouble() }
        meterRegistry.gauge("taskqueue_shutdown_inflight_tasks", this) { inFlightTasks.toDouble() }
    }

    // ── Shutdown sequence (Phases 0–4) ───────────────────────────

    fun onShutdown(@Observes ev: ShutdownEvent) {
        val shutdownStart = Instant.now()
        val shutdownConfig = config.shutdown()
        val wasLeader = leaderManager.isActive

        // ── Phase 0: Signal ──────────────────────────────────────
        val drainTimeout = shutdownConfig.drainTimeout()
        _drainDeadline = Instant.now().plus(drainTimeout)
        _state.set(ShutdownState.DRAINING)
        fireStateChanged(ShutdownState.RUNNING, ShutdownState.DRAINING)
        log.infof("Shutdown initiated. Drain deadline: %s. In-flight tasks: %d.",
            drainDeadline, inFlightTasks)

        // ── Phase 1: Leader Teardown (if applicable) ─────────────
        if (wasLeader) {
            phaseLeaderTeardown(shutdownConfig.leaderTeardownTimeout())
        }

        // ── Phase 2: Worker Drain ────────────────────────────────
        phaseWorkerDrain(drainTimeout, shutdownConfig.logInterval())

        // ── Phase 3: Release ─────────────────────────────────────
        _state.set(ShutdownState.RELEASING)
        fireStateChanged(ShutdownState.DRAINING, ShutdownState.RELEASING)
        phaseRelease()

        // ── Phase 4: Final ───────────────────────────────────────
        _state.set(ShutdownState.TERMINATED)
        fireStateChanged(ShutdownState.RELEASING, ShutdownState.TERMINATED)
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

    private fun fireStateChanged(previous: ShutdownState, new: ShutdownState) {
        try {
            shutdownStateEvent.fireAsync(ShutdownStateChanged(
                previousState = previous,
                newState = new,
                inFlightTasks = inFlightTasks,
                drainDeadline = _drainDeadline,
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire ShutdownStateChanged event")
        }
    }

    // ── Phase 1: Leader Teardown ─────────────────────────────────

    private fun phaseLeaderTeardown(timeout: Duration) {
        log.info("Phase 1: Leader teardown — cancelling orchestration loops")

        // Cancel all leader orchestration scopes
        synchronized(this) {
            for (cancel in leaderScopeCallbacks) {
                try {
                    cancel()
                } catch (e: Exception) {
                    log.warnf(e, "Error cancelling leader scope")
                }
            }
        }

        // Give loops time to exit (they cancel at their next suspension point)
        Thread.sleep(timeout.toMillis().coerceAtMost(5000))

        // Release the Kubernetes Lease explicitly for fast leader handoff
        leaderManager.releaseLeaseExplicitly()
        log.info("Phase 1 complete: Leader teardown done, lease released")
    }

    // ── Phase 2: Worker Drain ────────────────────────────────────

    private fun phaseWorkerDrain(drainTimeout: Duration, logInterval: Duration) {
        val sem = bulkheadSemaphore
        if (sem == null || inFlightTasks == 0) {
            log.info("Phase 2: No in-flight tasks — skipping drain")
            return
        }

        log.infof("Phase 2: Draining %d in-flight task(s) (timeout=%ds)",
            inFlightTasks, drainTimeout.seconds)

        val deadline = Instant.now().plus(drainTimeout)
        var lastLog = Instant.now()

        // Try to acquire all permits = all slots idle
        while (inFlightTasks > 0) {
            val remaining = Duration.between(Instant.now(), deadline)
            if (remaining.isNegative) {
                log.warnf("Phase 2: Drain timeout expired. %d task(s) still in-flight.", inFlightTasks)
                meterRegistry.counter("taskqueue_shutdown_drain_timeout_exceeded",
                    "pod", config.worker().id()).increment()
                break
            }

            // Log progress at intervals
            val sinceLastLog = Duration.between(lastLog, Instant.now())
            if (sinceLastLog >= logInterval) {
                log.infof("Draining: %d task(s) in-flight. %ds remaining.",
                    inFlightTasks, remaining.seconds)
                lastLog = Instant.now()
            }

            // Poll — sleep briefly and re-check
            Thread.sleep(1000)
        }

        if (inFlightTasks == 0) {
            log.info("Phase 2 complete: All tasks drained successfully")
        }
    }

    // ── Phase 3: Release ─────────────────────────────────────────

    private fun phaseRelease() {
        val podId = config.worker().id()
        log.info("Phase 3: Releasing uncompleted tasks back to PENDING")

        try {
            val released = taskRepository.releaseTasksByPod(podId)
            _tasksReleased.set(released)
            if (released > 0) {
                log.infof("Phase 3: Released %d task(s) back to PENDING", released)
            } else {
                log.info("Phase 3: No tasks to release")
            }
        } catch (e: Exception) {
            log.errorf(e, "Phase 3: Failed to release tasks — stale reaper will recover them")
        }
    }
}
