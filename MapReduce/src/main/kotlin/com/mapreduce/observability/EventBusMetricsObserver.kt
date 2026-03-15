package com.mapreduce.observability

import com.mapreduce.event.CircuitBreakerStateChanged
import com.mapreduce.event.FanoutJobStateChanged
import com.mapreduce.event.JobStateChanged
import com.mapreduce.event.LeadershipAcquired
import com.mapreduce.event.LeadershipLost
import com.mapreduce.event.ShutdownStateChanged
import com.mapreduce.event.TaskClaimed
import com.mapreduce.event.TaskCompleted
import com.mapreduce.event.TaskReclaimed
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.ObservesAsync
import org.jboss.logging.Logger
import java.time.Duration

/**
 * Centralized async observer for all framework CDI events.
 *
 * Every observer method is annotated with `@ObservesAsync` so that:
 * - Failures never propagate to the event producer.
 * - Metrics recording runs on a managed executor, not the producer's thread.
 *
 * Each method wraps its body in try-catch for error isolation (§7).
 */
@ApplicationScoped
class EventBusMetricsObserver(
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(EventBusMetricsObserver::class.java)

    // ── Leadership Events ─────────────────────────────────────

    fun onLeadershipAcquired(@ObservesAsync event: LeadershipAcquired) {
        try {
            meterRegistry.counter("framework.leadership.acquired", "pod", event.podId).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording LeadershipAcquired metric")
        }
    }

    fun onLeadershipLost(@ObservesAsync event: LeadershipLost) {
        try {
            meterRegistry.counter("framework.leadership.lost", "pod", event.podId).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording LeadershipLost metric")
        }
    }

    // ── Shutdown Events ───────────────────────────────────────

    fun onShutdownStateChanged(@ObservesAsync event: ShutdownStateChanged) {
        try {
            meterRegistry.counter(
                "framework.shutdown.transitions",
                "from", event.previousState.name,
                "to", event.newState.name,
            ).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording ShutdownStateChanged metric")
        }
    }

    // ── Task Events ───────────────────────────────────────────

    fun onTaskClaimed(@ObservesAsync event: TaskClaimed) {
        try {
            meterRegistry.counter(
                "framework.task.claimed",
                "handler", event.handler,
                "queue", event.queue,
            ).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording TaskClaimed metric")
        }
    }

    fun onTaskCompleted(@ObservesAsync event: TaskCompleted) {
        try {
            meterRegistry.counter(
                "framework.task.completed",
                "handler", event.handler,
                "result", event.result.name,
                "queue", event.queue,
            ).increment()
            meterRegistry.timer(
                "framework.task.execution.duration",
                "handler", event.handler,
                "result", event.result.name,
            ).record(Duration.ofMillis(event.durationMs))
        } catch (e: Exception) {
            log.warnf(e, "Error recording TaskCompleted metric")
        }
    }

    fun onTaskReclaimed(@ObservesAsync event: TaskReclaimed) {
        try {
            meterRegistry.counter(
                "framework.task.reclaimed",
                "handler", event.handler,
                "previous_pod", event.previousClaimedBy,
            ).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording TaskReclaimed metric")
        }
    }

    // ── Resilience Events ─────────────────────────────────────

    fun onCircuitBreakerStateChanged(@ObservesAsync event: CircuitBreakerStateChanged) {
        try {
            meterRegistry.counter(
                "framework.circuit_breaker.transitions",
                "name", event.name,
                "from", event.previousState.name,
                "to", event.newState.name,
            ).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording CircuitBreakerStateChanged metric")
        }
    }

    // ── Map-Reduce Events ─────────────────────────────────────

    fun onJobStateChanged(@ObservesAsync event: JobStateChanged) {
        try {
            meterRegistry.counter(
                "framework.job.transitions",
                "job_type", event.jobType,
                "from", event.previousStatus.name,
                "to", event.newStatus.name,
            ).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording JobStateChanged metric")
        }
    }

    // ── Fan-Out Events ───────────────────────────────────────

    fun onFanoutJobStateChanged(@ObservesAsync event: FanoutJobStateChanged) {
        try {
            meterRegistry.counter(
                "framework.fanout.job.transitions",
                "job_type", event.jobType,
                "from", event.previousStatus.name,
                "to", event.newStatus.name,
            ).increment()
        } catch (e: Exception) {
            log.warnf(e, "Error recording FanoutJobStateChanged metric")
        }
    }
}
