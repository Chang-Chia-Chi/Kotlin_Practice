package com.mapreduce.event

import com.mapreduce.fanout.model.FanoutJobStatus
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.shutdown.ShutdownState
import java.time.Instant

// ── Supporting Enums ──────────────────────────────────────────

/** Outcome of a task execution, used in [TaskCompleted]. */
enum class TaskResultType {
    SUCCESS, FAILED, RETRY, DEAD_LETTERED,
}

/** Circuit breaker state, used in [CircuitBreakerStateChanged]. */
enum class CBState {
    CLOSED, OPEN, HALF_OPEN,
}

// ── Lifecycle Events ──────────────────────────────────────────

/**
 * Fired when this pod wins the Kubernetes Lease leader election.
 *
 * Producers: [com.mapreduce.leader.LeaderManager]
 * Consumers: Metrics
 */
data class LeadershipAcquired(
    val epoch: Long,
    val podId: String,
    val acquiredAt: Instant = Instant.now(),
)

/**
 * Fired when this pod loses the Kubernetes Lease
 * (renewal failed, shutdown, or stepped down).
 *
 * Producers: [com.mapreduce.leader.LeaderManager]
 * Consumers: Metrics
 */
data class LeadershipLost(
    val lastEpoch: Long,
    val podId: String,
    val lostAt: Instant = Instant.now(),
)

/**
 * Fired when the shutdown coordinator transitions between phases
 * (RUNNING → DRAINING → RELEASING → TERMINATED).
 *
 * Producers: [com.mapreduce.shutdown.ShutdownCoordinator]
 * Consumers: Metrics
 */
data class ShutdownStateChanged(
    val previousState: ShutdownState,
    val newState: ShutdownState,
    val inFlightTasks: Int,
    val drainDeadline: Instant?,
)

// ── Task Events ───────────────────────────────────────────────

/**
 * Fired after a task is successfully claimed by this pod's worker loop.
 *
 * Producers: [com.mapreduce.queue.worker.WorkerLoop]
 * Consumers: Metrics (in-flight gauge)
 */
data class TaskClaimed(
    val taskId: String,
    val handler: String,
    val queue: String,
    val groupId: String?,
    val claimedAt: Instant = Instant.now(),
)

/**
 * Fired after a task finishes execution (any outcome).
 *
 * Producers: [com.mapreduce.queue.worker.TaskDispatcher]
 * Consumers: Metrics (latency, throughput)
 */
data class TaskCompleted(
    val taskId: String,
    val handler: String,
    val queue: String,
    val groupId: String?,
    val result: TaskResultType,
    val durationMs: Long,
    val retryCount: Int,
    val errorMessage: String?,
)

/**
 * CDI event fired when a task is moved to DEAD_LETTER status.
 *
 * Consumers:
 * - [com.mapreduce.deadletter.alerting.DeadLetterAlertEvaluator] — threshold alerting
 * - [com.mapreduce.deadletter.DeadLetterMetrics] — Prometheus counters
 *
 * Producers:
 * - [com.mapreduce.queue.worker.TaskDispatcher] — handler failure / no handler
 * - [com.mapreduce.queue.worker.StaleTaskReaper] — stale reclaim exhausts retries
 */
data class TaskDeadLettered(
    val taskId: String,
    val handler: String,
    val queue: String,
    val groupId: String?,
    val retryCount: Int,
    val lastError: String,
    val createdAt: Instant?,
    val deadLetteredAt: Instant = Instant.now(),
)

/**
 * Fired when the stale task reaper reclaims a task from a dead pod.
 *
 * Producers: [com.mapreduce.queue.worker.StaleTaskReaper]
 * Consumers: Metrics (reclaim counter)
 */
data class TaskReclaimed(
    val taskId: String,
    val handler: String,
    val previousClaimedBy: String,
    val retryCount: Int,
    val reclaimedAt: Instant = Instant.now(),
)

// ── Resilience Events ─────────────────────────────────────────

/**
 * Fired when a circuit breaker transitions state.
 *
 * Producers: [com.mapreduce.queue.worker.PodCircuitBreaker]
 * Consumers: Metrics, Health probes
 */
data class CircuitBreakerStateChanged(
    val name: String,
    val previousState: CBState,
    val newState: CBState,
    val failureRate: Double,
    val changedAt: Instant = Instant.now(),
)

// ── Map-Reduce Events ─────────────────────────────────────────

/**
 * Fired when a map-reduce job transitions state.
 *
 * Producers: [com.mapreduce.mr.orchestrator.MapReduceOrchestrator]
 * Consumers: Metrics
 */
data class JobStateChanged(
    val jobId: String,
    val jobType: String,
    val previousStatus: JobStatus,
    val newStatus: JobStatus,
    val completedTasks: Int,
    val failedTasks: Int,
    val totalTasks: Int,
    val changedAt: Instant = Instant.now(),
)

// ── Fan-Out Events ───────────────────────────────────────────

/**
 * Fired when a fan-out job transitions state.
 *
 * Producers: [com.mapreduce.fanout.orchestrator.FanoutOrchestrator]
 * Consumers: Metrics
 */
data class FanoutJobStateChanged(
    val jobId: String,
    val jobType: String,
    val previousStatus: FanoutJobStatus,
    val newStatus: FanoutJobStatus,
    val completedTasks: Int,
    val failedTasks: Int,
    val totalTasks: Int,
    val changedAt: Instant = Instant.now(),
)
