package com.mapreduce.event

import com.mapreduce.mr.model.JobStatus
import com.mapreduce.shutdown.ShutdownState
import java.time.Duration
import java.time.Instant

/** Outcome of a task execution, used in [TaskCompleted]. */
enum class TaskResultType {
    SUCCESS, FAILED, RETRY, DEAD_LETTERED,
}

/** Circuit breaker state, used in [CircuitBreakerStateChanged]. */
enum class CBState {
    CLOSED, OPEN, HALF_OPEN,
}

// ── Lifecycle Events ──────────────────────────────────────────

data class LeadershipAcquired(
    val epoch: Long,
    val podId: String,
    val acquiredAt: Instant = Instant.now(),
)

data class LeadershipLost(
    val lastEpoch: Long,
    val podId: String,
    val lostAt: Instant = Instant.now(),
)

data class ShutdownStateChanged(
    val previousState: ShutdownState,
    val newState: ShutdownState,
    val inFlightTasks: Int,
    val drainDeadline: Instant?,
)

// ── Task Events ───────────────────────────────────────────────

data class TaskClaimed(
    val taskId: String,
    val handler: String,
    val queue: String,
    val groupId: String?,
    val claimedAt: Instant = Instant.now(),
)

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

data class TaskReclaimed(
    val taskId: String,
    val handler: String,
    val previousClaimedBy: String,
    val retryCount: Int,
    val staleAge: Duration,
    val reclaimedAt: Instant = Instant.now(),
)

// ── Resilience Events ─────────────────────────────────────────

data class CircuitBreakerStateChanged(
    val name: String,
    val previousState: CBState,
    val newState: CBState,
    val failureRate: Double,
    val changedAt: Instant = Instant.now(),
)

// ── Map-Reduce Events ─────────────────────────────────────────

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
