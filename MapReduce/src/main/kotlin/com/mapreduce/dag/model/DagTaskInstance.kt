package com.mapreduce.dag.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class TaskInstanceStatus {
    /** Waiting for upstream dependencies to resolve. */
    BLOCKED,
    /** Dependencies satisfied, eligible for dispatch. */
    READY,
    /** Enqueued into Layer 1 task table, awaiting worker pickup. */
    QUEUED,
    /** Layer 1 task claimed by a worker and executing. */
    RUNNING,
    /** Handler returned success. */
    COMPLETED,
    /** Trigger rule not satisfied and all parents terminal. */
    SKIPPED,
    /** Handler returned failure after exhausting all retries. */
    FAILED,
    /** Execution exceeded timeout_at deadline. */
    TIMED_OUT,
    ;

    /** Whether this status represents a final, irreversible state. */
    val isTerminal: Boolean
        get() = this in TERMINAL_STATES

    companion object {
        val TERMINAL_STATES = setOf(COMPLETED, SKIPPED, FAILED, TIMED_OUT)
    }
}

enum class TriggerRule {
    /** All upstream parents must complete successfully. */
    ALL_SUCCESS,
    /** At least one upstream parent must complete successfully, rest terminal. */
    ONE_SUCCESS,
    /** All upstream parents must reach a terminal state (any state). */
    ALL_DONE,
    /** All upstream parents are COMPLETED or SKIPPED, none FAILED. */
    NONE_FAILED,
    /** Only dispatched when the Run has been marked FAILED (error-handler hook). */
    ON_FAILURE,
}

/** Error classification returned by handlers to drive retry behavior. */
enum class ErrorClass {
    /** Network timeout, DB lock, temporary unavailability — retryable. */
    TRANSIENT,
    /** Bad input, validation failure — not retryable, may skip branch. */
    DATA_ERROR,
    /** Infrastructure failure, config error — not retryable, fail Run. */
    FATAL,
}

/**
 * A single node within a [DagRun]. Backed 1:1 by a generic Layer 1 task
 * once dispatched (status transitions from READY → QUEUED → RUNNING).
 *
 * @param attempt Current attempt number (1-indexed). Incremented on DAG-level retry.
 * @param maxAttempts Maximum attempts before the node is marked FAILED.
 * @param error Structured error payload (classification, message, stacktrace reference).
 * @param timeoutAt Absolute deadline for the current attempt.
 * @param dispatchedAt When the node was last transitioned to QUEUED.
 * @param completedAt When the node reached a terminal state.
 */
data class DagTaskInstance(
    @ColumnName("instance_id") val instanceId: String,
    @ColumnName("run_id") val runId: String,
    @ColumnName("task_key") val taskKey: String,
    @ColumnName("node_type") val nodeType: String,
    @ColumnName("task_type") val taskType: String? = null,
    val dependencies: String? = null,
    val status: TaskInstanceStatus = TaskInstanceStatus.BLOCKED,
    @ColumnName("trigger_rule") val triggerRule: TriggerRule = TriggerRule.ALL_SUCCESS,
    val attempt: Int = 1,
    @ColumnName("max_attempts") val maxAttempts: Int = 1,
    @ColumnName("resolved_config") val resolvedConfig: String? = null,
    @ColumnName("output_data") val outputData: String? = null,
    val error: String? = null,
    @ColumnName("task_id") val taskId: String? = null,
    @ColumnName("timeout_at") val timeoutAt: Instant? = null,
    @ColumnName("dispatched_at") val dispatchedAt: Instant? = null,
    @ColumnName("completed_at") val completedAt: Instant? = null,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
