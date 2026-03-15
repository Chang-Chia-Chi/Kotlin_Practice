package com.mapreduce.dag.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class DagRunStatus {
    /** Awaiting capacity (max_parallel_runs not yet available). */
    PENDING,
    /** Active — nodes are being dispatched and executed. */
    RUNNING,
    /** All nodes completed or skipped successfully. */
    COMPLETED,
    /** At least one node failed after exhausting retries. */
    FAILED,
    /** Operator-requested cancellation (best-effort). */
    CANCELLED,
}

/** How a Run was created. */
enum class TriggerType {
    MANUAL, EVENT, SCHEDULED, SUB_DAG,
}

/**
 * A specific execution instance of a DAG Blueprint.
 * Acts as the correlation boundary (`group_id`) for underlying Layer 1 tasks.
 *
 * @param dagVersion Pinned blueprint version for this Run (immutable once created).
 * @param triggerType How the Run was created.
 * @param parentRunId For SUB_DAG: the parent Run that spawned this.
 * @param startedAt Timestamp when the first node was dispatched.
 * @param completedAt Timestamp when the Run reached a terminal state.
 * @param deadlineAt Run-level SLA deadline (alert emitted when exceeded).
 */
data class DagRun(
    @ColumnName("run_id") val runId: String,
    @ColumnName("dag_id") val dagId: String,
    val status: DagRunStatus,
    @ColumnName("dag_version") val dagVersion: Int = 1,
    @ColumnName("global_context") val globalContext: String? = null,
    @ColumnName("trigger_type") val triggerType: TriggerType = TriggerType.MANUAL,
    @ColumnName("trigger_metadata") val triggerMetadata: String? = null,
    @ColumnName("parent_run_id") val parentRunId: String? = null,
    @ColumnName("started_at") val startedAt: Instant? = null,
    @ColumnName("completed_at") val completedAt: Instant? = null,
    @ColumnName("deadline_at") val deadlineAt: Instant? = null,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
