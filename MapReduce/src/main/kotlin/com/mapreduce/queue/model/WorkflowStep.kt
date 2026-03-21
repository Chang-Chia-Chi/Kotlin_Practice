package com.mapreduce.queue.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class StepStatus {
    ACTIVE, COMPLETED, FAILED
}

/**
 * A workflow step with optimistic barrier detection.
 *
 * Each step in a pipeline gets its own row. Barrier detection uses
 * lock-free COUNT of remaining PENDING/CLAIMED tasks rather than a
 * decrementing counter — eliminating row-lock contention.
 */
data class WorkflowStep(
    @ColumnName("step_id") val stepId: String,
    @ColumnName("workflow_name") val workflowName: String,
    @ColumnName("run_id") val runId: String,
    val status: StepStatus,
    val params: String? = null,
    val queue: String = "default",
    @ColumnName("step_label") val stepLabel: String,
    @ColumnName("step_total") val stepTotal: Int = 0,
    @ColumnName("on_complete_handler") val onCompleteHandler: String? = null,
    @ColumnName("failure_policy") val failurePolicy: String = "FAIL_STEP",
    @ColumnName("failure_threshold") val failureThreshold: Double = 0.0,
    @ColumnName("result_metadata") val resultMetadata: String? = null,
    val version: Long = 0,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    @ColumnName("deadline_at") val deadlineAt: Instant? = null,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
