package com.mapreduce.dag.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class TaskInstanceStatus {
    BLOCKED, READY, RUNNING, COMPLETED, SKIPPED, FAILED
}

enum class TriggerRule {
    /** All upstream parents must complete successfully. */
    ALL_SUCCESS,
    /** At least one upstream parent must complete successfully. */
    ONE_SUCCESS,
    /** All upstream parents must reach a terminal state (success, failure, or skip). */
    ALL_DONE,
}

/**
 * A single node within a [DagRun]. Backed 1:1 by a generic Layer 1 task
 * once dispatched (status transitions from READY to RUNNING).
 */
data class DagTaskInstance(
    @ColumnName("instance_id") val instanceId: String,
    @ColumnName("run_id") val runId: String,
    @ColumnName("task_key") val taskKey: String,
    @ColumnName("node_type") val nodeType: String,
    val dependencies: String? = null,
    val status: TaskInstanceStatus = TaskInstanceStatus.BLOCKED,
    @ColumnName("trigger_rule") val triggerRule: TriggerRule = TriggerRule.ALL_SUCCESS,
    @ColumnName("output_data") val outputData: String? = null,
    @ColumnName("task_id") val taskId: String? = null,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
