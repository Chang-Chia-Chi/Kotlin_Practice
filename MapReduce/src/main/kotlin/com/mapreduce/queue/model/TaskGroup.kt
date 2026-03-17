package com.mapreduce.queue.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class GroupStatus {
    ACTIVE, COMPLETED, FAILED
}

/**
 * A group of related tasks with countdown barrier detection.
 *
 * [tasksPending] counts down to zero as tasks reach terminal states.
 * [tasksFailed] tracks how many of those were failures (for policy evaluation).
 * The generic queue layer stores [failurePolicy] and [failureThreshold] as opaque
 * values — only the callback handler (Layer 2) interprets them.
 */
data class TaskGroup(
    @ColumnName("group_id") val groupId: String,
    @ColumnName("group_type") val groupType: String,
    val status: GroupStatus,
    val params: String? = null,
    val queue: String = "default",
    val phase: String,
    @ColumnName("phase_total") val phaseTotal: Int = 0,
    @ColumnName("tasks_pending") val tasksPending: Int = 0,
    @ColumnName("tasks_failed") val tasksFailed: Int = 0,
    @ColumnName("on_complete_handler") val onCompleteHandler: String? = null,
    @ColumnName("failure_policy") val failurePolicy: String = "FAIL_GROUP",
    @ColumnName("failure_threshold") val failureThreshold: Double = 0.0,
    @ColumnName("result_metadata") val resultMetadata: String? = null,
    val version: Long = 0,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    @ColumnName("deadline_at") val deadlineAt: Instant? = null,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
