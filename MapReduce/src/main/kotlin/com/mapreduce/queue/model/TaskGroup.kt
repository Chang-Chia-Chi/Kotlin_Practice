package com.mapreduce.queue.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class GroupStatus {
    ACTIVE, COMPLETED, FAILED
}

enum class FailurePolicy {
    FAIL_GROUP, THRESHOLD, BEST_EFFORT
}

/**
 * Evaluate whether a failure policy is violated.
 * Returns a failure reason string if the policy is breached, or `null` if it passes.
 */
fun evaluateFailurePolicy(
    policy: FailurePolicy,
    failed: Int,
    total: Int,
    failureThreshold: Double,
): String? = when (policy) {
    FailurePolicy.FAIL_GROUP ->
        if (failed > 0) "FAIL_GROUP: $failed task(s) failed" else null

    FailurePolicy.THRESHOLD -> {
        val rate = failed.toDouble() / total
        if (rate > failureThreshold)
            "THRESHOLD: %.1f%% > %.1f%%".format(rate * 100, failureThreshold * 100)
        else null
    }

    FailurePolicy.BEST_EFFORT -> null
}

/**
 * A group of related tasks with per-phase tracking and reactive barrier detection.
 * Replaces the MR-specific `Job` model with a generic Layer 1 primitive.
 */
data class TaskGroup(
    @ColumnName("group_id") val groupId: String,
    @ColumnName("group_type") val groupType: String,
    val status: GroupStatus,
    val params: String? = null,
    val queue: String = "default",
    val phase: String,
    @ColumnName("phase_total") val phaseTotal: Int = 0,
    @ColumnName("phase_completed") val phaseCompleted: Int = 0,
    @ColumnName("phase_failed") val phaseFailed: Int = 0,
    @ColumnName("on_complete_handler") val onCompleteHandler: String? = null,
    @ColumnName("failure_policy") val failurePolicy: FailurePolicy = FailurePolicy.FAIL_GROUP,
    @ColumnName("failure_threshold") val failureThreshold: Double = 0.0,
    @ColumnName("result_metadata") val resultMetadata: String? = null,
    val version: Long = 0,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
