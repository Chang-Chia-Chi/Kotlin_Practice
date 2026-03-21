package com.workflow.engine

import java.time.Instant

enum class WorkflowStatus { PENDING, RUNNING, COMPLETED, FAILED }

enum class ActivityStatus { PENDING, DISPATCHED, SUCCEEDED, FAILED }

enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED;

    val isTerminal: Boolean get() = this == COMPLETED || this == FAILED
}

enum class TaskType { LINEAR, SCATTER, FAN_OUT_SUB, JOIN_AGGREGATION }

data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val currentActivityIndex: Int,
    val status: WorkflowStatus,
    val version: Int,
    val createdAt: Instant,
    val updatedAt: Instant,
)

data class ActivityInstance(
    val id: String,
    val workflowRunId: String,
    val sequenceNumber: Int,
    val definitionJson: String,
    val nextActivityIndex: Int?,
    val status: ActivityStatus,
    val version: Int,
    val createdAt: Instant,
    val updatedAt: Instant,
)

data class Task(
    val id: String,
    val activityId: String,
    val type: TaskType,
    val transition: String,
    val payloadJson: String?,
    val status: TaskStatus,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
    val claimedBy: String?,
    val claimedAt: Instant?,
    val completedAt: Instant?,
    val resultJson: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
)
