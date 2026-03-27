package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import java.time.Instant
import java.util.UUID

enum class WorkflowStatus { RUNNING, COMPLETED, FAILED }

enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED, DEAD_LETTER;

    val isTerminal: Boolean get() = this == COMPLETED || this == FAILED || this == DEAD_LETTER
}

data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val currentSequence: Int,
    val version: Int,
    val status: WorkflowStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
)

data class Task(
    val id: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val status: TaskStatus,
    val handlerKey: String,
    val payloadJson: String?,
    val resultJson: String?,
    val claimedBy: String?,
    val claimedAt: Instant?,
    val completedAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
)

internal fun createTaskForActivity(
    workflowId: String,
    sequenceNumber: Int,
    activity: ActivityDefinition,
    payload: String?,
    now: Instant,
): Task {
    return Task(
        id = UUID.randomUUID().toString(),
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = TaskStatus.PENDING,
        handlerKey = activity.transition,
        payloadJson = payload,
        resultJson = null,
        claimedBy = null,
        claimedAt = null,
        completedAt = null,
        retryCount = 0,
        maxRetries = activity.retries,
        deadlineAt = now.plus(activity.deadline),
    )
}
