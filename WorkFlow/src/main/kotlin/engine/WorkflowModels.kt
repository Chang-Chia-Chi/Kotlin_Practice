package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import java.time.Instant
import java.util.UUID

enum class WorkflowStatus { RUNNING, COMPLETED, FAILED }

enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED;

    val isTerminal: Boolean get() = this == COMPLETED || this == FAILED
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
    isScatter: Boolean,
    payload: String?,
    now: Instant,
): Task {
    val handlerKey = if (isScatter) activity.fanOut!!.transition else activity.transition
    val maxRetries = if (isScatter) activity.fanOut!!.retries else activity.retries
    val deadline = if (isScatter) activity.fanOut!!.deadline else activity.deadline
    return Task(
        id = UUID.randomUUID().toString(),
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = TaskStatus.PENDING,
        handlerKey = handlerKey,
        payloadJson = payload,
        resultJson = null,
        claimedBy = null,
        claimedAt = null,
        completedAt = null,
        retryCount = 0,
        maxRetries = maxRetries,
        deadlineAt = now.plus(deadline),
    )
}
