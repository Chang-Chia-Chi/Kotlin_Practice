package com.workflow.workflow.model

import java.time.Instant
import java.util.UUID

data class Task(
    val id: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val status: TaskStatus,
    val handlerKey: String,
    val item: String? = null,
    val resultJson: String?,
    val claimedBy: String?,
    val claimedAt: Instant?,
    val completedAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
    val notBefore: Instant? = null,
    val backoffBase: Int = 1,
    val backoffCap: Int = 300,
    val enqueuedAt: Instant = Instant.EPOCH,
    val queueName: String = "default",
)

internal fun createTaskForActivity(
    workflowId: String,
    sequenceNumber: Int,
    activity: ActivityDefinition,
    now: Instant,
    item: String? = null,
): Task {
    return Task(
        id = UUID.randomUUID().toString(),
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = TaskStatus.PENDING,
        handlerKey = activity.transition,
        item = item,
        resultJson = null,
        claimedBy = null,
        claimedAt = null,
        completedAt = null,
        retryCount = 0,
        maxRetries = activity.retries,
        deadlineAt = now.plus(activity.deadline),
        backoffBase = activity.backoffBase.seconds.toInt(),
        backoffCap = activity.backoffCap.seconds.toInt(),
        queueName = activity.queue,
    )
}
