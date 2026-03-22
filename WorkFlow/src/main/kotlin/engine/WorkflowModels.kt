package com.workflow.engine

import java.time.Instant

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
