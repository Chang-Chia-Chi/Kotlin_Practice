package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import java.time.Instant
import java.util.UUID

enum class WorkflowStatus {
    RUNNING, COMPLETED, FAILED, TIMED_OUT, CANCELLED;

    val isTerminal: Boolean get() = this != RUNNING

    companion object {
        private val allowed = setOf(
            RUNNING to COMPLETED,
            RUNNING to FAILED,
            RUNNING to TIMED_OUT,
            RUNNING to CANCELLED,
            FAILED to RUNNING,       // future: workflow reclaim
            TIMED_OUT to RUNNING,    // future: workflow reclaim
            CANCELLED to RUNNING,    // future: workflow reclaim
        )

        fun requireTransition(from: WorkflowStatus, to: WorkflowStatus) {
            require((from to to) in allowed) {
                "Illegal workflow transition: $from → $to"
            }
        }
    }
}

enum class TaskStatus {
    PENDING, PROCESSING, WAITING_FOR_SIGNAL, COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED;

    val isTerminal: Boolean get() = this in terminalStatuses

    companion object {
        private val terminalStatuses = setOf(COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED)
        private val allowed = setOf(
            PENDING to PROCESSING,
            PENDING to CANCELLED,
            PROCESSING to COMPLETED,
            PROCESSING to FAILED,
            PROCESSING to TIMED_OUT,
            PROCESSING to PENDING,              // stale reclaim
            PROCESSING to DEAD_LETTER,
            PROCESSING to WAITING_FOR_SIGNAL,   // handler suspends task
            WAITING_FOR_SIGNAL to COMPLETED,    // signal: approved
            WAITING_FOR_SIGNAL to FAILED,       // signal: rejected
            WAITING_FOR_SIGNAL to TIMED_OUT,    // sweeper: deadline expired
            WAITING_FOR_SIGNAL to CANCELLED,    // workflow cancelled
            FAILED to PENDING,                  // future: retry-on-failure
            FAILED to DEAD_LETTER,              // future: retry-on-failure exhausted
        )

        fun requireTransition(from: TaskStatus, to: TaskStatus) {
            require((from to to) in allowed) {
                "Illegal task transition: $from → $to"
            }
        }
    }
}

data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val currentSequence: Int,
    val version: Int,
    val status: WorkflowStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val deadlineAt: Instant,
)

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

sealed interface StartResult {
    data class Created(val workflowId: String) : StartResult
    data class AlreadyExists(val workflowId: String) : StartResult
}

val StartResult.workflowId: String
    get() = when (this) {
        is StartResult.Created -> workflowId
        is StartResult.AlreadyExists -> workflowId
    }
