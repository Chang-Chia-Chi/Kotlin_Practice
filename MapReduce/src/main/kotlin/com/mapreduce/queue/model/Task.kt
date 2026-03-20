package com.mapreduce.queue.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class TaskStatus {
    PENDING, CLAIMED, COMPLETED, FAILED, DEAD_LETTER
}

/**
 * The universal unit of work. Every task has a [handler] routing key,
 * an opaque [payload], and lifecycle state. The framework claims tasks,
 * invokes the handler, manages retries, and records outcomes.
 */
data class Task(
    @ColumnName("task_id") val taskId: String,
    val handler: String,
    val queue: String = "default",
    val payload: String,
    val status: TaskStatus = TaskStatus.PENDING,
    val priority: Int = 0,
    @ColumnName("step_id") val stepId: String? = null,
    val metadata: String? = null,
    @ColumnName("claimed_by") val claimedBy: String? = null,
    @ColumnName("claimed_at") val claimedAt: Instant? = null,
    @ColumnName("scheduled_at") val scheduledAt: Instant? = null,
    @ColumnName("retry_count") val retryCount: Int = 0,
    @ColumnName("max_retries") val maxRetries: Int = 3,
    @ColumnName("error_message") val errorMessage: String? = null,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("completed_at") val completedAt: Instant? = null,
    @ColumnName("execution_generation") val claimToken: String? = null,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    @ColumnName("output_uri") val outputUri: String? = null,
    @ColumnName("output_metadata") val outputMetadata: String? = null,
)

/**
 * Unified context passed through the middleware pipeline and into handlers.
 *
 * Handlers performing long-running work can check shutdown state via the
 * top-level [com.mapreduce.shutdown.isShuttingDown] suspend function.
 */
data class TaskContext(
    val taskId: String,
    val handler: String,
    val queue: String,
    val payload: String,
    val stepId: String? = null,
    val metadata: String? = null,
    val claimToken: String? = null,
    val retryCount: Int = 0,
    val maxRetries: Int = 3,
)

/** Value object for enqueuing a new task. */
data class EnqueueRequest(
    val handler: String,
    val payload: String,
    val queue: String = "default",
    val maxRetries: Int = 3,
    val priority: Int = 0,
    val stepId: String? = null,
    val metadata: String? = null,
    val scheduledAt: Instant? = null,
)
