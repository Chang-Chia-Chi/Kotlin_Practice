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
    @ColumnName("group_id") val groupId: String? = null,
    val metadata: String? = null,
    @ColumnName("claimed_by") val claimedBy: String? = null,
    @ColumnName("claimed_at") val claimedAt: Instant? = null,
    @ColumnName("scheduled_at") val scheduledAt: Instant? = null,
    @ColumnName("retry_count") val retryCount: Int = 0,
    @ColumnName("max_retries") val maxRetries: Int = 3,
    @ColumnName("error_message") val errorMessage: String? = null,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("completed_at") val completedAt: Instant? = null,
    @ColumnName("execution_generation") val executionGeneration: String? = null,
    val speculative: Int = 0,
)

/**
 * Context passed to a [com.mapreduce.queue.spi.TaskHandler].
 *
 * Handlers performing long-running work can check [isShuttingDown] to
 * cooperatively exit early during graceful shutdown, avoiding wasted drain time.
 */
data class TaskContext(
    val taskId: String,
    val payload: String,
    val groupId: String?,
    val metadata: String?,
    val executionGeneration: String?,
    private val shuttingDownSupplier: () -> Boolean = { false },
) {
    /** Returns true if the pod is shutting down and the handler should wrap up. */
    val isShuttingDown: Boolean get() = shuttingDownSupplier()
}

/** Value object for enqueuing a new task. */
data class EnqueueRequest(
    val handler: String,
    val payload: String,
    val queue: String = "default",
    val maxRetries: Int = 3,
    val priority: Int = 0,
    val groupId: String? = null,
    val metadata: String? = null,
    val scheduledAt: Instant? = null,
)
