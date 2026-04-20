package com.taskqueue.queue

import java.time.Instant

/**
 * Immutable snapshot of a claimed task, passed to [TaskHandler.handle].
 *
 * Handlers use this to read task metadata and payload.
 * Deadline checking is intentionally a method (not a constructor-time flag)
 * because the handler may run for a non-trivial duration after construction.
 */
data class TaskContext(
    val taskId: Long,
    val parentTaskId: Long?,
    val taskType: String,
    val payload: String?,
    val priority: Int,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
    val scheduledAt: Instant?,
    val createdAt: Instant,
) {
    /** Check at call-time — not construction-time — so long-running handlers get a fresh read. */
    fun isExpired(): Boolean =
        deadlineAt != null && Instant.now().isAfter(deadlineAt)

    /** True if the handler has retries remaining after a failure. */
    fun hasRetriesLeft(): Boolean = retryCount + 1 < maxRetries
}
