package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import java.time.Instant

/**
 * Immutable snapshot of the task being executed, passed through the
 * entire middleware chain.
 *
 * Constructed by the worker loop after claiming a task. Middlewares
 * cannot modify it — they use it to make decisions (e.g., which
 * circuit breaker to apply, what timeout to use).
 */
data class TaskExecutionContext(
    val taskId: String,
    val handler: String,
    val queue: String,
    val groupId: String?,
    val payload: String,
    val metadata: String?,
    val retryCount: Int,
    val maxRetries: Int,
    val claimedAt: Instant?,
    val executionGeneration: String?,
    /** Runtime context carried into the handler (e.g. isShuttingDown). */
    val taskContext: TaskContext,
)
