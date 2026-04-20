package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult

/**
 * A single layer in the task execution pipeline.
 *
 * Middlewares wrap handler execution with cross-cutting concerns
 * (metrics, tracing, timeout, error classification). They are
 * discovered via CDI, sorted by [order], and composed into an
 * onion-layer chain using `foldRight`.
 *
 * Lower order = outermost layer (runs first, finishes last).
 */
interface Middleware {
    val order: Int
    suspend fun invoke(
        context: TaskContext,
        next: suspend (TaskContext) -> TaskResult,
    ): TaskResult
}
