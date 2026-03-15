package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskResult

/**
 * A composable middleware that wraps handler execution.
 *
 * Middlewares are ordered by [order] (lower = outermost). The handler
 * itself is the innermost element and does not implement this interface.
 *
 * The chain is built once per handler at startup and reused for every
 * invocation. No per-invocation allocation.
 *
 * Semantic ordering boundaries:
 * - 10–20: observation (metrics, tracing)
 * - 20–40: resilience (circuit breaker, timeout)
 * - 40–50: error handling (error classification)
 */
interface HandlerMiddleware {

    /** Ordering priority. Lower numbers execute first (outermost layer). */
    val order: Int

    /**
     * Wrap the next stage in the chain.
     *
     * Call [next] to proceed down the chain, or return a [TaskResult]
     * directly to short-circuit execution.
     */
    suspend fun invoke(
        context: TaskExecutionContext,
        next: suspend (TaskExecutionContext) -> TaskResult,
    ): TaskResult
}
