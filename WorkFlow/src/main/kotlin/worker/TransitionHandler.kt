package com.workflow.worker

/**
 * Handler for task execution within the workflow engine.
 *
 * ## Delivery Guarantee
 *
 * The engine provides **at-least-once** delivery. A handler may be invoked
 * multiple times for the same logical task due to:
 * - Retry on failure (up to `maxRetries` per task)
 * - Stale task reclaim by the sweeper (visibility timeout expiry)
 *
 * ## Idempotency Requirement
 *
 * Handlers **must be idempotent**. Use [HandlerInput.taskId] as the
 * idempotency key when interacting with external systems. For example,
 * pass `taskId` as the idempotency key to payment APIs, message
 * deduplication headers, or database upsert conditions.
 *
 * ## Cancellation
 *
 * Long-running handlers should periodically check `isActive` or call
 * `yield()` to cooperate with graceful shutdown. On pod termination,
 * in-flight handlers receive [kotlinx.coroutines.CancellationException]
 * after the drain window expires. Tasks whose handlers are cancelled
 * remain in PROCESSING state and will be reclaimed by the sweeper.
 *
 * ## Shutdown Awareness
 *
 * Handlers can call [com.workflow.shutdown.isShuttingDown] from their
 * coroutine context to detect that the pod is draining. Use this to
 * skip optional work or checkpoint progress.
 */
interface TransitionHandler {
    suspend fun execute(input: HandlerInput): HandlerOutput
}

/**
 * Input provided to a [TransitionHandler] for task execution.
 *
 * @property taskId Unique task identifier — use as idempotency key for external calls.
 * @property workflowId Parent workflow identifier.
 * @property sequenceNumber Position in the workflow DAG.
 * @property inputs Resolved input map from declared activity inputs. Null if no inputs declared.
 * @property item Scatter chunk for parallel tasks. Null for non-parallel tasks.
 */
data class HandlerInput(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val inputs: String?,
    val item: String?,
)

/**
 * Output returned by a [TransitionHandler] after task execution.
 *
 * @property result JSON output passed to the next step or stored as the final workflow result.
 *                  Return `null` if the handler produces no output.
 */
data class HandlerOutput(
    val result: String?,
)
