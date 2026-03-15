package com.mapreduce.fanout.spi

import com.mapreduce.mr.model.FailurePolicy

/**
 * The higher-level SPI for fan-out job types.
 *
 * A developer implements three business logic methods and serialization hooks.
 * The framework auto-registers one [com.mapreduce.queue.spi.TaskHandler] per
 * definition: `"{jobType}.execute"`. There is no reduce handler.
 *
 * **OnCompleted** runs inline on the leader (not as a task) after the barrier
 * is met. It should be fast and non-blocking. For heavy post-completion work,
 * enqueue a standalone task from within OnCompleted.
 *
 * @param P Job parameters type (input to split)
 * @param I Task input type (produced by split, consumed by execute)
 */
interface FanoutDefinition<P, I> {

    val jobType: String

    val failurePolicy: FailurePolicy get() = FailurePolicy.FAIL_JOB

    val failureThreshold: Double get() = 0.0

    val maxRetries: Int get() = 3

    /** Queue name for execute tasks. Default: "fanout". */
    val queue: String get() = "fanout"

    // --- Serialization hooks (framework is payload-agnostic) ---

    fun serializeParams(params: P): String
    fun deserializeParams(json: String): P
    fun serializeInput(input: I): String
    fun deserializeInput(json: String): I

    // --- Business logic ---

    /** Given job parameters, produce the list of task inputs. Runs on the leader. */
    fun split(params: P): List<I>

    /** Process a single task input. Runs on any worker. Self-contained — no intermediate outputs. */
    suspend fun execute(input: I)

    /**
     * Optional post-completion callback — runs inline on the leader after the barrier is met.
     *
     * Receives a summary of the completed job (total, completed, failed counts).
     * Must be fast and non-blocking. For heavy processing, enqueue a standalone task.
     *
     * Default implementation is a no-op.
     */
    fun onCompleted(summary: FanoutSummary) {}
}

/**
 * Summary passed to [FanoutDefinition.onCompleted] after barrier detection.
 */
data class FanoutSummary(
    val jobId: String,
    val jobType: String,
    val totalTasks: Int,
    val completedTasks: Int,
    val failedTasks: Int,
)

@Suppress("UNCHECKED_CAST")
fun FanoutDefinition<*, *>.unsafeCast(): FanoutDefinition<Any, Any> =
    this as FanoutDefinition<Any, Any>
