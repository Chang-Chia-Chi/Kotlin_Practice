package com.mapreduce.mr.spi

import com.mapreduce.queue.model.FailurePolicy
import kotlinx.coroutines.flow.Flow

/**
 * The higher-level SPI for map-reduce job types.
 *
 * A developer implements four business logic methods and serialization hooks.
 * The framework auto-registers two [com.mapreduce.queue.spi.TaskHandler] instances
 * per definition: `"{jobType}.map"` and `"{jobType}.reduce"`.
 *
 * @param P Job parameters type (input to split)
 * @param I Task input type (produced by split, consumed by map)
 * @param O Intermediate output type (produced by map, consumed by reduce)
 * @param R Final result type (produced by reduce, consumed by onCompleted)
 */
interface MapReduceDefinition<P, I, O, R> {

    val jobType: String

    val failurePolicy: FailurePolicy get() = FailurePolicy.FAIL_GROUP

    val failureThreshold: Double get() = 0.0

    val maxRetries: Int get() = 3

    /** Queue name for map/reduce tasks. Default: "mr". */
    val queue: String get() = "mr"

    // --- Serialization hooks (framework is payload-agnostic) ---

    fun serializeParams(params: P): String
    fun deserializeParams(json: String): P
    fun serializeInput(input: I): String
    fun deserializeInput(json: String): I
    fun serializeOutput(output: O): String
    fun deserializeOutput(json: String): O
    fun serializeResult(result: R): String

    // --- Business logic ---

    /** Given job parameters, produce the list of task inputs. Runs on the leader. */
    fun split(params: P): List<I>

    /** Given a single task input, produce intermediate outputs as a stream. Runs on any worker. */
    fun map(input: I): Flow<O>

    /** Given all intermediate outputs as a stream, produce the final result. */
    suspend fun reduce(outputs: Flow<O>): R

    /** Post-reduce callback — publish, upload, or notify. */
    fun onCompleted(result: R)
}

@Suppress("UNCHECKED_CAST")
fun MapReduceDefinition<*, *, *, *>.unsafeCast(): MapReduceDefinition<Any, Any, Any, Any> =
    this as MapReduceDefinition<Any, Any, Any, Any>
