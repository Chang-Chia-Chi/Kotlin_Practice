package com.mapreduce.queue.spi

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult

/**
 * The only thing a developer implements for Layer 1.
 *
 * A handler is a named function that receives a payload and returns a result.
 * The framework discovers all handlers at startup via CDI and programmatic
 * registration, building a `handlerName -> implementation` registry.
 *
 * If a task arrives with an unrecognized handler, it is immediately dead-lettered.
 */
interface TaskHandler {
    /** Routing key — e.g. `"email.send"`, `"dispatch.map"`. */
    val handlerName: String

    /** Execute the work. Return success, failure, or retry-with-delay. */
    suspend fun handle(ctx: TaskContext): TaskResult
}
