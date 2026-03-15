package com.mapreduce.queue.model

import java.time.Duration

/** Return type for [com.mapreduce.queue.spi.TaskHandler.handle]. */
sealed interface TaskResult {
    /**
     * Handler completed successfully. Framework marks task COMPLETED.
     *
     * @param output Optional output payload (serialized JSON). Used by the
     *   Chained Tasks pattern to pass step output to the next step.
     *   Handlers that are chain-unaware can omit it.
     */
    data class Success(val output: String? = null) : TaskResult

    /** Handler requests a retry with optional delay. */
    data class Retry(
        val delay: Duration? = null,
        val reason: String = "Retry requested",
    ) : TaskResult

    /** Handler declares permanent failure. */
    data class Failure(val message: String) : TaskResult
}
