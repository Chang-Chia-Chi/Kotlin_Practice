package com.mapreduce.queue.model

import java.time.Duration

/** Return type for [com.mapreduce.queue.spi.TaskHandler.handle]. */
sealed interface TaskResult {
    /** Handler completed successfully. Framework marks task COMPLETED. */
    data object Success : TaskResult

    /** Handler requests a retry with optional delay. */
    data class Retry(
        val delay: Duration? = null,
        val reason: String = "Retry requested",
    ) : TaskResult

    /** Handler declares permanent failure. */
    data class Failure(val message: String) : TaskResult
}
