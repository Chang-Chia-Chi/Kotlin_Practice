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

    /**
     * Handler requests a retry with optional delay.
     *
     * @param consumeRetry When `true` (default), the retry counts against the
     *   task's retry budget. When `false`, the task is re-enqueued without
     *   incrementing `retry_count` (used by circuit breaker and shutdown-aware
     *   timeout to avoid penalizing system-level requeues).
     */
    data class Retry(
        val delay: Duration? = null,
        val reason: String = "Retry requested",
        val consumeRetry: Boolean = true,
    ) : TaskResult

    /** Handler declares permanent failure. */
    data class Failure(val message: String) : TaskResult

    /**
     * Immediately dead-letter the task, skipping remaining retries.
     *
     * Used by the error classifier for permanent errors (bad input,
     * deserialization failures) where retrying would never succeed.
     */
    data class DeadLetter(val reason: String) : TaskResult
}
