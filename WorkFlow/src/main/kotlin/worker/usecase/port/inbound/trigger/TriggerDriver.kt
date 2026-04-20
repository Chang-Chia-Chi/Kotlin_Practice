package com.workflow.worker.usecase.port.inbound.trigger

/**
 * SPI for monitoring external task completion.
 *
 * ## Lifecycle contract
 * - [start] is called each sweep cycle with the **full** set of DEFERRED tasks
 *   for this driver's [type]. The driver diffs internally -- add new tasks,
 *   remove already-resolved ones.
 * - [poll] returns results since last call. Must be non-blocking.
 * - [cancel] is best-effort cleanup (e.g., delete K8s Job, cancel SQL query).
 * - [close] is called on shutdown for resource cleanup.
 */
interface TriggerDriver {
    fun type(): String
    suspend fun start(tasks: List<DeferredTaskRef>)
    suspend fun poll(): List<TriggerResult>
    suspend fun cancel(taskId: String)
    suspend fun close()
}
