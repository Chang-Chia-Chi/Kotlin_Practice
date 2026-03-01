package com.taskqueue.queue

import java.time.Instant

/**
 * Accumulates child task definitions during a single [TaskHandler.handle] invocation.
 *
 * The consumer reads [drain] after the handler returns and bulk-inserts the children.
 * This is the *only* sanctioned way to spawn follow-up tasks — handlers must never
 * INSERT into TASK_QUEUE directly.
 *
 * Thread-safety: not required. Each handler invocation receives its own instance —
 * even with concurrent processing, there is no shared mutable state.
 */
class TaskEmitter(private val parentTaskId: Long) {

    data class PendingTask(
        val taskType: String,
        val payload: String?,
        val priority: Int,
        val deadlineAt: Instant?,
        val uniqueKey: String? = null,
    )

    private val pending = mutableListOf<PendingTask>()

    /** Enqueue a single child task. */
    fun emit(
        taskType: String,
        payload: String? = null,
        priority: Int = 5,
        deadlineAt: Instant? = null,
    ) {
        pending += PendingTask(taskType, payload, priority, deadlineAt)
    }

    /** Convenience for fan-out: one child per payload, all sharing the same type/priority/deadline. */
    fun emitAll(
        taskType: String,
        payloads: List<String?>,
        priority: Int = 5,
        deadlineAt: Instant? = null,
    ) {
        payloads.mapTo(pending) { PendingTask(taskType, it, priority, deadlineAt) }
    }

    /** Returns collected tasks and clears the internal buffer. Called once by the consumer. */
    internal fun drain(): List<PendingTask> {
        val snapshot = pending.toList()
        pending.clear()
        return snapshot
    }

    /** Number of tasks accumulated so far. Useful for logging/metrics. */
    val size: Int get() = pending.size
}
