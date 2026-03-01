package com.taskqueue.queue

import java.time.Duration

/**
 * Return type for [TaskHandler.handle]. Determines the post-handler state transition.
 *
 * Inspired by riverqueue's job completion signals:
 * - [Success] → DONE (children inserted)
 * - [Snooze]  → SCHEDULED (re-execute after [Snooze.duration])
 * - [Cancel]  → CANCELLED (explicit cancel with reason, no retry)
 *
 * Unhandled exceptions still trigger the retry/discard path — handlers do NOT need
 * to catch exceptions and wrap them in a result type.
 */
sealed class TaskResult {

    /** Handler completed successfully. Children are inserted, task marked DONE. */
    data object Success : TaskResult()

    /** Handler wants to defer re-execution. Task moves to SCHEDULED with a future SCHEDULED_AT. */
    data class Snooze(val duration: Duration) : TaskResult()

    /** Handler explicitly cancels the task. No retry, moves to CANCELLED. */
    data class Cancel(val reason: String) : TaskResult()
}
