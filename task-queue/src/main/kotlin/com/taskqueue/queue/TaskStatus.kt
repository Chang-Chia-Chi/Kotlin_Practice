package com.taskqueue.queue

/**
 * Canonical task lifecycle states. Matches the STATUS column in TASK_QUEUE.
 *
 * State machine (valid transitions):
 *   PENDING     → PROCESSING | EXPIRED | SCHEDULED
 *   SCHEDULED   → PENDING (promoted when SCHEDULED_AT <= now)
 *   PROCESSING  → DONE | RETRYABLE | CANCELLED | DISCARDED | EXPIRED
 *   RETRYABLE   → PENDING (promoted when SCHEDULED_AT <= now)
 *   DONE        — terminal
 *   CANCELLED   — terminal (explicit cancel by handler via TaskResult.Cancel)
 *   DISCARDED   — terminal (retries exhausted)
 *   EXPIRED     — terminal
 */
enum class TaskStatus {
    PENDING,
    SCHEDULED,
    PROCESSING,
    RETRYABLE,
    DONE,
    CANCELLED,
    DISCARDED,
    EXPIRED,
    ;

    companion object {
        /** Terminal states — no further transitions allowed. */
        val TERMINAL: Set<TaskStatus> = setOf(DONE, CANCELLED, DISCARDED, EXPIRED)

        /** States eligible for claiming by consumers. */
        val CLAIMABLE: Set<TaskStatus> = setOf(PENDING)

        /** States eligible for promotion to PENDING by the scheduled-task promoter. */
        val PROMOTABLE: Set<TaskStatus> = setOf(RETRYABLE, SCHEDULED)
    }
}
