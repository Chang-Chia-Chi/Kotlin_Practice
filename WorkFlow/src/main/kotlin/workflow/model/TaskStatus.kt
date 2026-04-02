package com.workflow.workflow.model

enum class TaskStatus {
    PENDING, PROCESSING, WAITING_FOR_SIGNAL, COMPLETED, FAILED,
    TIMED_OUT, DEAD_LETTER, CANCELLED,
    SKIPPED;   // terminal: inserted by phase gate when a conditional edge is not taken

    val isTerminal: Boolean get() = this in terminalStatuses

    companion object {
        private val terminalStatuses = setOf(COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED, SKIPPED)
        private val allowed = setOf(
            PENDING to PROCESSING,
            PENDING to CANCELLED,
            PROCESSING to COMPLETED,
            PROCESSING to FAILED,
            PROCESSING to TIMED_OUT,
            PROCESSING to PENDING,
            PROCESSING to DEAD_LETTER,
            PROCESSING to WAITING_FOR_SIGNAL,
            WAITING_FOR_SIGNAL to COMPLETED,
            WAITING_FOR_SIGNAL to FAILED,
            WAITING_FOR_SIGNAL to TIMED_OUT,
            WAITING_FOR_SIGNAL to CANCELLED,
            FAILED to PENDING,
            FAILED to DEAD_LETTER,
        )

        fun requireTransition(from: TaskStatus, to: TaskStatus) {
            require((from to to) in allowed) {
                "Illegal task transition: $from \u2192 $to"
            }
        }
    }
}
