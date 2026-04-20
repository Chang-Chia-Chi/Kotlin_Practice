package com.workflow.workflow.model

enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED,
    TIMED_OUT, DEAD_LETTER, CANCELLED, DEFERRED,
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
            PROCESSING to DEFERRED,
            DEFERRED to COMPLETED,
            DEFERRED to FAILED,
            DEFERRED to TIMED_OUT,
            DEFERRED to CANCELLED,
            DEFERRED to PENDING,
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
