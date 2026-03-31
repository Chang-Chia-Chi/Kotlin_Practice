package com.workflow.workflow.model

enum class WorkflowStatus {
    RUNNING, COMPLETED, FAILED, TIMED_OUT, CANCELLED;

    val isTerminal: Boolean get() = this != RUNNING

    companion object {
        private val allowed = setOf(
            RUNNING to COMPLETED,
            RUNNING to FAILED,
            RUNNING to TIMED_OUT,
            RUNNING to CANCELLED,
            FAILED to RUNNING,
            TIMED_OUT to RUNNING,
            CANCELLED to RUNNING,
        )

        fun requireTransition(from: WorkflowStatus, to: WorkflowStatus) {
            require((from to to) in allowed) {
                "Illegal workflow transition: $from \u2192 $to"
            }
        }
    }
}
