package com.workflow.infrastructure.shutdown

enum class ShutdownState {
    RUNNING,
    DRAINING,
    TERMINATED,
}
