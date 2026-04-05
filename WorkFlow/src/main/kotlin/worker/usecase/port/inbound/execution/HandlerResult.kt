package com.workflow.worker.usecase.port.inbound.execution

sealed interface HandlerResult {
    data class Completed(val result: String?) : HandlerResult
    data class Defer(val triggerType: String, val triggerMeta: String) : HandlerResult
}
