package com.workflow.worker.usecase.port.inbound.execution

sealed interface HandlerResult {
    data class Completed(
        val result: String?,
        val items: String? = null,
    ) : HandlerResult
}
