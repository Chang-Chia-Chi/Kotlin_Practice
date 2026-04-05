package com.workflow.worker.usecase.port.inbound.trigger

sealed interface TriggerResult {
    val taskId: String
    data class Succeeded(override val taskId: String, val result: String?) : TriggerResult
    data class Failed(override val taskId: String, val reason: String) : TriggerResult
}
