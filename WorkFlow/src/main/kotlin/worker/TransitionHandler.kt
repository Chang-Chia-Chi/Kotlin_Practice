package com.workflow.worker

interface TransitionHandler {
    suspend fun execute(input: HandlerInput): HandlerOutput
}

data class HandlerInput(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val payload: String?,
)

data class HandlerOutput(
    val result: String?,
)
