package com.workflow.workflow.model

sealed interface StartResult {
    data class Created(val workflowId: String) : StartResult
    data class AlreadyExists(val workflowId: String) : StartResult
}

val StartResult.workflowId: String
    get() = when (this) {
        is StartResult.Created -> workflowId
        is StartResult.AlreadyExists -> workflowId
    }
