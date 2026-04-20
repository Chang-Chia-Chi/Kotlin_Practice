package com.workflow.workflow.usecase.port.inbound.orchestration

import com.workflow.workflow.model.TaskCompletionEvent

interface PhaseGate {
    suspend fun onTaskCompleted(event: TaskCompletionEvent)

    suspend fun recoverStuckWorkflow(workflowId: String)
}
