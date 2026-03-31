package com.workflow.workflow.usecase.port.inbound.orchestration

import com.workflow.workflow.model.StartResult
import com.workflow.workflow.model.WorkflowDefinition

interface WorkflowLifecycle {
    suspend fun startWorkflow(
        definition: WorkflowDefinition,
        idempotencyKey: String? = null,
    ): StartResult

    suspend fun cancelWorkflow(workflowId: String): Boolean
    suspend fun replayWorkflow(workflowId: String): Boolean
}
