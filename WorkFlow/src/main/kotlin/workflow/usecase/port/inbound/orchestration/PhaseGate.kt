package com.workflow.workflow.usecase.port.inbound.orchestration

import com.workflow.workflow.model.TaskStatus
import java.time.Instant

interface PhaseGate {
    suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        itemsJson: String? = null,
    )

    suspend fun recoverStuckWorkflow(workflowId: String)
}
