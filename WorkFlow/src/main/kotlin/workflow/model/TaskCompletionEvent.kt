package com.workflow.workflow.model

import java.time.Instant

data class TaskCompletionEvent(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val status: TaskStatus,
    val resultJson: String?,
    val claimedBy: String? = null,
    val claimedAt: Instant? = null,
    val itemsJson: String? = null,
)
