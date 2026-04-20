package com.workflow.worker.usecase.port.inbound.trigger

import java.time.Instant

data class DeferredTaskRef(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val triggerType: String,
    val triggerMeta: String,
    val deadlineAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
)
