package com.workflow.workflow.model

import java.time.Instant

data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val version: Int,
    val status: WorkflowStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val deadlineAt: Instant,
)
