package com.mapreduce.workflow.api.dto

import com.mapreduce.queue.model.WorkflowStep

data class JobResponse(
    val runId: String,
    val workflowName: String,
    val status: String,
    val stepLabel: String,
    val stepTotal: Int,
    val tasksPending: Int,
    val tasksFailed: Int,
    val failurePolicy: String,
    val resultMetadata: String?,
    val deadlineAt: String?,
    val createdAt: String?,
    val updatedAt: String?,
) {
    companion object {
        fun from(step: WorkflowStep, tasksPending: Int, tasksFailed: Int) = JobResponse(
            runId = step.runId,
            workflowName = step.workflowName,
            status = step.status.name,
            stepLabel = step.stepLabel,
            stepTotal = step.stepTotal,
            tasksPending = tasksPending,
            tasksFailed = tasksFailed,
            failurePolicy = step.failurePolicy,
            resultMetadata = step.resultMetadata,
            deadlineAt = step.deadlineAt?.toString(),
            createdAt = step.createdAt?.toString(),
            updatedAt = step.updatedAt?.toString(),
        )
    }
}
