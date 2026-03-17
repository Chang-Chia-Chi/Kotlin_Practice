package com.mapreduce.mr.api.dto

import com.mapreduce.queue.model.TaskGroup

data class JobResponse(
    val jobId: String,
    val jobType: String,
    val status: String,
    val phase: String,
    val phaseTotal: Int,
    val tasksPending: Int,
    val tasksFailed: Int,
    val failurePolicy: String,
    val resultMetadata: String?,
    val deadlineAt: String?,
    val createdAt: String?,
    val updatedAt: String?,
) {
    companion object {
        fun from(group: TaskGroup) = JobResponse(
            jobId = group.groupId,
            jobType = group.groupType,
            status = group.status.name,
            phase = group.phase,
            phaseTotal = group.phaseTotal,
            tasksPending = group.tasksPending,
            tasksFailed = group.tasksFailed,
            failurePolicy = group.failurePolicy,
            resultMetadata = group.resultMetadata,
            deadlineAt = group.deadlineAt?.toString(),
            createdAt = group.createdAt?.toString(),
            updatedAt = group.updatedAt?.toString(),
        )
    }
}
