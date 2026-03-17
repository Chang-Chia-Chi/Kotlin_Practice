package com.mapreduce.mr.api.dto

import com.mapreduce.queue.model.TaskGroup

data class JobResponse(
    val jobId: String,
    val jobType: String,
    val status: String,
    val phase: String,
    val phaseTotal: Int,
    val phaseCompleted: Int,
    val phaseFailed: Int,
    val failurePolicy: String,
    val resultMetadata: String?,
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
            phaseCompleted = group.phaseCompleted,
            phaseFailed = group.phaseFailed,
            failurePolicy = group.failurePolicy,
            resultMetadata = group.resultMetadata,
            createdAt = group.createdAt?.toString(),
            updatedAt = group.updatedAt?.toString(),
        )
    }
}
