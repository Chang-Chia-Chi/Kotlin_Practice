package com.mapreduce.mr.api.dto

import com.mapreduce.mr.model.Job

data class JobResponse(
    val jobId: String,
    val jobType: String,
    val status: String,
    val totalTasks: Int,
    val completedTasks: Int,
    val failedTasks: Int,
    val failurePolicy: String,
    val totalPartitions: Int,
    val resultMetadata: String?,
    val createdAt: String?,
    val updatedAt: String?,
) {
    companion object {
        fun from(job: Job) = JobResponse(
            jobId = job.jobId,
            jobType = job.jobType,
            status = job.status.name,
            totalTasks = job.totalTasks,
            completedTasks = job.completedTasks,
            failedTasks = job.failedTasks,
            failurePolicy = job.failurePolicy.name,
            totalPartitions = job.totalPartitions,
            resultMetadata = job.resultMetadata,
            createdAt = job.createdAt?.toString(),
            updatedAt = job.updatedAt?.toString(),
        )
    }
}
