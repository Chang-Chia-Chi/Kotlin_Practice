package com.mapreduce.fanout.api.dto

import com.mapreduce.fanout.model.FanoutJob

data class FanoutJobResponse(
    val jobId: String,
    val jobType: String,
    val status: String,
    val totalTasks: Int,
    val completedTasks: Int,
    val failedTasks: Int,
    val failurePolicy: String,
    val resultSummary: String?,
    val createdAt: String?,
    val updatedAt: String?,
) {
    companion object {
        fun from(job: FanoutJob) = FanoutJobResponse(
            jobId = job.jobId,
            jobType = job.jobType,
            status = job.status.name,
            totalTasks = job.totalTasks,
            completedTasks = job.completedTasks,
            failedTasks = job.failedTasks,
            failurePolicy = job.failurePolicy.name,
            resultSummary = job.resultSummary,
            createdAt = job.createdAt?.toString(),
            updatedAt = job.updatedAt?.toString(),
        )
    }
}
