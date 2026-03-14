package com.mapreduce.mr.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class JobStatus {
    CREATED, RUNNING, REDUCING, COMPLETED, FAILED
}

enum class FailurePolicy {
    FAIL_JOB, THRESHOLD, BEST_EFFORT
}

/**
 * A single map-reduce execution cycle. Tracks overall lifecycle,
 * task counters, failure policy, and output path.
 */
data class Job(
    @ColumnName("job_id") val jobId: String,
    @ColumnName("job_type") val jobType: String,
    val status: JobStatus,
    @ColumnName("job_params") val jobParams: String,
    @ColumnName("total_tasks") val totalTasks: Int,
    @ColumnName("completed_tasks") val completedTasks: Int = 0,
    @ColumnName("failed_tasks") val failedTasks: Int = 0,
    @ColumnName("failure_policy") val failurePolicy: FailurePolicy = FailurePolicy.FAIL_JOB,
    @ColumnName("failure_threshold") val failureThreshold: Double = 0.0,
    @ColumnName("reducing_fence_token") val reducingFenceToken: String? = null,
    @ColumnName("result_metadata") val resultMetadata: String? = null,
    val version: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
