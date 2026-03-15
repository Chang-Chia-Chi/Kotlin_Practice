package com.mapreduce.fanout.model

import com.mapreduce.mr.model.FailurePolicy
import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class FanoutJobStatus {
    CREATED, RUNNING, COMPLETED, FAILED
}

/**
 * A single fan-out execution cycle. Tracks overall lifecycle,
 * task counters, and failure policy.
 *
 * Unlike [com.mapreduce.mr.model.Job], there is no REDUCING state,
 * no reduce-related columns (reducingFenceToken, totalPartitions),
 * and no intermediate output storage. The barrier transitions
 * directly from RUNNING to COMPLETED.
 */
data class FanoutJob(
    @ColumnName("job_id") val jobId: String,
    @ColumnName("job_type") val jobType: String,
    val status: FanoutJobStatus,
    @ColumnName("job_params") val jobParams: String,
    @ColumnName("total_tasks") val totalTasks: Int,
    @ColumnName("completed_tasks") val completedTasks: Int = 0,
    @ColumnName("failed_tasks") val failedTasks: Int = 0,
    @ColumnName("failure_policy") val failurePolicy: FailurePolicy = FailurePolicy.FAIL_JOB,
    @ColumnName("failure_threshold") val failureThreshold: Double = 0.0,
    @ColumnName("result_summary") val resultSummary: String? = null,
    @ColumnName("last_epoch") val lastEpoch: Long = 0,
    val version: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
