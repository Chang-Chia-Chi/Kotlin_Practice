package com.mapreduce.mr.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.model.Job
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.queue.model.EnqueueRequest
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.runBlocking
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.util.UUID

/**
 * Layer 2 persistence — map-reduce specific.
 *
 * Handles mr_job, mr_output, and the atomic fan-out (job + tasks in one txn).
 * Writes to the generic `task` table for fan-out and reduce dispatch,
 * keeping atomicity via the same JDBI transaction.
 */
@ApplicationScoped
class JobRepository(
    private val jdbi: Jdbi,
    private val objectMapper: ObjectMapper,
) {
    companion object {
        private const val OUTPUT_BATCH_SIZE = 1000
    }

    /**
     * Atomic fan-out: insert job row + N map tasks in one Oracle transaction.
     * Tasks go into the generic `task` table with `group_id = jobId` and
     * `handler = "{jobType}.map"`.
     */
    fun submitJob(
        jobId: String,
        jobType: String,
        jobParams: String,
        taskInputs: List<String>,
        maxRetries: Int,
        failurePolicy: FailurePolicy,
        failureThreshold: Double,
        queue: String,
    ) {
        jdbi.useTransaction<Exception> { h ->
            h
                .createUpdate(
                    """
                INSERT INTO mr_job (job_id, job_type, status, job_params, total_tasks,
                    completed_tasks, failed_tasks, failure_policy, failure_threshold,
                    version, created_at, updated_at)
                VALUES (:jobId, :jobType, 'RUNNING', :jobParams, :totalTasks,
                    0, 0, :failurePolicy, :failureThreshold,
                    0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                ).bind("jobId", jobId)
                .bind("jobType", jobType)
                .bind("jobParams", jobParams)
                .bind("totalTasks", taskInputs.size)
                .bind("failurePolicy", failurePolicy.name)
                .bind("failureThreshold", failureThreshold)
                .execute()

            val handler = "$jobType.map"
            val batch =
                h.prepareBatch(
                    """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', 0,
                    :groupId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
                )

            taskInputs.forEachIndexed { index, input ->
                batch
                    .bind("taskId", UUID.randomUUID().toString())
                    .bind("handler", handler)
                    .bind("queue", queue)
                    .bind("payload", input)
                    .bind("groupId", jobId)
                    .bind("metadata", objectMapper.writeValueAsString(mapOf("task_index" to index, "phase" to "MAP")))
                    .bind("maxRetries", maxRetries)
                    .add()
            }
            batch.execute()
        }
    }

    fun findJobById(jobId: String): Job? =
        jdbi.withHandle<Job?, Exception> { h ->
            h
                .createQuery("SELECT * FROM mr_job WHERE job_id = :jobId")
                .bind("jobId", jobId)
                .mapTo(Job::class.java)
                .findOne()
                .orElse(null)
        }

    fun findJobsByStatus(status: JobStatus): List<Job> =
        jdbi.withHandle<List<Job>, Exception> { h ->
            h
                .createQuery("SELECT * FROM mr_job WHERE status = :status")
                .bind("status", status.name)
                .mapTo(Job::class.java)
                .list()
        }

    fun findAllJobs(limit: Int = 100): List<Job> =
        jdbi.withHandle<List<Job>, Exception> { h ->
            h
                .createQuery("SELECT * FROM mr_job ORDER BY created_at DESC FETCH FIRST :limit ROWS ONLY")
                .bind("limit", limit)
                .mapTo(Job::class.java)
                .list()
        }

    /**
     * Compare-and-swap for job status transitions.
     * Returns true if the transition succeeded (exactly-once semantics).
     */
    fun casJobStatus(
        jobId: String,
        expectedStatus: JobStatus,
        newStatus: JobStatus,
        expectedVersion: Long,
        fenceToken: String?,
    ): Boolean {
        val updated =
            jdbi.withHandle<Int, Exception> { h ->
                h
                    .createUpdate(
                        """
                UPDATE mr_job
                SET status = :newStatus, reducing_fence_token = :fenceToken,
                    version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = :jobId AND status = :expectedStatus AND version = :expectedVersion
                """,
                    ).bind("jobId", jobId)
                    .bind("expectedStatus", expectedStatus.name)
                    .bind("newStatus", newStatus.name)
                    .bind("expectedVersion", expectedVersion)
                    .bind("fenceToken", fenceToken)
                    .execute()
            }
        return updated > 0
    }

    fun setResultMetadata(
        jobId: String,
        metadata: String,
    ) {
        jdbi.useHandle<Exception> { h ->
            h
                .createUpdate(
                    """
                UPDATE mr_job SET result_metadata = :metadata, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = :jobId
                """,
                ).bind("jobId", jobId)
                .bind("metadata", metadata)
                .execute()
        }
    }

    /**
     * Atomically: persist map outputs in chunks, mark task COMPLETED, increment completed_tasks.
     * All in one transaction for correctness (design doc S5.5 Phase 2, step 4).
     *
     * Outputs are collected from the [Flow] in bounded chunks to avoid OOM
     * when a single map task produces millions of intermediate records.
     * The task status UPDATE is guarded with `AND status = 'CLAIMED'` to prevent
     * double-incrementing `completed_tasks` on stale reclaim + re-execution.
     */
    fun completeMapTask(
        taskId: String,
        jobId: String,
        outputs: Flow<String>,
    ) {
        jdbi.useTransaction<Exception> { h ->
            val buffer = mutableListOf<String>()

            runBlocking {
                outputs.collect { output ->
                    buffer.add(output)
                    if (buffer.size >= OUTPUT_BATCH_SIZE) {
                        insertOutputBatch(h, jobId, taskId, buffer)
                        buffer.clear()
                    }
                }
            }

            if (buffer.isNotEmpty()) {
                insertOutputBatch(h, jobId, taskId, buffer)
            }

            val updated =
                h
                    .createUpdate(
                        "UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP WHERE task_id = :taskId AND status = 'CLAIMED'",
                    ).bind("taskId", taskId)
                    .execute()

            if (updated > 0) {
                h
                    .createUpdate(
                        "UPDATE mr_job SET completed_tasks = completed_tasks + 1, updated_at = CURRENT_TIMESTAMP WHERE job_id = :jobId",
                    ).bind("jobId", jobId)
                    .execute()
            }
        }
    }

    private fun insertOutputBatch(
        h: Handle,
        jobId: String,
        taskId: String,
        outputs: List<String>,
    ) {
        val batch =
            h.prepareBatch(
                """
            INSERT INTO mr_output (output_id, job_id, task_id, output_data, created_at)
            VALUES (:outputId, :jobId, :taskId, :outputData, CURRENT_TIMESTAMP)
            """,
            )
        outputs.forEach { output ->
            batch
                .bind("outputId", UUID.randomUUID().toString())
                .bind("jobId", jobId)
                .bind("taskId", taskId)
                .bind("outputData", output)
                .add()
        }
        batch.execute()
    }

    /** Mark reduce task COMPLETED and store result metadata on the job. */
    fun completeReduceTask(
        taskId: String,
        jobId: String,
        resultMetadata: String,
    ) {
        jdbi.useTransaction<Exception> { h ->
            h
                .createUpdate(
                    "UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP WHERE task_id = :taskId",
                ).bind("taskId", taskId)
                .execute()

            h
                .createUpdate(
                    """
                UPDATE mr_job SET result_metadata = :metadata, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = :jobId
                """,
                ).bind("metadata", resultMetadata)
                .bind("jobId", jobId)
                .execute()
        }
    }

    /** Enqueue a single reduce task into the generic task table. */
    fun insertReduceTask(
        jobId: String,
        jobType: String,
        maxRetries: Int,
        queue: String,
    ) {
        jdbi.useHandle<Exception> { h ->
            h
                .createUpdate(
                    """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, '{}', 'PENDING', 0,
                    :groupId, '{"phase":"REDUCE"}', 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
                ).bind("taskId", UUID.randomUUID().toString())
                .bind("handler", "$jobType.reduce")
                .bind("queue", queue)
                .bind("groupId", jobId)
                .bind("maxRetries", maxRetries)
                .execute()
        }
    }

    /** Update the failed_tasks counter (called by orchestrator at barrier detection). */
    fun updateFailedTasks(
        jobId: String,
        failedCount: Int,
    ) {
        jdbi.useHandle<Exception> { h ->
            h
                .createUpdate(
                    "UPDATE mr_job SET failed_tasks = :count, updated_at = CURRENT_TIMESTAMP WHERE job_id = :jobId",
                ).bind("jobId", jobId)
                .bind("count", failedCount)
                .execute()
        }
    }

    /**
     * Returns a [Flow] of output data for a job, backed by a DB cursor.
     * The JDBI handle stays open while the flow is collected.
     */
    fun streamOutputs(jobId: String): Flow<String> =
        flow {
            val handle = jdbi.open()
            try {
                val stream =
                    handle
                        .createQuery(
                            "SELECT output_data FROM mr_output WHERE job_id = :jobId ORDER BY output_id",
                        ).bind("jobId", jobId)
                        .mapTo(String::class.java)
                        .stream()

                stream.use { s ->
                    val iter = s.iterator()
                    while (iter.hasNext()) {
                        emit(iter.next())
                    }
                }
            } finally {
                handle.close()
            }
        }.flowOn(Dispatchers.IO)
}
