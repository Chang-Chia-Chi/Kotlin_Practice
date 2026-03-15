package com.mapreduce.fanout.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.fanout.model.FanoutJob
import com.mapreduce.fanout.model.FanoutJobStatus
import com.mapreduce.leader.FencedRepository
import com.mapreduce.mr.model.FailurePolicy
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.util.UUID

/**
 * Layer 2 persistence — fan-out specific.
 *
 * Extends [FencedRepository] for leader-only writes that participate in
 * the fenced leader election pattern. The fencing epoch is read from
 * [com.mapreduce.leader.FencingTokenHolder] and applied as a SQL WHERE
 * guard on all leader writes.
 */
@ApplicationScoped
class FanoutJobRepository(
    jdbi: Jdbi,
    private val objectMapper: ObjectMapper,
) : FencedRepository(jdbi) {

    /**
     * Atomic fan-out: insert job row + N execute tasks in one Oracle transaction.
     * Tasks go into the generic `task` table with `group_id = jobId` and
     * `handler = "{jobType}.execute"`.
     *
     * Not a leader-only write — called from the REST endpoint.
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
                INSERT INTO fanout_job (job_id, job_type, status, job_params, total_tasks,
                    completed_tasks, failed_tasks, failure_policy, failure_threshold,
                    last_epoch, version, created_at, updated_at)
                VALUES (:jobId, :jobType, 'RUNNING', :jobParams, :totalTasks,
                    0, 0, :failurePolicy, :failureThreshold,
                    0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                ).bind("jobId", jobId)
                .bind("jobType", jobType)
                .bind("jobParams", jobParams)
                .bind("totalTasks", taskInputs.size)
                .bind("failurePolicy", failurePolicy.name)
                .bind("failureThreshold", failureThreshold)
                .execute()

            val handler = "$jobType.execute"
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
                    .bind("metadata", objectMapper.writeValueAsString(mapOf("task_index" to index, "phase" to "EXECUTE")))
                    .bind("maxRetries", maxRetries)
                    .add()
            }
            batch.execute()
        }
    }

    fun findJobById(jobId: String): FanoutJob? =
        jdbi.withHandle<FanoutJob?, Exception> { h ->
            h
                .createQuery("SELECT * FROM fanout_job WHERE job_id = :jobId")
                .bind("jobId", jobId)
                .mapTo(FanoutJob::class.java)
                .findOne()
                .orElse(null)
        }

    fun findJobsByStatus(status: FanoutJobStatus): List<FanoutJob> =
        jdbi.withHandle<List<FanoutJob>, Exception> { h ->
            h
                .createQuery("SELECT * FROM fanout_job WHERE status = :status")
                .bind("status", status.name)
                .mapTo(FanoutJob::class.java)
                .list()
        }

    fun findAllJobs(limit: Int = 100): List<FanoutJob> =
        jdbi.withHandle<List<FanoutJob>, Exception> { h ->
            h
                .createQuery("SELECT * FROM fanout_job ORDER BY created_at DESC FETCH FIRST :limit ROWS ONLY")
                .bind("limit", limit)
                .mapTo(FanoutJob::class.java)
                .list()
        }

    /**
     * Compare-and-swap for job status transitions (leader-only).
     *
     * Combines version-based CAS with epoch fencing:
     * - Version ensures exactly-once semantics between competing leaders.
     * - Epoch ensures a zombie leader's stale writes are rejected.
     *
     * Returns true if the transition succeeded.
     */
    fun casJobStatus(
        jobId: String,
        expectedStatus: FanoutJobStatus,
        newStatus: FanoutJobStatus,
        expectedVersion: Long,
    ): Boolean {
        val epoch = optionalEpoch()
        val updated =
            jdbi.withHandle<Int, Exception> { h ->
                if (epoch != null) {
                    h.createUpdate(
                        """
                        UPDATE fanout_job
                        SET status = :newStatus, last_epoch = :epoch,
                            version = version + 1, updated_at = CURRENT_TIMESTAMP
                        WHERE job_id = :jobId AND status = :expectedStatus
                          AND version = :expectedVersion AND last_epoch <= :epoch
                        """,
                    ).bind("jobId", jobId)
                        .bind("expectedStatus", expectedStatus.name)
                        .bind("newStatus", newStatus.name)
                        .bind("expectedVersion", expectedVersion)
                        .bind("epoch", epoch)
                        .execute()
                } else {
                    h.createUpdate(
                        """
                        UPDATE fanout_job
                        SET status = :newStatus,
                            version = version + 1, updated_at = CURRENT_TIMESTAMP
                        WHERE job_id = :jobId AND status = :expectedStatus
                          AND version = :expectedVersion
                        """,
                    ).bind("jobId", jobId)
                        .bind("expectedStatus", expectedStatus.name)
                        .bind("newStatus", newStatus.name)
                        .bind("expectedVersion", expectedVersion)
                        .execute()
                }
            }
        return updated > 0
    }

    fun setResultSummary(
        jobId: String,
        summary: String,
    ) {
        jdbi.useHandle<Exception> { h ->
            h
                .createUpdate(
                    """
                UPDATE fanout_job SET result_summary = :summary, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = :jobId
                """,
                ).bind("jobId", jobId)
                .bind("summary", summary)
                .execute()
        }
    }

    /**
     * Atomically: mark task COMPLETED + increment completed_tasks.
     * All in one transaction for correctness.
     *
     * No intermediate outputs — fan-out tasks are self-contained.
     *
     * The task status UPDATE is guarded with `AND status = 'CLAIMED'` to prevent
     * double-incrementing `completed_tasks` on stale reclaim + re-execution.
     */
    fun completeFanoutTask(
        taskId: String,
        jobId: String,
        executionGeneration: String? = null,
    ) {
        jdbi.useTransaction<Exception> { h ->
            val fenceClause = if (executionGeneration != null) " AND execution_generation = :gen" else ""
            val update = h
                .createUpdate(
                    "UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause",
                ).bind("taskId", taskId)
            if (executionGeneration != null) update.bind("gen", executionGeneration)
            val updated = update.execute()

            if (updated > 0) {
                h
                    .createUpdate(
                        "UPDATE fanout_job SET completed_tasks = completed_tasks + 1, updated_at = CURRENT_TIMESTAMP WHERE job_id = :jobId",
                    ).bind("jobId", jobId)
                    .execute()
            }
        }
    }

    /**
     * Update the failed_tasks counter (leader-only, fenced).
     */
    fun updateFailedTasks(
        jobId: String,
        failedCount: Int,
    ) {
        val epoch = optionalEpoch()
        jdbi.useHandle<Exception> { h ->
            if (epoch != null) {
                h.createUpdate(
                    """
                    UPDATE fanout_job SET failed_tasks = :count, last_epoch = :epoch,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = :jobId AND last_epoch <= :epoch
                    """,
                ).bind("jobId", jobId)
                    .bind("count", failedCount)
                    .bind("epoch", epoch)
                    .execute()
            } else {
                h.createUpdate(
                    "UPDATE fanout_job SET failed_tasks = :count, updated_at = CURRENT_TIMESTAMP WHERE job_id = :jobId",
                ).bind("jobId", jobId)
                    .bind("count", failedCount)
                    .execute()
            }
        }
    }
}
