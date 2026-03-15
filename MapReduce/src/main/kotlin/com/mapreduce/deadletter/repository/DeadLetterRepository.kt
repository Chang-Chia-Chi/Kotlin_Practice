package com.mapreduce.deadletter.repository

import com.mapreduce.mr.model.JobStatus
import com.mapreduce.queue.model.Task
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.time.Instant

/**
 * Persistence layer for dead-letter inspection and replay operations.
 *
 * Queries the generic `task` table filtered by `status = 'DEAD_LETTER'`.
 * Replay operations atomically transition tasks back to PENDING and
 * adjust MR job counters when applicable.
 */
@ApplicationScoped
class DeadLetterRepository(private val jdbi: Jdbi) {

    /**
     * Paginated listing of dead-lettered tasks with optional filters.
     * Payload is excluded for performance (see §3.1).
     */
    fun findDeadLetters(
        handler: String? = null,
        groupId: String? = null,
        since: Instant? = null,
        until: Instant? = null,
        errorPattern: String? = null,
        limit: Int = 50,
        offset: Int = 0,
    ): List<Task> = jdbi.withHandle<List<Task>, Exception> { h ->
        val conditions = mutableListOf("status = 'DEAD_LETTER'")
        val binds = mutableMapOf<String, Any>()

        handler?.let { conditions.add("handler = :handler"); binds["handler"] = it }
        groupId?.let { conditions.add("group_id = :groupId"); binds["groupId"] = it }
        since?.let { conditions.add("created_at >= :since"); binds["since"] = it }
        until?.let { conditions.add("created_at < :until"); binds["until"] = it }
        errorPattern?.let { conditions.add("error_message LIKE :errorPattern"); binds["errorPattern"] = it }

        val where = conditions.joinToString(" AND ")
        val query = h.createQuery(
            """
            SELECT task_id, handler, queue, status, priority, group_id, metadata,
                   claimed_by, claimed_at, scheduled_at, retry_count, max_retries,
                   error_message, created_at, completed_at, execution_generation, speculative
            FROM task
            WHERE $where
            ORDER BY created_at DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
            """,
        )
        for ((k, v) in binds) query.bind(k, v)
        query.bind("limit", limit).bind("offset", offset)

        // Map without payload for list responses
        query.map { rs, _ ->
            Task(
                taskId = rs.getString("task_id"),
                handler = rs.getString("handler"),
                queue = rs.getString("queue"),
                payload = "", // excluded from list for performance
                status = com.mapreduce.queue.model.TaskStatus.DEAD_LETTER,
                priority = rs.getInt("priority"),
                groupId = rs.getString("group_id"),
                metadata = rs.getString("metadata"),
                claimedBy = rs.getString("claimed_by"),
                claimedAt = rs.getTimestamp("claimed_at")?.toInstant(),
                scheduledAt = rs.getTimestamp("scheduled_at")?.toInstant(),
                retryCount = rs.getInt("retry_count"),
                maxRetries = rs.getInt("max_retries"),
                errorMessage = rs.getString("error_message"),
                createdAt = rs.getTimestamp("created_at")?.toInstant(),
                completedAt = rs.getTimestamp("completed_at")?.toInstant(),
                executionGeneration = rs.getString("execution_generation"),
                speculative = rs.getInt("speculative"),
            )
        }.list()
    }

    /** Full task row including payload — the debugging endpoint (§3.2). */
    fun findDeadLetterById(taskId: String): Task? = jdbi.withHandle<Task?, Exception> { h ->
        h.createQuery("SELECT * FROM task WHERE task_id = :taskId AND status = 'DEAD_LETTER'")
            .bind("taskId", taskId)
            .mapTo(Task::class.java)
            .findOne().orElse(null)
    }

    /** Count dead-lettered tasks grouped by handler (§3.3). */
    fun summaryByHandler(since: Instant? = null): List<HandlerSummary> =
        jdbi.withHandle<List<HandlerSummary>, Exception> { h ->
            val sinceClause = if (since != null) " AND created_at >= :since" else ""
            val query = h.createQuery(
                """
                SELECT handler, COUNT(*) AS cnt,
                       MAX(error_message) AS latest_error,
                       MIN(created_at) AS earliest,
                       MAX(created_at) AS latest
                FROM task
                WHERE status = 'DEAD_LETTER'$sinceClause
                GROUP BY handler
                ORDER BY cnt DESC
                """,
            )
            if (since != null) query.bind("since", since)
            query.map { rs, _ ->
                HandlerSummary(
                    handler = rs.getString("handler"),
                    count = rs.getInt("cnt"),
                    latestError = rs.getString("latest_error"),
                    earliest = rs.getTimestamp("earliest")?.toInstant(),
                    latest = rs.getTimestamp("latest")?.toInstant(),
                )
            }.list()
        }

    /** Count dead-lettered tasks grouped by group_id (§3.3). */
    fun summaryByGroupId(since: Instant? = null): List<GroupSummary> =
        jdbi.withHandle<List<GroupSummary>, Exception> { h ->
            val sinceClause = if (since != null) " AND created_at >= :since" else ""
            val query = h.createQuery(
                """
                SELECT group_id, handler, COUNT(*) AS cnt,
                       MAX(error_message) AS latest_error,
                       MIN(created_at) AS earliest,
                       MAX(created_at) AS latest
                FROM task
                WHERE status = 'DEAD_LETTER' AND group_id IS NOT NULL$sinceClause
                GROUP BY group_id, handler
                ORDER BY cnt DESC
                """,
            )
            if (since != null) query.bind("since", since)
            query.map { rs, _ ->
                GroupSummary(
                    groupId = rs.getString("group_id"),
                    handler = rs.getString("handler"),
                    count = rs.getInt("cnt"),
                    latestError = rs.getString("latest_error"),
                    earliest = rs.getTimestamp("earliest")?.toInstant(),
                    latest = rs.getTimestamp("latest")?.toInstant(),
                )
            }.list()
        }

    /** Error pattern grouping — first 200 chars of error_message (§3.4). */
    fun errorPatternGroups(handler: String? = null, since: Instant? = null): List<ErrorPatternGroup> =
        jdbi.withHandle<List<ErrorPatternGroup>, Exception> { h ->
            val conditions = mutableListOf("status = 'DEAD_LETTER'")
            val binds = mutableMapOf<String, Any>()
            handler?.let { conditions.add("handler = :handler"); binds["handler"] = it }
            since?.let { conditions.add("created_at >= :since"); binds["since"] = it }
            val where = conditions.joinToString(" AND ")

            val query = h.createQuery(
                """
                SELECT SUBSTR(error_message, 1, 200) AS error_pattern, COUNT(*) AS cnt
                FROM task
                WHERE $where
                GROUP BY SUBSTR(error_message, 1, 200)
                ORDER BY cnt DESC
                """,
            )
            for ((k, v) in binds) query.bind(k, v)
            query.map { rs, _ ->
                ErrorPatternGroup(
                    errorPattern = rs.getString("error_pattern") ?: "",
                    count = rs.getInt("cnt"),
                )
            }.list()
        }

    // ── Replay Operations ─────────────────────────────────────────

    /**
     * Replay a single dead-lettered task: DEAD_LETTER → PENDING (§4.1).
     *
     * @return true if the task was replayed, false if already replayed (race)
     */
    fun replaySingle(taskId: String, maxRetries: Int?, scheduledAt: Instant?): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            val setMaxRetries = if (maxRetries != null) ", max_retries = :maxRetries" else ""
            val update = h.createUpdate(
                """
                UPDATE task SET status = 'PENDING', retry_count = 0,
                    error_message = NULL, claimed_by = NULL, claimed_at = NULL,
                    scheduled_at = :scheduledAt$setMaxRetries
                WHERE task_id = :taskId AND status = 'DEAD_LETTER'
                """,
            )
                .bind("taskId", taskId)
                .bind("scheduledAt", scheduledAt)
            if (maxRetries != null) update.bind("maxRetries", maxRetries)
            update.execute() > 0
        }

    /**
     * Bulk replay by filter: atomically transition matching tasks to PENDING (§4.2).
     *
     * @return count of tasks replayed
     */
    fun replayByFilter(
        handler: String? = null,
        groupId: String? = null,
        since: Instant? = null,
        errorPattern: String? = null,
        maxRetries: Int? = null,
        scheduledAt: Instant? = null,
    ): Int = jdbi.withHandle<Int, Exception> { h ->
        val conditions = mutableListOf("status = 'DEAD_LETTER'")
        val binds = mutableMapOf<String, Any>()

        handler?.let { conditions.add("handler = :handler"); binds["handler"] = it }
        groupId?.let { conditions.add("group_id = :groupId"); binds["groupId"] = it }
        since?.let { conditions.add("created_at >= :since"); binds["since"] = it }
        errorPattern?.let { conditions.add("error_message LIKE :errorPattern"); binds["errorPattern"] = it }

        val where = conditions.joinToString(" AND ")
        val setMaxRetries = if (maxRetries != null) ", max_retries = :maxRetries" else ""

        val update = h.createUpdate(
            """
            UPDATE task SET status = 'PENDING', retry_count = 0,
                error_message = NULL, claimed_by = NULL, claimed_at = NULL,
                scheduled_at = :scheduledAt$setMaxRetries
            WHERE $where
            """,
        )
        for ((k, v) in binds) update.bind(k, v)
        update.bind("scheduledAt", scheduledAt)
        if (maxRetries != null) update.bind("maxRetries", maxRetries)
        update.execute()
    }

    /**
     * Replay all dead-lettered tasks for a job and adjust MR counters atomically (§4.3–4.4).
     *
     * Transaction:
     * 1. Count DEAD_LETTER tasks for the group
     * 2. UPDATE tasks → PENDING
     * 3. Decrement mr_job.failed_tasks
     * 4. If job is FAILED, transition to RUNNING (CAS with version)
     *
     * @return count of tasks replayed, or -1 if the job is COMPLETED (rejected)
     */
    fun replayJob(jobId: String, force: Boolean = false): Int =
        jdbi.inTransaction<Int, Exception> { h ->
            // Check job status
            val jobRow = h.createQuery(
                "SELECT status, version FROM mr_job WHERE job_id = :jobId",
            ).bind("jobId", jobId)
                .map { rs, _ -> rs.getString("status") to rs.getLong("version") }
                .findOne().orElse(null)
                ?: return@inTransaction 0

            val (jobStatus, version) = jobRow

            // Guard: reject replay for COMPLETED jobs unless forced (§4.4)
            if (jobStatus == JobStatus.COMPLETED.name && !force) {
                return@inTransaction -1
            }

            // Replay all DEAD_LETTER tasks for this group
            val replayed = h.createUpdate(
                """
                UPDATE task SET status = 'PENDING', retry_count = 0,
                    error_message = NULL, claimed_by = NULL, claimed_at = NULL,
                    scheduled_at = NULL
                WHERE group_id = :jobId AND status = 'DEAD_LETTER'
                """,
            ).bind("jobId", jobId).execute()

            if (replayed == 0) return@inTransaction 0

            // Decrement failed_tasks on the job
            h.createUpdate(
                """
                UPDATE mr_job SET failed_tasks = GREATEST(failed_tasks - :count, 0),
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = :jobId
                """,
            ).bind("jobId", jobId).bind("count", replayed).execute()

            // If job was FAILED, transition back to RUNNING
            if (jobStatus == JobStatus.FAILED.name) {
                h.createUpdate(
                    """
                    UPDATE mr_job SET status = 'RUNNING', version = version + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = :jobId AND status = 'FAILED' AND version = :version
                    """,
                ).bind("jobId", jobId).bind("version", version).execute()
            }

            replayed
        }

    // ── Cleanup ───────────────────────────────────────────────────

    /**
     * Delete dead-lettered tasks older than the retention period.
     *
     * @return count of tasks deleted
     */
    fun deleteOlderThan(cutoff: Instant): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createUpdate(
                "DELETE FROM task WHERE status = 'DEAD_LETTER' AND created_at < :cutoff",
            ).bind("cutoff", cutoff).execute()
        }

    /** Count all dead-lettered tasks. */
    fun countAll(): Int = jdbi.withHandle<Int, Exception> { h ->
        h.createQuery("SELECT COUNT(*) FROM task WHERE status = 'DEAD_LETTER'")
            .mapTo(Int::class.java).one()
    }

    /** Count dead-lettered tasks by handler. */
    fun countByHandler(handler: String): Int = jdbi.withHandle<Int, Exception> { h ->
        h.createQuery("SELECT COUNT(*) FROM task WHERE status = 'DEAD_LETTER' AND handler = :handler")
            .bind("handler", handler)
            .mapTo(Int::class.java).one()
    }
}

data class HandlerSummary(
    val handler: String,
    val count: Int,
    val latestError: String?,
    val earliest: Instant?,
    val latest: Instant?,
)

data class GroupSummary(
    val groupId: String,
    val handler: String,
    val count: Int,
    val latestError: String?,
    val earliest: Instant?,
    val latest: Instant?,
)

data class ErrorPatternGroup(
    val errorPattern: String,
    val count: Int,
)
