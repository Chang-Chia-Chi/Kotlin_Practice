package com.mapreduce.queue.repository

import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Layer 1 persistence — generic task CRUD.
 *
 * Every operation here is task-type agnostic. It knows nothing about
 * map-reduce, jobs, or any orchestration pattern.
 */
@ApplicationScoped
class TaskRepository(private val jdbi: Jdbi) {

    fun enqueue(request: EnqueueRequest): String {
        val taskId = UUID.randomUUID().toString()
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, scheduled_at, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', :priority,
                    :groupId, :metadata, :scheduledAt, 0, :maxRetries, CURRENT_TIMESTAMP)
                """
            )
                .bind("taskId", taskId)
                .bind("handler", request.handler)
                .bind("queue", request.queue)
                .bind("payload", request.payload)
                .bind("priority", request.priority)
                .bind("groupId", request.groupId)
                .bind("metadata", request.metadata)
                .bind("scheduledAt", request.scheduledAt)
                .bind("maxRetries", request.maxRetries)
                .execute()
        }
        return taskId
    }

    /**
     * Claim one task using SELECT FOR UPDATE SKIP LOCKED.
     *
     * Filters by subscribed [queues], PENDING status, and scheduled_at.
     * Orders by priority DESC, created_at ASC.
     */
    fun claim(workerId: String, queues: List<String>): Task? {
        if (queues.isEmpty()) return null

        return jdbi.inTransaction<Task?, Exception> { h ->
            val inClause = queues.indices.joinToString(", ") { ":queue$it" }

            val query = h.createQuery(
                """
                SELECT * FROM task
                WHERE status = 'PENDING'
                  AND queue IN ($inClause)
                  AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP)
                ORDER BY priority DESC, created_at ASC
                FETCH FIRST 1 ROWS ONLY
                FOR UPDATE SKIP LOCKED
                """
            )
            queues.forEachIndexed { i, q -> query.bind("queue$i", q) }

            val task = query.mapTo(Task::class.java).findOne().orElse(null) ?: return@inTransaction null

            h.createUpdate(
                """
                UPDATE task SET status = 'CLAIMED', claimed_by = :workerId,
                    claimed_at = CURRENT_TIMESTAMP
                WHERE task_id = :taskId
                """
            )
                .bind("workerId", workerId)
                .bind("taskId", task.taskId)
                .execute()

            task.copy(status = TaskStatus.CLAIMED, claimedBy = workerId)
        }
    }

    /**
     * Mark a task as COMPLETED. Idempotent — no-op if already completed
     * (supports handlers that complete the task themselves in the same transaction
     * as their side-effects, e.g. map-reduce handlers).
     */
    fun complete(taskId: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP
                WHERE task_id = :taskId AND status = 'CLAIMED'
                """
            )
                .bind("taskId", taskId)
                .execute()
        }
    }

    /**
     * Handle task failure with automatic retry / dead-letter logic.
     *
     * Increments retry_count. If retries remain, resets to PENDING (with optional
     * delay via [retryDelay]). Otherwise, moves to DEAD_LETTER.
     */
    fun fail(taskId: String, errorMessage: String, retryDelay: Duration? = null) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                UPDATE task SET retry_count = retry_count + 1,
                    error_message = :error
                WHERE task_id = :taskId
                """
            )
                .bind("taskId", taskId)
                .bind("error", errorMessage.take(4000))
                .execute()

            val (retryCount, maxRetries) = h.createQuery(
                "SELECT retry_count, max_retries FROM task WHERE task_id = :taskId"
            )
                .bind("taskId", taskId)
                .map { rs, _ -> rs.getInt("retry_count") to rs.getInt("max_retries") }
                .one()

            if (retryCount < maxRetries) {
                if (retryDelay != null) {
                    h.createUpdate(
                        """
                        UPDATE task SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
                            scheduled_at = CURRENT_TIMESTAMP + NUMTODSINTERVAL(:delay, 'SECOND')
                        WHERE task_id = :taskId
                        """
                    ).bind("taskId", taskId).bind("delay", retryDelay.toSeconds()).execute()
                } else {
                    h.createUpdate(
                        """
                        UPDATE task SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL
                        WHERE task_id = :taskId
                        """
                    ).bind("taskId", taskId).execute()
                }
            } else {
                h.createUpdate(
                    "UPDATE task SET status = 'DEAD_LETTER' WHERE task_id = :taskId"
                ).bind("taskId", taskId).execute()
            }
        }
    }

    /** Immediately dead-letter a task (e.g. unrecognized handler). */
    fun deadLetter(taskId: String, reason: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE task SET status = 'DEAD_LETTER', error_message = :reason
                WHERE task_id = :taskId
                """
            )
                .bind("taskId", taskId)
                .bind("reason", reason.take(4000))
                .execute()
        }
    }

    fun findStaleTasks(threshold: Instant): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h.createQuery(
                "SELECT * FROM task WHERE status = 'CLAIMED' AND claimed_at < :threshold"
            )
                .bind("threshold", threshold)
                .mapTo(Task::class.java)
                .list()
        }

    /**
     * Reclaim a stale task: increment retry, then either PENDING or DEAD_LETTER.
     */
    fun reclaimStaleTask(taskId: String) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                UPDATE task SET retry_count = retry_count + 1
                WHERE task_id = :taskId AND status = 'CLAIMED'
                """
            ).bind("taskId", taskId).execute()

            val (retryCount, maxRetries) = h.createQuery(
                "SELECT retry_count, max_retries FROM task WHERE task_id = :taskId"
            )
                .bind("taskId", taskId)
                .map { rs, _ -> rs.getInt("retry_count") to rs.getInt("max_retries") }
                .one()

            if (retryCount < maxRetries) {
                h.createUpdate(
                    """
                    UPDATE task SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL
                    WHERE task_id = :taskId
                    """
                ).bind("taskId", taskId).execute()
            } else {
                h.createUpdate(
                    "UPDATE task SET status = 'DEAD_LETTER' WHERE task_id = :taskId"
                ).bind("taskId", taskId).execute()
            }
        }
    }

    /** Count tasks by group and status — used by MR orchestrator for barrier detection. */
    fun countByGroupAndStatus(groupId: String, status: TaskStatus): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE group_id = :groupId AND status = :status"
            )
                .bind("groupId", groupId)
                .bind("status", status.name)
                .mapTo(Int::class.java)
                .one()
        }

    fun findByGroupAndHandler(groupId: String, handler: String): Task? =
        jdbi.withHandle<Task?, Exception> { h ->
            h.createQuery(
                "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler"
            )
                .bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .findOne().orElse(null)
        }
}
