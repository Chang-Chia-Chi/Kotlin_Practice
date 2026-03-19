package com.mapreduce.queue.repository

import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.time.Duration
import java.time.Instant
import java.util.UUID

// TODO: check indexes of task table to ensure good query performance

/**
 * Layer 1 persistence — generic task CRUD.
 *
 * Every operation here is task-type agnostic. It knows nothing about
 * map-reduce, jobs, or any orchestration pattern.
 */
@ApplicationScoped
class TaskRepository(
    private val jdbi: Jdbi,
) {
    fun enqueue(request: EnqueueRequest): String {
        val taskId = UUID.randomUUID().toString()
        jdbi.useHandle<Exception> { h ->
            h
                .createUpdate(
                    """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, scheduled_at, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', :priority,
                    :groupId, :metadata, :scheduledAt, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
                ).bind("taskId", taskId)
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
    fun claim(
        workerId: String,
        queues: List<String>,
    ): Task? {
        if (queues.isEmpty()) return null

        return jdbi.inTransaction<Task?, Exception> { h ->
            val inClause = queues.indices.joinToString(", ") { ":queue$it" }

            val query =
                h.createQuery(
                    """
                SELECT * FROM task
                WHERE status = 'PENDING'
                  AND queue IN ($inClause)
                  AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP)
                ORDER BY priority DESC, created_at ASC
                FETCH FIRST 1 ROWS ONLY
                FOR UPDATE SKIP LOCKED
                """,
                )
            queues.forEachIndexed { i, q -> query.bind("queue$i", q) }

            val task = query.mapTo(Task::class.java).findOne().orElse(null) ?: return@inTransaction null

            val generation = UUID.randomUUID().toString()
            h
                .createUpdate(
                    """
                UPDATE task SET status = 'CLAIMED', claimed_by = :workerId,
                    claimed_at = CURRENT_TIMESTAMP,
                    execution_generation = :generation
                WHERE task_id = :taskId
                """,
                ).bind("workerId", workerId)
                .bind("taskId", task.taskId)
                .bind("generation", generation)
                .execute()

            task.copy(status = TaskStatus.CLAIMED, claimedBy = workerId, claimToken = generation)
        }
    }

    /**
     * Mark a task as COMPLETED. Idempotent — no-op if already completed
     * (supports handlers that complete the task themselves in the same transaction
     * as their side-effects, e.g. map-reduce handlers).
     */
    fun complete(
        taskId: String,
        claimToken: String? = null,
    ) {
        jdbi.useHandle<Exception> { h ->
            val fenceClause = if (claimToken != null) " AND execution_generation = :gen" else ""
            val update =
                h
                    .createUpdate(
                        """
                UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """,
                    ).bind("taskId", taskId)
            if (claimToken != null) update.bind("gen", claimToken)
            update.execute()
        }
    }

    /**
     * Handle task failure with automatic retry / dead-letter logic.
     *
     * Increments retry_count. If retries remain, resets to PENDING (with optional
     * delay via [retryDelay]). Otherwise, moves to DEAD_LETTER.
     *
     * @return `true` if the task was dead-lettered (retries exhausted)
     */
    fun fail(
        taskId: String,
        errorMessage: String,
        retryDelay: Duration? = null,
        claimToken: String? = null,
    ): Boolean {
        return jdbi.inTransaction<Boolean, Exception> { h ->
            val fenceClause = if (claimToken != null) " AND execution_generation = :gen" else ""
            val scheduledAt = retryDelay?.let { Instant.now().plusSeconds(it.toSeconds()) }

            val update =
                h
                    .createUpdate(
                        """
                UPDATE task SET
                    retry_count    = retry_count + 1,
                    error_message  = :error,
                    status         = CASE WHEN retry_count + 1 < max_retries THEN 'PENDING' ELSE 'DEAD_LETTER' END,
                    claimed_by     = CASE WHEN retry_count + 1 < max_retries THEN NULL ELSE claimed_by END,
                    claimed_at     = CASE WHEN retry_count + 1 < max_retries THEN NULL ELSE claimed_at END,
                    scheduled_at   = CASE WHEN retry_count + 1 < max_retries THEN :scheduledAt ELSE scheduled_at END
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """,
                    ).bind("taskId", taskId)
                    .bind("error", errorMessage.take(4000))
                    .bind("scheduledAt", scheduledAt)
            if (claimToken != null) update.bind("gen", claimToken)
            val updated = update.execute()

            if (updated == 0) return@inTransaction false

            h
                .createQuery("SELECT status FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java)
                .one() == "DEAD_LETTER"
        }
    }

    /**
     * Re-enqueue a task to PENDING without incrementing retry_count.
     *
     * Used when the retry does not consume a retry attempt (e.g., circuit
     * breaker requeue, shutdown-aware timeout). The task is moved back to
     * PENDING with an optional delay.
     */
    fun requeue(
        taskId: String,
        delay: Duration? = null,
        claimToken: String? = null,
    ) {
        jdbi.useHandle<Exception> { h ->
            val fenceClause = if (claimToken != null) " AND execution_generation = :gen" else ""
            val hasDelay = delay != null && !delay.isZero
            val scheduledClause = if (hasDelay) ", scheduled_at = :scheduledAt" else ""
            val scheduledAt = if (hasDelay) Instant.now().plusSeconds(delay!!.toSeconds()) else null

            val update =
                h
                    .createUpdate(
                        """
                UPDATE task SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL$scheduledClause
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """,
                    ).bind("taskId", taskId)

            if (claimToken != null) update.bind("gen", claimToken)
            if (hasDelay) update.bind("scheduledAt", scheduledAt)

            update.execute()
        }
    }

    /**
     * Immediately dead-letter a task (e.g. unrecognized handler).
     *
     * Guarded by `WHERE status = 'CLAIMED'` to prevent a zombie worker from
     * overwriting a task that has already been completed, retried, or dead-lettered.
     *
     * @return `true` if the task was dead-lettered, `false` if the status guard
     *         or claimToken fence rejected the update (zombie / already-handled).
     */
    fun deadLetter(
        taskId: String,
        reason: String,
        claimToken: String? = null,
    ): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            val fenceClause = if (claimToken != null) " AND execution_generation = :gen" else ""
            val update =
                h
                    .createUpdate(
                        """
                UPDATE task SET status = 'DEAD_LETTER', error_message = :reason
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """,
                    ).bind("taskId", taskId)
                    .bind("reason", reason.take(4000))
            if (claimToken != null) update.bind("gen", claimToken)
            update.execute() > 0
        }

    /**
     * Find stale CLAIMED tasks by [claimed_at] age.
     *
     * A task is stale when its [claimed_at] is older than [threshold],
     * indicating the worker has been executing it beyond the expected
     * maximum duration.
     *
     * Results are ordered by claimed_at ascending (oldest first)
     * and limited to [batchSize] to avoid locking too many rows.
     */
    fun findStaleTasks(
        threshold: Instant,
        batchSize: Int = 50,
    ): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h
                .createQuery(
                    """
                SELECT * FROM task
                WHERE status = 'CLAIMED'
                  AND claimed_at < :threshold
                ORDER BY claimed_at ASC
                FETCH FIRST :batchSize ROWS ONLY
                """,
                ).bind("threshold", threshold)
                .bind("batchSize", batchSize)
                .mapTo(Task::class.java)
                .list()
        }

    /**
     * Reclaim a stale task.
     *
     * Increments retry_count, clears claim state, then sets
     * status to PENDING or DEAD_LETTER based on remaining retries.
     *
     * Idempotent via `WHERE status = 'CLAIMED'` — if two leaders race,
     * only one sees CLAIMED and updates; the other gets 0 rows.
     *
     * @param errorMessage descriptive message including the dead pod ID
     * @return `true` if dead-lettered, `false` if reclaimed to PENDING,
     *         `null` if already handled (0 rows — status was not CLAIMED)
     */
    fun reclaimStaleTask(
        taskId: String,
        errorMessage: String,
    ): Boolean? {
        return jdbi.inTransaction<Boolean?, Exception> { h ->
            val updated =
                h
                    .createUpdate(
                        """
                UPDATE task
                   SET retry_count    = retry_count + 1,
                       claimed_by     = NULL,
                       claimed_at     = NULL,
                       error_message  = :error,
                       status         = CASE WHEN retry_count + 1 < max_retries THEN 'PENDING' ELSE 'DEAD_LETTER' END
                 WHERE task_id  = :taskId
                   AND status   = 'CLAIMED'
                """,
                    ).bind("taskId", taskId)
                    .bind("error", errorMessage.take(4000))
                    .execute()

            if (updated == 0) return@inTransaction null

            h
                .createQuery("SELECT status FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java)
                .one() == "DEAD_LETTER"
        }
    }

    /** Count tasks by group and status — used by MR orchestrator for barrier detection. */
    fun countByGroupAndStatus(
        groupId: String,
        status: TaskStatus,
    ): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h
                .createQuery(
                    "SELECT COUNT(*) FROM task WHERE group_id = :groupId AND status = :status",
                ).bind("groupId", groupId)
                .bind("status", status.name)
                .mapTo(Int::class.java)
                .one()
        }

    fun findById(taskId: String): Task? =
        jdbi.withHandle<Task?, Exception> { h ->
            h
                .createQuery("SELECT * FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(Task::class.java)
                .findOne()
                .orElse(null)
        }

    fun findByGroupAndHandler(
        groupId: String,
        handler: String,
    ): Task? =
        jdbi.withHandle<Task?, Exception> { h ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler",
                ).bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .findOne()
                .orElse(null)
        }

    fun findAllByGroupAndHandler(
        groupId: String,
        handler: String,
    ): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler",
                ).bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .list()
        }

    fun findCompletedByGroupAndHandler(
        groupId: String,
        handler: String,
    ): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler AND status = 'COMPLETED'",
                ).bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .list()
        }

    fun findClaimedByGroupAndHandler(
        groupId: String,
        handler: String,
    ): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler AND status = 'CLAIMED'",
                ).bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .list()
        }

    /**
     * Release all tasks claimed by this pod back to PENDING.
     * Used during graceful shutdown Phase 3 — no retry count increment.
     *
     * The WHERE clause guards against racing with handlers that complete
     * between the decision to release and the UPDATE execution.
     *
     * @return number of tasks released
     */
    fun releaseTasksByPod(podId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h
                .createUpdate(
                    """
                UPDATE task
                   SET status         = 'PENDING',
                       claimed_by     = NULL,
                       claimed_at     = NULL,
                       scheduled_at   = NULL
                 WHERE claimed_by     = :podId
                   AND status         = 'CLAIMED'
                """,
                ).bind("podId", podId)
                .execute()
        }
}
