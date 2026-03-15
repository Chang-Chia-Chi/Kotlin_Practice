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
     * Sets [last_heartbeat] to current timestamp so the reaper can track liveness.
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

            val generation = UUID.randomUUID().toString()
            h.createUpdate(
                """
                UPDATE task SET status = 'CLAIMED', claimed_by = :workerId,
                    claimed_at = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP,
                    execution_generation = :generation
                WHERE task_id = :taskId
                """
            )
                .bind("workerId", workerId)
                .bind("taskId", task.taskId)
                .bind("generation", generation)
                .execute()

            task.copy(status = TaskStatus.CLAIMED, claimedBy = workerId, executionGeneration = generation)
        }
    }

    /**
     * Update heartbeat timestamp for a claimed task.
     *
     * Called periodically by the worker loop while a handler is executing.
     * Fenced by [executionGeneration] to prevent a stale worker from
     * heartbeating a task that has been reclaimed and reassigned.
     */
    fun updateHeartbeat(taskId: String, executionGeneration: String?) {
        jdbi.useHandle<Exception> { h ->
            val fenceClause = if (executionGeneration != null) " AND execution_generation = :gen" else ""
            val update = h.createUpdate(
                "UPDATE task SET last_heartbeat = CURRENT_TIMESTAMP WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause"
            ).bind("taskId", taskId)
            if (executionGeneration != null) update.bind("gen", executionGeneration)
            update.execute()
        }
    }

    /**
     * Mark a task as COMPLETED. Idempotent — no-op if already completed
     * (supports handlers that complete the task themselves in the same transaction
     * as their side-effects, e.g. map-reduce handlers).
     */
    fun complete(taskId: String, executionGeneration: String? = null) {
        jdbi.useHandle<Exception> { h ->
            val sql = if (executionGeneration != null) {
                """
                UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP,
                    last_heartbeat = NULL
                WHERE task_id = :taskId AND status = 'CLAIMED' AND execution_generation = :gen
                """
            } else {
                """
                UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP,
                    last_heartbeat = NULL
                WHERE task_id = :taskId AND status = 'CLAIMED'
                """
            }
            val update = h.createUpdate(sql).bind("taskId", taskId)
            if (executionGeneration != null) update.bind("gen", executionGeneration)
            update.execute()
        }
    }

    /**
     * Handle task failure with automatic retry / dead-letter logic.
     *
     * Increments retry_count. If retries remain, resets to PENDING (with optional
     * delay via [retryDelay]). Otherwise, moves to DEAD_LETTER.
     * Clears [last_heartbeat] in all cases since the task is no longer actively claimed.
     *
     * @return `true` if the task was dead-lettered (retries exhausted)
     */
    fun fail(taskId: String, errorMessage: String, retryDelay: Duration? = null, executionGeneration: String? = null): Boolean {
        return jdbi.inTransaction<Boolean, Exception> { h ->
            val fenceClause = if (executionGeneration != null) " AND execution_generation = :gen" else ""
            val update = h.createUpdate(
                """
                UPDATE task SET retry_count = retry_count + 1,
                    error_message = :error
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """
            )
                .bind("taskId", taskId)
                .bind("error", errorMessage.take(4000))
            if (executionGeneration != null) update.bind("gen", executionGeneration)
            val updated = update.execute()

            if (updated == 0) return@inTransaction false

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
                            last_heartbeat = NULL,
                            scheduled_at = CURRENT_TIMESTAMP + NUMTODSINTERVAL(:delay, 'SECOND')
                        WHERE task_id = :taskId
                        """
                    ).bind("taskId", taskId).bind("delay", retryDelay.toSeconds()).execute()
                } else {
                    h.createUpdate(
                        """
                        UPDATE task SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
                            last_heartbeat = NULL
                        WHERE task_id = :taskId
                        """
                    ).bind("taskId", taskId).execute()
                }
                false
            } else {
                h.createUpdate(
                    "UPDATE task SET status = 'DEAD_LETTER', last_heartbeat = NULL WHERE task_id = :taskId"
                ).bind("taskId", taskId).execute()
                true
            }
        }
    }

    /**
     * Re-enqueue a task to PENDING without incrementing retry_count.
     *
     * Used when the retry does not consume a retry attempt (e.g., circuit
     * breaker requeue, shutdown-aware timeout). The task is moved back to
     * PENDING with an optional delay.
     */
    fun requeue(taskId: String, delay: Duration? = null, executionGeneration: String? = null) {
        jdbi.useHandle<Exception> { h ->
            val fenceClause = if (executionGeneration != null) " AND execution_generation = :gen" else ""
            val scheduledClause = if (delay != null && !delay.isZero)
                ", scheduled_at = CURRENT_TIMESTAMP + NUMTODSINTERVAL(:delay, 'SECOND')"
            else ""

            val update = h.createUpdate(
                """
                UPDATE task SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
                    last_heartbeat = NULL$scheduledClause
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """
            )
                .bind("taskId", taskId)

            if (executionGeneration != null) update.bind("gen", executionGeneration)
            if (delay != null && !delay.isZero) update.bind("delay", delay.toSeconds())

            update.execute()
        }
    }

    /** Immediately dead-letter a task (e.g. unrecognized handler). */
    fun deadLetter(taskId: String, reason: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE task SET status = 'DEAD_LETTER', error_message = :reason,
                    last_heartbeat = NULL
                WHERE task_id = :taskId
                """
            )
                .bind("taskId", taskId)
                .bind("reason", reason.take(4000))
                .execute()
        }
    }

    /**
     * Find stale CLAIMED tasks by heartbeat age.
     *
     * A task is stale when its [last_heartbeat] is older than [threshold]
     * (or NULL, which indicates a claimed task that never heartbeated —
     * defensive handling for migration edge cases).
     *
     * Results are ordered by heartbeat age descending (oldest first)
     * and limited to [batchSize] to avoid locking too many rows.
     */
    fun findStaleTasks(threshold: Instant, batchSize: Int = 50): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h.createQuery(
                """
                SELECT * FROM task
                WHERE status = 'CLAIMED'
                  AND (last_heartbeat IS NULL OR last_heartbeat < :threshold)
                ORDER BY last_heartbeat ASC NULLS FIRST
                FETCH FIRST :batchSize ROWS ONLY
                """
            )
                .bind("threshold", threshold)
                .bind("batchSize", batchSize)
                .mapTo(Task::class.java)
                .list()
        }

    /**
     * Reclaim a stale task with fenced writes.
     *
     * Increments retry_count, clears claim state and heartbeat, then sets
     * status to PENDING or DEAD_LETTER based on remaining retries.
     *
     * The [leaderEpoch] fence prevents a zombie leader from reclaiming
     * tasks that the current leader has already handled:
     *   WHERE last_epoch <= :epoch ... SET last_epoch = :epoch
     *
     * @param errorMessage descriptive message including the dead pod ID
     * @return `true` if the task was dead-lettered (retries exhausted),
     *         `false` if reclaimed to PENDING, or if the fence/status check failed (0 rows)
     */
    fun reclaimStaleTask(taskId: String, leaderEpoch: Long, errorMessage: String): Boolean {
        return jdbi.inTransaction<Boolean, Exception> { h ->
            val updated = h.createUpdate(
                """
                UPDATE task
                   SET retry_count    = retry_count + 1,
                       claimed_by     = NULL,
                       claimed_at     = NULL,
                       last_heartbeat = NULL,
                       error_message  = :error,
                       last_epoch     = :epoch
                 WHERE task_id  = :taskId
                   AND status   = 'CLAIMED'
                   AND last_epoch <= :epoch
                """
            )
                .bind("taskId", taskId)
                .bind("error", errorMessage.take(4000))
                .bind("epoch", leaderEpoch)
                .execute()

            if (updated == 0) return@inTransaction false

            val (retryCount, maxRetries) = h.createQuery(
                "SELECT retry_count, max_retries FROM task WHERE task_id = :taskId"
            )
                .bind("taskId", taskId)
                .map { rs, _ -> rs.getInt("retry_count") to rs.getInt("max_retries") }
                .one()

            if (retryCount < maxRetries) {
                h.createUpdate("UPDATE task SET status = 'PENDING' WHERE task_id = :taskId")
                    .bind("taskId", taskId).execute()
                false
            } else {
                h.createUpdate("UPDATE task SET status = 'DEAD_LETTER' WHERE task_id = :taskId")
                    .bind("taskId", taskId).execute()
                true
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

    /** Count PENDING tasks grouped by queue — used by the leader for the HPA queue depth gauge. */
    fun countPendingByQueue(): Map<String, Int> =
        jdbi.withHandle<Map<String, Int>, Exception> { h ->
            h.createQuery(
                """
                SELECT queue, COUNT(*) AS cnt FROM task
                WHERE status = 'PENDING'
                GROUP BY queue
                """
            )
                .map { rs, _ -> rs.getString("queue") to rs.getInt("cnt") }
                .list()
                .toMap()
        }

    fun findById(taskId: String): Task? =
        jdbi.withHandle<Task?, Exception> { h ->
            h.createQuery("SELECT * FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(Task::class.java)
                .findOne().orElse(null)
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

    fun findAllByGroupAndHandler(groupId: String, handler: String): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h.createQuery(
                "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler"
            )
                .bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .list()
        }

    fun findCompletedByGroupAndHandler(groupId: String, handler: String): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h.createQuery(
                "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler AND status = 'COMPLETED'"
            )
                .bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .list()
        }

    fun findClaimedByGroupAndHandler(groupId: String, handler: String): List<Task> =
        jdbi.withHandle<List<Task>, Exception> { h ->
            h.createQuery(
                "SELECT * FROM task WHERE group_id = :groupId AND handler = :handler AND status = 'CLAIMED'"
            )
                .bind("groupId", groupId)
                .bind("handler", handler)
                .mapTo(Task::class.java)
                .list()
        }

    /**
     * Release all tasks claimed by this pod back to PENDING.
     * Used during graceful shutdown Phase 3 — no retry count increment.
     * Clears [last_heartbeat] since tasks are no longer actively claimed.
     *
     * The WHERE clause guards against racing with handlers that complete
     * between the decision to release and the UPDATE execution.
     *
     * @return number of tasks released
     */
    fun releaseTasksByPod(podId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createUpdate(
                """
                UPDATE task
                   SET status         = 'PENDING',
                       claimed_by     = NULL,
                       claimed_at     = NULL,
                       last_heartbeat = NULL,
                       scheduled_at   = NULL
                 WHERE claimed_by     = :podId
                   AND status         = 'CLAIMED'
                """
            )
                .bind("podId", podId)
                .execute()
        }

    fun markSpeculative(taskId: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate("UPDATE task SET speculative = 1 WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .execute()
        }
    }
}
