package com.mapreduce.queue.repository

import com.mapreduce.leader.FencedRepository
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Result of resolving a task within a group.
 * [barrierMet] is true when this was the final task for the current phase.
 */
data class GroupTaskResolution(
    val updated: Boolean,
    val barrierMet: Boolean,
)

/**
 * Lightweight value object for streaming task outputs (URI + metadata).
 */
data class TaskOutput(
    val uri: String,
    val metadata: String?,
)

/**
 * Result of an atomic group failure operation.
 *
 * @param taskUpdated false if the status guard / claimToken fence rejected the update (zombie)
 * @param deadLettered true if the task was terminally dead-lettered (retries exhausted or unconditional)
 * @param barrierMet true if this was the last pending task in the group
 */
data class GroupFailResult(
    val taskUpdated: Boolean,
    val deadLettered: Boolean,
    val barrierMet: Boolean,
)

/**
 * Layer 1 persistence for task groups with countdown barrier detection.
 *
 * The core mechanism: when a task reaches a terminal state (success or dead-letter),
 * [tasks_pending] is decremented under a row lock. The worker that drives it to zero
 * atomically inserts a callback task in the same transaction — no polling needed.
 */
@ApplicationScoped
class TaskGroupRepository(
    jdbi: Jdbi,
) : FencedRepository(jdbi) {

    private val log = Logger.getLogger(TaskGroupRepository::class.java)

    /**
     * Atomic fan-out: insert task_group row + N tasks in one transaction.
     * [tasks_pending] is initialized to the number of tasks.
     */
    fun submitGroup(group: TaskGroup, tasks: List<EnqueueRequest>) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO task_group (group_id, group_type, status, params, queue,
                    phase, phase_total, tasks_pending, tasks_failed,
                    on_complete_handler, failure_policy, failure_threshold,
                    result_metadata, version, last_epoch, deadline_at, created_at, updated_at)
                VALUES (:groupId, :groupType, :status, :params, :queue,
                    :phase, :phaseTotal, :tasksPending, 0,
                    :onCompleteHandler, :failurePolicy, :failureThreshold,
                    :resultMetadata, 0, 0, :deadlineAt, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            ).bind("groupId", group.groupId)
                .bind("groupType", group.groupType)
                .bind("status", group.status.name)
                .bind("params", group.params)
                .bind("queue", group.queue)
                .bind("phase", group.phase)
                .bind("phaseTotal", group.phaseTotal)
                .bind("tasksPending", group.phaseTotal)
                .bind("onCompleteHandler", group.onCompleteHandler)
                .bind("failurePolicy", group.failurePolicy)
                .bind("failureThreshold", group.failureThreshold)
                .bind("resultMetadata", group.resultMetadata)
                .bind("deadlineAt", group.deadlineAt)
                .execute()

            val batch = h.prepareBatch(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', :priority,
                    :groupId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
            )
            for (task in tasks) {
                batch
                    .bind("taskId", UUID.randomUUID().toString())
                    .bind("handler", task.handler)
                    .bind("queue", task.queue)
                    .bind("payload", task.payload)
                    .bind("priority", task.priority)
                    .bind("groupId", task.groupId)
                    .bind("metadata", task.metadata)
                    .bind("maxRetries", task.maxRetries)
                    .add()
            }
            batch.execute()
        }
    }

    /**
     * Unified task resolution: decrement [tasks_pending], conditionally increment
     * [tasks_failed], check barrier, and create callback task if barrier is met.
     *
     * For successful tasks ([failed] = false): also marks the task row COMPLETED
     * with output fields, guarded by execution_generation (zombie detection).
     *
     * For failed tasks ([failed] = true): only updates the group counters.
     * The caller (TaskDispatcher/StaleTaskReaper) has already marked the task
     * as DEAD_LETTER before calling this.
     *
     * The row lock on task_group serializes concurrent resolutions, ensuring
     * exactly one worker observes the barrier condition.
     */
    fun resolveGroupTask(
        taskId: String? = null,
        groupId: String,
        claimToken: String? = null,
        failed: Boolean = false,
        outputUri: String? = null,
        outputMetadata: String? = null,
    ): GroupTaskResolution {
        return jdbi.inTransaction<GroupTaskResolution, Exception> { h ->
            // Step 1 (success path only): Mark task COMPLETED with output fields
            if (!failed && taskId != null) {
                val fenceClause = if (claimToken != null) " AND execution_generation = :gen" else ""
                val update = h.createUpdate(
                    """
                    UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP,
                        output_uri = :outputUri, output_metadata = :outputMeta
                    WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                    """,
                ).bind("taskId", taskId)
                    .bind("outputUri", outputUri)
                    .bind("outputMeta", outputMetadata)
                if (claimToken != null) update.bind("gen", claimToken)
                val updated = update.execute()

                if (updated == 0) {
                    log.warnf("Zombie detected for task %s (gen=%s) — skipping group counter", taskId, claimToken)
                    return@inTransaction GroupTaskResolution(updated = false, barrierMet = false)
                }
            }

            val barrierMet = resolveGroupCounter(h, groupId, failed)
            GroupTaskResolution(updated = true, barrierMet = barrierMet)
        }
    }

    // ── Atomic group failure methods ───────────────────────────────────

    /**
     * Atomic fail-with-retry for a grouped task.
     *
     * In a single transaction: increments retry_count, conditionally moves to
     * DEAD_LETTER (retries exhausted) or PENDING (retries remain). Only when
     * the task is terminally dead-lettered does this decrement the group counter.
     *
     * @return [GroupFailResult] — `taskUpdated=false` if fenced out (zombie).
     */
    fun failGroupTask(
        taskId: String,
        groupId: String,
        errorMessage: String,
        retryDelay: Duration? = null,
        claimToken: String? = null,
    ): GroupFailResult {
        return jdbi.inTransaction<GroupFailResult, Exception> { h ->
            val fenceClause = if (claimToken != null) " AND execution_generation = :gen" else ""
            val scheduledAt = retryDelay?.let { Instant.now().plusSeconds(it.toSeconds()) }

            val update = h.createUpdate(
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
            )
                .bind("taskId", taskId)
                .bind("error", errorMessage.take(4000))
                .bind("scheduledAt", scheduledAt)
            if (claimToken != null) update.bind("gen", claimToken)
            val updated = update.execute()

            if (updated == 0) {
                return@inTransaction GroupFailResult(taskUpdated = false, deadLettered = false, barrierMet = false)
            }

            val newStatus = h.createQuery("SELECT status FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java)
                .one()
            val deadLettered = newStatus == "DEAD_LETTER"

            val barrierMet = if (deadLettered) resolveGroupCounter(h, groupId, failed = true) else false
            GroupFailResult(taskUpdated = true, deadLettered = deadLettered, barrierMet = barrierMet)
        }
    }

    /**
     * Atomic unconditional dead-letter for a grouped task.
     *
     * In a single transaction: marks task DEAD_LETTER (with status guard)
     * and decrements the group counter.
     *
     * Used for no-handler and explicit [TaskResult.DeadLetter] results.
     *
     * @return [GroupFailResult] — `taskUpdated=false` if fenced out (zombie).
     */
    fun deadLetterGroupTask(
        taskId: String,
        groupId: String,
        reason: String,
        claimToken: String? = null,
    ): GroupFailResult {
        return jdbi.inTransaction<GroupFailResult, Exception> { h ->
            val fenceClause = if (claimToken != null) " AND execution_generation = :gen" else ""
            val update = h.createUpdate(
                """
                UPDATE task SET status = 'DEAD_LETTER', error_message = :reason
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """,
            )
                .bind("taskId", taskId)
                .bind("reason", reason.take(4000))
            if (claimToken != null) update.bind("gen", claimToken)
            val updated = update.execute()

            if (updated == 0) {
                return@inTransaction GroupFailResult(taskUpdated = false, deadLettered = false, barrierMet = false)
            }

            val barrierMet = resolveGroupCounter(h, groupId, failed = true)
            GroupFailResult(taskUpdated = true, deadLettered = true, barrierMet = barrierMet)
        }
    }

    /**
     * Atomic reclaim for a grouped stale task.
     *
     * In a single transaction: clears claim fields, increments retry_count,
     * conditionally DEAD_LETTER, and (if dead-lettered) decrements group counter.
     *
     * @return [GroupFailResult], or `null` if already handled (0 rows — not CLAIMED).
     */
    fun reclaimGroupTask(
        taskId: String,
        groupId: String,
        errorMessage: String,
    ): GroupFailResult? {
        return jdbi.inTransaction<GroupFailResult?, Exception> { h ->
            val updated = h.createUpdate(
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
            )
                .bind("taskId", taskId)
                .bind("error", errorMessage.take(4000))
                .execute()

            if (updated == 0) return@inTransaction null

            val newStatus = h.createQuery("SELECT status FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java)
                .one()
            val deadLettered = newStatus == "DEAD_LETTER"

            val barrierMet = if (deadLettered) resolveGroupCounter(h, groupId, failed = true) else false
            GroupFailResult(taskUpdated = true, deadLettered = deadLettered, barrierMet = barrierMet)
        }
    }

    // ── Private helpers ────────────────────────────────────────────────

    /**
     * Decrement [tasks_pending], conditionally increment [tasks_failed],
     * check barrier, and create callback task if barrier is met.
     *
     * Must be called within an existing transaction (caller provides [Handle]).
     *
     * @return `true` if the barrier was met (tasks_pending reached 0).
     */
    private fun resolveGroupCounter(h: Handle, groupId: String, failed: Boolean): Boolean {
        val failedIncrement = if (failed) 1 else 0
        h.createUpdate(
            """
            UPDATE task_group
            SET tasks_pending = tasks_pending - 1,
                tasks_failed = tasks_failed + :failedIncrement,
                updated_at = CURRENT_TIMESTAMP
            WHERE group_id = :groupId AND status = 'ACTIVE'
            """,
        ).bind("groupId", groupId)
            .bind("failedIncrement", failedIncrement)
            .execute()

        val group = h.createQuery(
            """
            SELECT tasks_pending, tasks_failed, phase_total, on_complete_handler
            FROM task_group WHERE group_id = :groupId
            """,
        ).bind("groupId", groupId)
            .map { rs, _ ->
                object {
                    val tasksPending = rs.getInt("tasks_pending")
                    val tasksFailed = rs.getInt("tasks_failed")
                    val phaseTotal = rs.getInt("phase_total")
                    val onCompleteHandler = rs.getString("on_complete_handler")
                }
            }
            .one()

        val barrierMet = group.tasksPending == 0
        if (barrierMet && group.onCompleteHandler != null) {
            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, 'default', :payload, 'PENDING', 10,
                    NULL, NULL, 0, 3, CURRENT_TIMESTAMP)
                """,
            ).bind("taskId", UUID.randomUUID().toString())
                .bind("handler", group.onCompleteHandler)
                .bind("payload", groupId)
                .execute()

            log.infof(
                "Barrier met for group %s (pending=0, failed=%d, total=%d) — callback task created",
                groupId, group.tasksFailed, group.phaseTotal,
            )
        }

        return barrierMet
    }

    /**
     * Transition to a new phase: CAS version, reset counters, insert new tasks.
     * [tasks_pending] is initialized to [newPhaseTotal], [tasks_failed] reset to 0.
     */
    fun transitionPhase(
        groupId: String,
        expectedVersion: Long,
        newPhase: String,
        newPhaseTotal: Int,
        tasks: List<EnqueueRequest>,
        onCompleteHandler: String?,
    ): Boolean {
        val epoch = optionalEpoch()
        return jdbi.inTransaction<Boolean, Exception> { h ->
            val epochClause = if (epoch != null) " AND last_epoch <= :epoch" else ""
            val epochSet = if (epoch != null) ", last_epoch = :epoch" else ""

            val updated = h.createUpdate(
                """
                UPDATE task_group
                SET phase = :newPhase, phase_total = :newPhaseTotal,
                    tasks_pending = :newPhaseTotal, tasks_failed = 0,
                    on_complete_handler = :onCompleteHandler,
                    version = version + 1$epochSet, updated_at = CURRENT_TIMESTAMP
                WHERE group_id = :groupId AND status = 'ACTIVE'
                  AND version = :expectedVersion$epochClause
                """,
            ).bind("groupId", groupId)
                .bind("newPhase", newPhase)
                .bind("newPhaseTotal", newPhaseTotal)
                .bind("onCompleteHandler", onCompleteHandler)
                .bind("expectedVersion", expectedVersion)
                .apply { if (epoch != null) bind("epoch", epoch) }
                .execute()

            if (updated == 0) return@inTransaction false

            val batch = h.prepareBatch(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', 0,
                    :groupId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
            )
            for (task in tasks) {
                batch
                    .bind("taskId", UUID.randomUUID().toString())
                    .bind("handler", task.handler)
                    .bind("queue", task.queue)
                    .bind("payload", task.payload)
                    .bind("groupId", task.groupId)
                    .bind("metadata", task.metadata)
                    .bind("maxRetries", task.maxRetries)
                    .add()
            }
            batch.execute()

            true
        }
    }

    /**
     * Compare-and-swap for group status transitions.
     * Combines version-based CAS with optional epoch fencing.
     */
    fun casGroupStatus(
        groupId: String,
        expectedStatus: GroupStatus,
        newStatus: GroupStatus,
        expectedVersion: Long,
        resultMetadata: String? = null,
    ): Boolean {
        val epoch = optionalEpoch()
        val updated = jdbi.withHandle<Int, Exception> { h ->
            val epochClause = if (epoch != null) " AND last_epoch <= :epoch" else ""
            val epochSet = if (epoch != null) ", last_epoch = :epoch" else ""
            val metaSet = if (resultMetadata != null) ", result_metadata = :resultMetadata" else ""

            val update = h.createUpdate(
                """
                UPDATE task_group
                SET status = :newStatus, version = version + 1$epochSet$metaSet,
                    updated_at = CURRENT_TIMESTAMP
                WHERE group_id = :groupId AND status = :expectedStatus
                  AND version = :expectedVersion$epochClause
                """,
            ).bind("groupId", groupId)
                .bind("expectedStatus", expectedStatus.name)
                .bind("newStatus", newStatus.name)
                .bind("expectedVersion", expectedVersion)
            if (epoch != null) update.bind("epoch", epoch)
            if (resultMetadata != null) update.bind("resultMetadata", resultMetadata)
            update.execute()
        }
        return updated > 0
    }

    /**
     * Stream output URIs and metadata from completed tasks in a group
     * filtered by handler name (e.g., "wordcount.map").
     */
    fun streamTaskOutputs(groupId: String, handler: String): Flow<TaskOutput> =
        flow {
            val handle = jdbi.open()
            try {
                val stream = handle.createQuery(
                    """
                    SELECT output_uri, output_metadata FROM task
                    WHERE group_id = :groupId AND handler = :handler
                      AND status = 'COMPLETED' AND output_uri IS NOT NULL
                    ORDER BY created_at ASC
                    """,
                ).bind("groupId", groupId)
                    .bind("handler", handler)
                    .map { rs, _ -> TaskOutput(rs.getString("output_uri"), rs.getString("output_metadata")) }
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

    fun findGroup(groupId: String): TaskGroup? =
        jdbi.withHandle<TaskGroup?, Exception> { h ->
            h.createQuery("SELECT * FROM task_group WHERE group_id = :groupId")
                .bind("groupId", groupId)
                .mapTo(TaskGroup::class.java)
                .findOne()
                .orElse(null)
        }

    fun findGroupsByStatus(status: GroupStatus): List<TaskGroup> =
        jdbi.withHandle<List<TaskGroup>, Exception> { h ->
            h.createQuery("SELECT * FROM task_group WHERE status = :status")
                .bind("status", status.name)
                .mapTo(TaskGroup::class.java)
                .list()
        }

    fun findAllGroups(limit: Int = 100): List<TaskGroup> =
        jdbi.withHandle<List<TaskGroup>, Exception> { h ->
            h.createQuery("SELECT * FROM task_group ORDER BY created_at DESC FETCH FIRST :limit ROWS ONLY")
                .bind("limit", limit)
                .mapTo(TaskGroup::class.java)
                .list()
        }
}
