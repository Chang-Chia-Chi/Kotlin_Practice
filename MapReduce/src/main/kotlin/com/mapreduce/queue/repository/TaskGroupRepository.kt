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
import org.jdbi.v3.core.Jdbi
import org.jboss.logging.Logger
import java.util.UUID

/**
 * Result of completing a task within a group.
 * [barrierMet] is true when this was the final task for the current phase.
 */
data class GroupTaskCompletionResult(
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
 * Layer 1 persistence for task groups with reactive barrier detection.
 *
 * The core mechanism: when a task completes, the group counter is incremented
 * under a row lock. The worker that sees the counter reach [phaseTotal]
 * atomically inserts a callback task in the same transaction — no polling needed.
 */
@ApplicationScoped
class TaskGroupRepository(
    jdbi: Jdbi,
) : FencedRepository(jdbi) {

    private val log = Logger.getLogger(TaskGroupRepository::class.java)

    /**
     * Atomic fan-out: insert task_group row + N tasks in one transaction.
     * Not a leader-only write — called from the REST endpoint.
     */
    fun submitGroup(group: TaskGroup, tasks: List<EnqueueRequest>) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO task_group (group_id, group_type, status, params, queue,
                    phase, phase_total, phase_completed, phase_failed,
                    on_complete_handler, failure_policy, failure_threshold,
                    result_metadata, version, last_epoch, created_at, updated_at)
                VALUES (:groupId, :groupType, :status, :params, :queue,
                    :phase, :phaseTotal, 0, 0,
                    :onCompleteHandler, :failurePolicy, :failureThreshold,
                    :resultMetadata, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            ).bind("groupId", group.groupId)
                .bind("groupType", group.groupType)
                .bind("status", group.status.name)
                .bind("params", group.params)
                .bind("queue", group.queue)
                .bind("phase", group.phase)
                .bind("phaseTotal", group.phaseTotal)
                .bind("onCompleteHandler", group.onCompleteHandler)
                .bind("failurePolicy", group.failurePolicy.name)
                .bind("failureThreshold", group.failureThreshold)
                .bind("resultMetadata", group.resultMetadata)
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
     * Atomically: mark task COMPLETED, increment group counter, check barrier,
     * and create callback task if barrier is met — all in one Oracle transaction.
     *
     * The row lock on task_group serializes concurrent completions, ensuring
     * exactly one worker observes the barrier condition.
     */
    fun completeGroupTask(
        taskId: String,
        groupId: String,
        executionGeneration: String?,
        outputUri: String?,
        outputMetadata: String?,
    ): GroupTaskCompletionResult {
        return jdbi.inTransaction<GroupTaskCompletionResult, Exception> { h ->
            // Step 1: Mark task COMPLETED with output fields
            val fenceClause = if (executionGeneration != null) " AND execution_generation = :gen" else ""
            val update = h.createUpdate(
                """
                UPDATE task SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP,
                    output_uri = :outputUri, output_metadata = :outputMeta
                WHERE task_id = :taskId AND status = 'CLAIMED'$fenceClause
                """,
            ).bind("taskId", taskId)
                .bind("outputUri", outputUri)
                .bind("outputMeta", outputMetadata)
            if (executionGeneration != null) update.bind("gen", executionGeneration)
            val updated = update.execute()

            if (updated == 0) {
                log.warnf("Zombie detected for task %s (gen=%s) — skipping group counter", taskId, executionGeneration)
                return@inTransaction GroupTaskCompletionResult(updated = false, barrierMet = false)
            }

            // Step 2: Increment phase_completed (row lock serializes concurrent completions)
            h.createUpdate(
                """
                UPDATE task_group SET phase_completed = phase_completed + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE group_id = :groupId AND status = 'ACTIVE'
                """,
            ).bind("groupId", groupId)
                .execute()

            // Step 3: Read counters + callback handler
            val group = h.createQuery(
                """
                SELECT phase_completed, phase_failed, phase_total, on_complete_handler
                FROM task_group WHERE group_id = :groupId
                """,
            ).bind("groupId", groupId)
                .map { rs, _ ->
                    object {
                        val phaseCompleted = rs.getInt("phase_completed")
                        val phaseFailed = rs.getInt("phase_failed")
                        val phaseTotal = rs.getInt("phase_total")
                        val onCompleteHandler = rs.getString("on_complete_handler")
                    }
                }
                .one()

            // Step 4: Check barrier — create callback task if met
            val barrierMet = group.phaseCompleted + group.phaseFailed >= group.phaseTotal
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

                log.infof("Barrier met for group %s (completed=%d, failed=%d, total=%d) — callback task created",
                    groupId, group.phaseCompleted, group.phaseFailed, group.phaseTotal)
            }

            GroupTaskCompletionResult(updated = true, barrierMet = barrierMet)
        }
    }

    /**
     * Increment phase_failed, check barrier + failure policy, create callback task
     * if barrier is met. Called when a task is dead-lettered.
     */
    fun recordGroupTaskFailure(groupId: String) {
        jdbi.useTransaction<Exception> { h ->
            // Increment phase_failed
            h.createUpdate(
                """
                UPDATE task_group SET phase_failed = phase_failed + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE group_id = :groupId AND status = 'ACTIVE'
                """,
            ).bind("groupId", groupId)
                .execute()

            // Read counters + callback handler
            val group = h.createQuery(
                """
                SELECT phase_completed, phase_failed, phase_total, on_complete_handler
                FROM task_group WHERE group_id = :groupId
                """,
            ).bind("groupId", groupId)
                .map { rs, _ ->
                    object {
                        val phaseCompleted = rs.getInt("phase_completed")
                        val phaseFailed = rs.getInt("phase_failed")
                        val phaseTotal = rs.getInt("phase_total")
                        val onCompleteHandler = rs.getString("on_complete_handler")
                    }
                }
                .one()

            // Check barrier — create callback task if met
            val barrierMet = group.phaseCompleted + group.phaseFailed >= group.phaseTotal
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

                log.infof("Barrier met (via failure) for group %s (completed=%d, failed=%d, total=%d) — callback task created",
                    groupId, group.phaseCompleted, group.phaseFailed, group.phaseTotal)
            }
        }
    }

    /**
     * Transition to a new phase: CAS version, reset counters, insert new tasks.
     * All in one transaction for atomicity.
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
                    phase_completed = 0, phase_failed = 0,
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
