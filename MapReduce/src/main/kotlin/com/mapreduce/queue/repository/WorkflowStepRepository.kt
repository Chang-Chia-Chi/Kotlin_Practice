package com.mapreduce.queue.repository

import com.mapreduce.config.inTransactionSuspend
import com.mapreduce.config.withHandleSuspend
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.StepStatus
import com.mapreduce.queue.model.WorkflowStep
import com.mapreduce.workflow.spi.WorkflowDefinition
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
 * Result of resolving a task within a step.
 * [barrierMet] is true when this was the final task for the current step.
 */
data class StepTaskResolution(
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
 * Result of an atomic step failure operation.
 *
 * @param taskUpdated false if the status guard / claimToken fence rejected the update (zombie)
 * @param deadLettered true if the task was terminally dead-lettered (retries exhausted or unconditional)
 * @param barrierMet true if this was the last pending task in the step
 */
data class StepFailResult(
    val taskUpdated: Boolean,
    val deadLettered: Boolean,
    val barrierMet: Boolean,
)

/**
 * Layer 1 persistence for workflow steps with countdown barrier detection.
 *
 * The core mechanism: when a task reaches a terminal state (success or dead-letter),
 * [tasks_pending] is decremented under a row lock. The worker that drives it to zero
 * atomically inserts a callback task in the same transaction — no polling needed.
 */
@ApplicationScoped
class WorkflowStepRepository(
    private val jdbi: Jdbi,
    private val leaderManager: LeaderManager,
) {

    private val log = Logger.getLogger(WorkflowStepRepository::class.java)

    /**
     * Read the current fencing epoch from [LeaderManager], or null if not leader.
     * Used for optional defense-in-depth epoch guards in leader-only writes.
     */
    private fun optionalEpoch(): Long? =
        if (leaderManager.isActive) leaderManager.token else null

    /**
     * Atomic fan-out: insert workflow_step row + N tasks in one transaction.
     * [tasks_pending] is initialized to the number of tasks.
     */
    fun submitStep(step: WorkflowStep, tasks: List<EnqueueRequest>) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO workflow_step (step_id, workflow_name, run_id, status, params, queue,
                    step_label, step_total, tasks_pending, tasks_failed,
                    on_complete_handler, failure_policy, failure_threshold,
                    result_metadata, version, last_epoch, deadline_at, created_at, updated_at)
                VALUES (:stepId, :workflowName, :runId, :status, :params, :queue,
                    :stepLabel, :stepTotal, :tasksPending, 0,
                    :onCompleteHandler, :failurePolicy, :failureThreshold,
                    :resultMetadata, 0, 0, :deadlineAt, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            ).bind("stepId", step.stepId)
                .bind("workflowName", step.workflowName)
                .bind("runId", step.runId)
                .bind("status", step.status.name)
                .bind("params", step.params)
                .bind("queue", step.queue)
                .bind("stepLabel", step.stepLabel)
                .bind("stepTotal", step.stepTotal)
                .bind("tasksPending", step.stepTotal)
                .bind("onCompleteHandler", step.onCompleteHandler)
                .bind("failurePolicy", step.failurePolicy)
                .bind("failureThreshold", step.failureThreshold)
                .bind("resultMetadata", step.resultMetadata)
                .bind("deadlineAt", step.deadlineAt)
                .execute()

            val batch = h.prepareBatch(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    step_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', :priority,
                    :stepId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
            )
            for (task in tasks) {
                batch
                    .bind("taskId", UUID.randomUUID().toString())
                    .bind("handler", task.handler)
                    .bind("queue", task.queue)
                    .bind("payload", task.payload)
                    .bind("priority", task.priority)
                    .bind("stepId", task.stepId)
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
     * For failed tasks ([failed] = true): only updates the step counters.
     * The caller (TaskDispatcher/StaleTaskReaper) has already marked the task
     * as DEAD_LETTER before calling this.
     *
     * The row lock on workflow_step serializes concurrent resolutions, ensuring
     * exactly one worker observes the barrier condition.
     */
    suspend fun resolveStepTask(
        taskId: String? = null,
        stepId: String,
        claimToken: String? = null,
        failed: Boolean = false,
        outputUri: String? = null,
        outputMetadata: String? = null,
    ): StepTaskResolution {
        return jdbi.inTransactionSuspend<StepTaskResolution, Exception> { h ->
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
                    log.warnf("Zombie detected for task %s (gen=%s) — skipping step counter", taskId, claimToken)
                    return@inTransactionSuspend StepTaskResolution(updated = false, barrierMet = false)
                }
            }

            val barrierMet = resolveStepCounter(h, stepId, failed)
            StepTaskResolution(updated = true, barrierMet = barrierMet)
        }
    }

    // ── Atomic step failure methods ───────────────────────────────────

    /**
     * Atomic fail-with-retry for a step task.
     *
     * In a single transaction: increments retry_count, conditionally moves to
     * DEAD_LETTER (retries exhausted) or PENDING (retries remain). Only when
     * the task is terminally dead-lettered does this decrement the step counter.
     *
     * @return [StepFailResult] — `taskUpdated=false` if fenced out (zombie).
     */
    suspend fun failStepTask(
        taskId: String,
        stepId: String,
        errorMessage: String,
        retryDelay: Duration? = null,
        claimToken: String? = null,
    ): StepFailResult {
        return jdbi.inTransactionSuspend<StepFailResult, Exception> { h ->
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
                return@inTransactionSuspend StepFailResult(taskUpdated = false, deadLettered = false, barrierMet = false)
            }

            val newStatus = h.createQuery("SELECT status FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java)
                .one()
            val deadLettered = newStatus == "DEAD_LETTER"

            val barrierMet = if (deadLettered) resolveStepCounter(h, stepId, failed = true) else false
            StepFailResult(taskUpdated = true, deadLettered = deadLettered, barrierMet = barrierMet)
        }
    }

    /**
     * Atomic unconditional dead-letter for a step task.
     *
     * In a single transaction: marks task DEAD_LETTER (with status guard)
     * and decrements the step counter.
     *
     * Used for no-handler and explicit [TaskResult.DeadLetter] results.
     *
     * @return [StepFailResult] — `taskUpdated=false` if fenced out (zombie).
     */
    suspend fun deadLetterStepTask(
        taskId: String,
        stepId: String,
        reason: String,
        claimToken: String? = null,
    ): StepFailResult {
        return jdbi.inTransactionSuspend<StepFailResult, Exception> { h ->
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
                return@inTransactionSuspend StepFailResult(taskUpdated = false, deadLettered = false, barrierMet = false)
            }

            val barrierMet = resolveStepCounter(h, stepId, failed = true)
            StepFailResult(taskUpdated = true, deadLettered = true, barrierMet = barrierMet)
        }
    }

    /**
     * Atomic reclaim for a step's stale task.
     *
     * In a single transaction: clears claim fields, increments retry_count,
     * conditionally DEAD_LETTER, and (if dead-lettered) decrements step counter.
     *
     * @return [StepFailResult], or `null` if already handled (0 rows — not CLAIMED).
     */
    fun reclaimStepTask(
        taskId: String,
        stepId: String,
        errorMessage: String,
    ): StepFailResult? {
        return jdbi.inTransaction<StepFailResult?, Exception> { h ->
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

            val barrierMet = if (deadLettered) resolveStepCounter(h, stepId, failed = true) else false
            StepFailResult(taskUpdated = true, deadLettered = deadLettered, barrierMet = barrierMet)
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
    private fun resolveStepCounter(h: Handle, stepId: String, failed: Boolean): Boolean {
        val failedIncrement = if (failed) 1 else 0
        h.createUpdate(
            """
            UPDATE workflow_step
            SET tasks_pending = tasks_pending - 1,
                tasks_failed = tasks_failed + :failedIncrement,
                updated_at = CURRENT_TIMESTAMP
            WHERE step_id = :stepId AND status = 'ACTIVE'
            """,
        ).bind("stepId", stepId)
            .bind("failedIncrement", failedIncrement)
            .execute()

        val step = h.createQuery(
            """
            SELECT tasks_pending, tasks_failed, step_total, on_complete_handler
            FROM workflow_step WHERE step_id = :stepId
            """,
        ).bind("stepId", stepId)
            .map { rs, _ ->
                object {
                    val tasksPending = rs.getInt("tasks_pending")
                    val tasksFailed = rs.getInt("tasks_failed")
                    val stepTotal = rs.getInt("step_total")
                    val onCompleteHandler = rs.getString("on_complete_handler")
                }
            }
            .one()

        val barrierMet = step.tasksPending == 0
        if (barrierMet && step.onCompleteHandler != null) {
            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    step_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, 'default', :payload, 'PENDING', 10,
                    NULL, NULL, 0, 3, CURRENT_TIMESTAMP)
                """,
            ).bind("taskId", UUID.randomUUID().toString())
                .bind("handler", step.onCompleteHandler)
                .bind("payload", stepId)
                .execute()

            log.infof(
                "Barrier met for step %s (pending=0, failed=%d, total=%d) — callback task created",
                stepId, step.tasksFailed, step.stepTotal,
            )
        }

        return barrierMet
    }

    // ── Lock-free barrier infrastructure ──────────────────────────────

    /**
     * Lock-free barrier check with atomic callback dispatch.
     *
     * Phase 1 (lock-free): COUNT non-terminal tasks. If > 0, exit.
     * Phase 2 (short lock): SELECT FOR UPDATE the step row, re-verify COUNT,
     *   clear on_complete_handler (prevents duplicates), and INSERT callback task.
     *   At most 1–2 workers reach Phase 2 — no hot-row contention.
     *
     * @return true if this call dispatched the callback task.
     */
    private fun checkAndDispatchBarrier(stepId: String): Boolean {
        // Phase 1: Lock-free peek
        val pendingCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("""
                SELECT COUNT(1) FROM task
                WHERE step_id = :stepId AND status IN ('PENDING', 'CLAIMED')
            """).bind("stepId", stepId).mapTo(Int::class.java).one()
        }
        if (pendingCount > 0) return false

        // Phase 2: Atomic verify-and-dispatch
        return jdbi.inTransaction<Boolean, Exception> { h ->
            val handler = h.createQuery("""
                SELECT on_complete_handler FROM workflow_step
                WHERE step_id = :stepId AND status = 'ACTIVE'
                  AND on_complete_handler IS NOT NULL
                FOR UPDATE
            """).bind("stepId", stepId)
                .mapTo(String::class.java)
                .findOne()
                .orElse(null) ?: return@inTransaction false

            val recheck = h.createQuery("""
                SELECT COUNT(1) FROM task
                WHERE step_id = :stepId AND status IN ('PENDING', 'CLAIMED')
            """).bind("stepId", stepId).mapTo(Int::class.java).one()
            if (recheck > 0) return@inTransaction false

            h.createUpdate("""
                UPDATE workflow_step
                SET on_complete_handler = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE step_id = :stepId
            """).bind("stepId", stepId).execute()

            h.createUpdate("""
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    step_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, 'default', :payload, 'PENDING', 10,
                    NULL, NULL, 0, 3, CURRENT_TIMESTAMP)
            """).bind("taskId", UUID.randomUUID().toString())
                .bind("handler", handler)
                .bind("payload", stepId)
                .execute()

            log.infof("Barrier met for step %s — callback dispatched", stepId)
            true
        }
    }

    /**
     * Public entry point for barrier dispatch. Used by the stuck-step sweeper.
     */
    fun tryDispatchBarrier(stepId: String): Boolean = checkAndDispatchBarrier(stepId)

    /**
     * Count DEAD_LETTER tasks for a step. Used by StepTransitionHandler
     * to evaluate failure policy on demand (instead of maintaining a counter).
     */
    fun countFailedTasks(stepId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("""
                SELECT COUNT(1) FROM task
                WHERE step_id = :stepId AND status = 'DEAD_LETTER'
            """).bind("stepId", stepId).mapTo(Int::class.java).one()
        }

    /**
     * Count PENDING/CLAIMED tasks for a step. Used by API layer
     * to report progress on demand.
     */
    fun countPendingTasks(stepId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("""
                SELECT COUNT(1) FROM task
                WHERE step_id = :stepId AND status IN ('PENDING', 'CLAIMED')
            """).bind("stepId", stepId).mapTo(Int::class.java).one()
        }

    /**
     * Find ACTIVE steps with no PENDING/CLAIMED tasks whose last update
     * is older than [threshold]. These are "stuck" — all tasks reached
     * terminal state but the callback was never dispatched (crash during
     * Phase 2, or callback task itself was dead-lettered).
     */
    fun findStuckSteps(threshold: Instant): List<String> =
        jdbi.withHandle<List<String>, Exception> { h ->
            h.createQuery("""
                SELECT ws.step_id FROM workflow_step ws
                WHERE ws.status = 'ACTIVE'
                  AND ws.on_complete_handler IS NOT NULL
                  AND ws.updated_at < :threshold
                  AND NOT EXISTS (
                      SELECT 1 FROM task t
                      WHERE t.step_id = ws.step_id
                        AND t.status IN ('PENDING', 'CLAIMED')
                  )
            """).bind("threshold", threshold)
                .mapTo(String::class.java)
                .list()
        }

    /**
     * Create a new step row for the next pipeline stage.
     *
     * INSERTs a new workflow_step row (new step_id, same run_id/workflow_name),
     * inserts tasks for the new step, and CAS the previous step to COMPLETED.
     * All in one transaction; version/epoch fencing on the CAS prevents duplicate transitions.
     */
    suspend fun createNextStep(
        previousStepId: String,
        expectedVersion: Long,
        newStep: WorkflowStep,
        tasks: List<EnqueueRequest>,
    ): Boolean {
        val epoch = optionalEpoch()
        return jdbi.inTransactionSuspend<Boolean, Exception> { h ->
            // INSERT new workflow_step row
            h.createUpdate(
                """
                INSERT INTO workflow_step (step_id, workflow_name, run_id, status, params, queue,
                    step_label, step_total, tasks_pending, tasks_failed,
                    on_complete_handler, failure_policy, failure_threshold,
                    result_metadata, version, last_epoch, deadline_at, created_at, updated_at)
                VALUES (:stepId, :workflowName, :runId, 'ACTIVE', :params, :queue,
                    :stepLabel, :stepTotal, :tasksPending, 0,
                    :onCompleteHandler, :failurePolicy, :failureThreshold,
                    NULL, 0, 0, :deadlineAt, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            ).bind("stepId", newStep.stepId)
                .bind("workflowName", newStep.workflowName)
                .bind("runId", newStep.runId)
                .bind("params", newStep.params)
                .bind("queue", newStep.queue)
                .bind("stepLabel", newStep.stepLabel)
                .bind("stepTotal", newStep.stepTotal)
                .bind("tasksPending", newStep.stepTotal)
                .bind("onCompleteHandler", newStep.onCompleteHandler)
                .bind("failurePolicy", newStep.failurePolicy)
                .bind("failureThreshold", newStep.failureThreshold)
                .bind("deadlineAt", newStep.deadlineAt)
                .execute()

            // INSERT tasks for the new step
            val batch = h.prepareBatch(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    step_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', 0,
                    :stepId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
            )
            for (task in tasks) {
                batch
                    .bind("taskId", UUID.randomUUID().toString())
                    .bind("handler", task.handler)
                    .bind("queue", task.queue)
                    .bind("payload", task.payload)
                    .bind("stepId", task.stepId)
                    .bind("metadata", task.metadata)
                    .bind("maxRetries", task.maxRetries)
                    .add()
            }
            batch.execute()

            // CAS previous step to COMPLETED
            val epochClause = if (epoch != null) " AND last_epoch <= :epoch" else ""
            val epochSet = if (epoch != null) ", last_epoch = :epoch" else ""

            val updated = h.createUpdate(
                """
                UPDATE workflow_step
                SET status = 'COMPLETED', version = version + 1$epochSet,
                    updated_at = CURRENT_TIMESTAMP
                WHERE step_id = :stepId AND status = 'ACTIVE'
                  AND version = :expectedVersion$epochClause
                """,
            ).bind("stepId", previousStepId)
                .bind("expectedVersion", expectedVersion)
                .apply { if (epoch != null) bind("epoch", epoch) }
                .execute()

            updated > 0
        }
    }

    /**
     * Compare-and-swap for step status transitions.
     * Combines version-based CAS with optional epoch fencing.
     */
    suspend fun casStepStatus(
        stepId: String,
        expectedStatus: StepStatus,
        newStatus: StepStatus,
        expectedVersion: Long,
        resultMetadata: String? = null,
    ): Boolean {
        val epoch = optionalEpoch()
        val updated = jdbi.withHandleSuspend<Int, Exception> { h ->
            val epochClause = if (epoch != null) " AND last_epoch <= :epoch" else ""
            val epochSet = if (epoch != null) ", last_epoch = :epoch" else ""
            val metaSet = if (resultMetadata != null) ", result_metadata = :resultMetadata" else ""

            val update = h.createUpdate(
                """
                UPDATE workflow_step
                SET status = :newStatus, version = version + 1$epochSet$metaSet,
                    updated_at = CURRENT_TIMESTAMP
                WHERE step_id = :stepId AND status = :expectedStatus
                  AND version = :expectedVersion$epochClause
                """,
            ).bind("stepId", stepId)
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
     * Atomically CAS step to FAILED and enqueue a compensation task.
     *
     * Both operations happen in one transaction — if the process crashes
     * after CAS but before compensation insert, the compensation is not lost.
     * The compensation handler receives the step_id as payload, enabling it
     * to query failed task details and perform rollback.
     *
     * @return true if the CAS succeeded (step was ACTIVE at expected version).
     */
    suspend fun failStepWithCompensation(
        stepId: String,
        expectedVersion: Long,
        compensationHandler: String,
        queue: String,
    ): Boolean {
        val epoch = optionalEpoch()
        return jdbi.inTransactionSuspend<Boolean, Exception> { h ->
            val epochClause = if (epoch != null) " AND last_epoch <= :epoch" else ""
            val epochSet = if (epoch != null) ", last_epoch = :epoch" else ""

            val updated = h.createUpdate(
                """
                UPDATE workflow_step
                SET status = 'FAILED', version = version + 1$epochSet,
                    updated_at = CURRENT_TIMESTAMP
                WHERE step_id = :stepId AND status = 'ACTIVE'
                  AND version = :expectedVersion$epochClause
                """,
            ).bind("stepId", stepId)
                .bind("expectedVersion", expectedVersion)
                .apply { if (epoch != null) bind("epoch", epoch) }
                .execute()

            if (updated == 0) return@inTransactionSuspend false

            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    step_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, :queue, :payload, 'PENDING', 10,
                    NULL, NULL, 0, 3, CURRENT_TIMESTAMP)
                """,
            ).bind("taskId", UUID.randomUUID().toString())
                .bind("handler", compensationHandler)
                .bind("queue", queue)
                .bind("payload", stepId)
                .execute()

            true
        }
    }

    /**
     * Stream output URIs and metadata from completed tasks in a step
     * filtered by handler name.
     */
    fun streamTaskOutputs(stepId: String, handler: String): Flow<TaskOutput> =
        flow {
            jdbi.open().use { handle ->
                handle.createQuery(
                    """
                    SELECT output_uri, output_metadata FROM task
                    WHERE step_id = :stepId AND handler = :handler
                      AND status = 'COMPLETED' AND output_uri IS NOT NULL
                    ORDER BY created_at ASC
                    """,
                ).bind("stepId", stepId)
                    .bind("handler", handler)
                    .map { rs, _ -> TaskOutput(rs.getString("output_uri"), rs.getString("output_metadata")) }
                    .stream()
                    .use { s ->
                        val iter = s.iterator()
                        while (iter.hasNext()) {
                            emit(iter.next())
                        }
                    }
            }
        }.flowOn(Dispatchers.IO)

    suspend fun findStep(stepId: String): WorkflowStep? =
        jdbi.withHandleSuspend<WorkflowStep?, Exception> { h ->
            h.createQuery("SELECT * FROM workflow_step WHERE step_id = :stepId")
                .bind("stepId", stepId)
                .mapTo(WorkflowStep::class.java)
                .findOne()
                .orElse(null)
        }

    fun findStepsByStatus(status: StepStatus): List<WorkflowStep> =
        jdbi.withHandle<List<WorkflowStep>, Exception> { h ->
            h.createQuery("SELECT * FROM workflow_step WHERE status = :status")
                .bind("status", status.name)
                .mapTo(WorkflowStep::class.java)
                .list()
        }

    fun findStepsByRunId(runId: String): List<WorkflowStep> =
        jdbi.withHandle<List<WorkflowStep>, Exception> { h ->
            h.createQuery("SELECT * FROM workflow_step WHERE run_id = :runId ORDER BY created_at")
                .bind("runId", runId)
                .mapTo(WorkflowStep::class.java)
                .list()
        }

    fun findAllSteps(limit: Int = 100): List<WorkflowStep> =
        jdbi.withHandle<List<WorkflowStep>, Exception> { h ->
            h.createQuery("SELECT * FROM workflow_step ORDER BY created_at DESC FETCH FIRST :limit ROWS ONLY")
                .bind("limit", limit)
                .mapTo(WorkflowStep::class.java)
                .list()
        }

    /**
     * Bulk-fail ACTIVE steps whose [deadline_at] has passed.
     * Returns the number of steps transitioned to FAILED.
     */
    fun failExpiredSteps(now: Instant): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createUpdate(
                """
                UPDATE workflow_step
                SET status = 'FAILED', result_metadata = 'Step deadline exceeded',
                    version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE status = 'ACTIVE'
                  AND deadline_at IS NOT NULL
                  AND deadline_at <= :now
                """,
            ).bind("now", now)
                .execute()
        }
}
