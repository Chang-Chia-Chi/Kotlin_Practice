package com.workflow.workflow.adapter.persistent

import com.workflow.infrastructure.persistence.caseInsensitive
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.persistence.readClob
import com.workflow.infrastructure.persistence.readNullableTimestamp
import com.workflow.infrastructure.persistence.readTimestamp
import com.workflow.infrastructure.persistence.withHandleSuspend
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.sql.Types
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@ApplicationScoped
class JdbiTaskRepository(
    private val jdbi: Jdbi,
) : TaskRepository {
    // -- Suspend methods (open own connection) --

    override suspend fun insertBatch(tasks: List<Task>) {
        jdbi.inTransactionSuspend<Unit, Exception> { h: Handle -> insertBatchWithHandle(h, tasks) }
    }

    override suspend fun claimNext(
        workerId: String,
        limit: Int,
        queueName: String,
    ): List<Task> =
        jdbi.inTransactionSuspend<List<Task>, Exception> { h: Handle ->
            val now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS)
            val rows =
                h
                    .createQuery(
                        """
                SELECT * FROM task
                WHERE id IN (
                    SELECT id FROM task
                    WHERE status = 'PENDING'
                      AND queue_name = :queueName
                      AND (deadline_at IS NULL OR deadline_at > :now)
                      AND (not_before IS NULL OR not_before <= :now)
                    ORDER BY enqueued_at ASC, id
                    FETCH FIRST :limit ROWS ONLY
                )
                FOR UPDATE SKIP LOCKED
                """,
                    ).bind("limit", limit)
                    .bind("now", now)
                    .bind("queueName", queueName)
                    .mapToMap()
                    .list()

            if (rows.isEmpty()) return@inTransactionSuspend emptyList()

            val ids = rows.map { caseInsensitive(it)["ID"] as String }
            h
                .createUpdate(
                    """
                UPDATE task SET status = 'PROCESSING', claimed_by = :workerId, claimed_at = SYSTIMESTAMP,
                    stale_at = SYSTIMESTAMP + NUMTODSINTERVAL(stale_threshold_secs, 'SECOND')
                WHERE id IN (<ids>)
                """,
                ).bind("workerId", workerId)
                .bindList("ids", ids)
                .execute()

            // Re-read claimed rows to get exact DB-assigned claimed_at for fencing
            h
                .createQuery("SELECT * FROM task WHERE id IN (<ids>) ORDER BY enqueued_at ASC, id")
                .bindList("ids", ids)
                .mapToMap()
                .list()
                .map(::mapTaskRow)
        }

    override suspend fun findByWorkflowAndSequence(
        workflowId: String,
        sequenceNumber: Int,
    ): List<Task> =
        jdbi.withHandleSuspend<List<Task>, Exception> { h: Handle ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE workflow_id = :workflowId AND sequence_number = :seq",
                ).bind("workflowId", workflowId)
                .bind("seq", sequenceNumber)
                .mapToMap()
                .list()
                .map(::mapTaskRow)
        }

    override suspend fun resetForRetry(
        id: String,
        newRetryCount: Int,
        claimedBy: String?,
        claimedAt: java.time.Instant?,
    ): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            val update = h.createUpdate(
                """
                UPDATE task
                SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, stale_at = NULL,
                    retry_count = :newRetryCount,
                    not_before = :now + NUMTODSINTERVAL(LEAST(backoff_base * POWER(2, :newRetryCount), backoff_cap), 'SECOND')
                WHERE id = :id
                  AND status IN ('PROCESSING', 'DEFERRED')
                  AND (:claimedBy IS NULL OR (claimed_by = :claimedBy AND claimed_at = :claimedAt))
                """,
            ).bind("id", id)
                .bind("newRetryCount", newRetryCount)
                .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
            if (claimedBy != null) update.bind("claimedBy", claimedBy) else update.bindNull("claimedBy", Types.VARCHAR)
            if (claimedAt != null) {
                update.bind("claimedAt", LocalDateTime.ofInstant(claimedAt, ZoneOffset.UTC))
            } else {
                update.bindNull("claimedAt", Types.TIMESTAMP)
            }
            update.execute() > 0
        }

    override suspend fun replayDeadLetterTask(taskId: String): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            val count = h
                .createUpdate(
                    """
                UPDATE task
                SET status = 'PENDING', retry_count = 0,
                    claimed_by = NULL, claimed_at = NULL, stale_at = NULL,
                    completed_at = NULL, result = NULL, not_before = NULL
                WHERE id = :taskId AND status = 'DEAD_LETTER'
                """,
                ).bind("taskId", taskId)
                .execute()
            count > 0
        }

    override suspend fun replayDeadLetterBatch(workflowId: String): Int =
        jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
            replayDeadLetterBatchWithHandle(h, workflowId)
        }

    override suspend fun findExpired(now: Instant): List<Task> =
        jdbi.withHandleSuspend<List<Task>, Exception> { h: Handle ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE status = 'PROCESSING' AND deadline_at < :now",
                ).bind("now", LocalDateTime.ofInstant(now, ZoneOffset.UTC))
                .mapToMap()
                .list()
                .map(::mapTaskRow)
        }

    override suspend fun resetStaleTasks(now: Instant): Int =
        jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
            h
                .createUpdate(
                    """
                UPDATE task
                SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, stale_at = NULL,
                    retry_count = retry_count + 1,
                    not_before = :now + NUMTODSINTERVAL(LEAST(backoff_base * POWER(2, retry_count + 1), backoff_cap), 'SECOND')
                WHERE status = 'PROCESSING' AND stale_at < :now AND retry_count < max_retries
                """,
                ).bind("now", LocalDateTime.ofInstant(now, ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
                .execute()
        }

    override suspend fun deadLetterExhaustedTasks(now: Instant): Int =
        jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
            h
                .createUpdate(
                    """
                UPDATE task SET status = 'DEAD_LETTER', completed_at = :now
                WHERE status = 'PROCESSING' AND stale_at < :now AND retry_count >= max_retries
                """,
                ).bind("now", LocalDateTime.ofInstant(now, ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
                .execute()
        }

    override suspend fun defer(taskId: String, triggerType: String, triggerMeta: String): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            val update = h.createUpdate(
                """
                UPDATE task SET status = 'DEFERRED', trigger_type = :triggerType, trigger_meta = :triggerMeta
                WHERE id = :taskId AND status = 'PROCESSING'
                """,
            ).bind("taskId", taskId)
                .bind("triggerType", triggerType)
            bindNullableClob(update, "triggerMeta", triggerMeta)
            val count = update.execute()
            count > 0
        }

    override suspend fun findDeferred(): List<DeferredTaskRef> =
        jdbi.withHandleSuspend<List<DeferredTaskRef>, Exception> { h: Handle ->
            h.createQuery(
                """
                SELECT id, workflow_id, sequence_number, trigger_type, trigger_meta,
                       deadline_at, retry_count, max_retries
                FROM task WHERE status = 'DEFERRED'
                """,
            ).mapToMap()
                .list()
                .map { row ->
                    val ci = caseInsensitive(row)
                    DeferredTaskRef(
                        taskId = ci["ID"] as String,
                        workflowId = ci["WORKFLOW_ID"] as String,
                        sequenceNumber = (ci["SEQUENCE_NUMBER"] as Number).toInt(),
                        triggerType = ci["TRIGGER_TYPE"] as String,
                        triggerMeta = ci["TRIGGER_META"]?.let { readClob(it) }
                            ?: error("DEFERRED task ${ci["ID"]} has null trigger_meta — data integrity violation"),
                        deadlineAt = readNullableTimestamp(ci["DEADLINE_AT"]),
                        retryCount = (ci["RETRY_COUNT"] as Number).toInt(),
                        maxRetries = (ci["MAX_RETRIES"] as Number).toInt(),
                    )
                }
        }

    // -- Handle methods (for barrier transaction) --

    override fun updateStatusWithHandle(
        handle: Handle,
        id: String,
        newStatus: TaskStatus,
        resultJson: String?,
        claimedBy: String?,
        claimedAt: Instant?,
        fanOutPayloadsJson: String?,
    ): Boolean {
        val count =
            if (newStatus.isTerminal) {
                val update = handle
                    .createUpdate(
                        """
                UPDATE task SET status = :status, result = :result, fan_out_payloads = :fanOutPayloads, completed_at = :now
                WHERE id = :id
                  AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
                  AND (claimed_by = :claimedBy AND claimed_at = :claimedAt OR :claimedBy IS NULL)
                """,
                    ).bind("id", id)
                    .bind("status", newStatus.name)
                    .bind("now", LocalDateTime.now(ZoneOffset.UTC))
                    .let { if (claimedBy != null) it.bind("claimedBy", claimedBy) else it.bindNull("claimedBy", Types.VARCHAR) }
                    .let {
                        if (claimedAt != null) {
                            it.bind("claimedAt", LocalDateTime.ofInstant(claimedAt, ZoneOffset.UTC))
                        } else {
                            it.bindNull("claimedAt", Types.TIMESTAMP)
                        }
                    }
                if (resultJson != null) update.bind("result", resultJson) else update.bindNull("result", Types.CLOB)
                bindNullableClob(update, "fanOutPayloads", fanOutPayloadsJson)
                update.execute()
            } else {
                handle
                    .createUpdate(
                        """
                UPDATE task SET status = :status, result = :result
                WHERE id = :id AND status = 'PROCESSING'
                """,
                    ).bind("id", id)
                    .bind("status", newStatus.name)
                    .let { if (resultJson != null) it.bind("result", resultJson) else it.bindNull("result", Types.CLOB) }
                    .execute()
            }
        return count > 0
    }

    override fun countNonTerminalWithHandle(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        handle
            .createQuery(
                """
            SELECT COUNT(*) FROM task
            WHERE workflow_id = :workflowId AND sequence_number = :seq
              AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
            """,
            ).bind("workflowId", workflowId)
            .bind("seq", sequenceNumber)
            .mapTo(Int::class.java)
            .one()

    override fun countTotalBySequenceWithHandle(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        handle
            .createQuery(
                """
            SELECT COUNT(*) FROM task
            WHERE workflow_id = :workflowId AND sequence_number = :seq
            """,
            ).bind("workflowId", workflowId)
            .bind("seq", sequenceNumber)
            .mapTo(Int::class.java)
            .one()

    // Cancels PENDING and DEFERRED tasks for a workflow.
    override fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int {
        return handle.createUpdate(
            """
            UPDATE task SET status = 'CANCELLED', completed_at = :now
            WHERE workflow_id = :workflowId AND status IN ('PENDING', 'DEFERRED')
            """,
        )
            .bind("workflowId", workflowId)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
            .execute()
    }

    override fun insertBatchWithHandle(handle: Handle, tasks: List<Task>) {
        if (tasks.isEmpty()) return
        val batch = handle.prepareBatch(
            """
            INSERT INTO task (id, workflow_id, activity_name, sequence_number, status, handler_key,
                              task_payload, result, fan_out_payloads, claimed_by, claimed_at, completed_at,
                              retry_count, max_retries, deadline_at, not_before, backoff_base, backoff_cap, queue_name,
                              trigger_type, trigger_meta, stale_threshold_secs)
            VALUES (:id, :workflowId, :activityName, :sequenceNumber, :status, :handlerKey,
                    :taskPayload, :result, :fanOutPayloads, :claimedBy, :claimedAt, :completedAt,
                    :retryCount, :maxRetries, :deadlineAt, :notBefore, :backoffBase, :backoffCap, :queueName,
                    :triggerType, :triggerMeta, :staleThresholdSecs)
            """,
        )
        for (task in tasks) {
            batch
                .bind("id", task.id)
                .bind("workflowId", task.workflowId)
                .bind("activityName", task.activityName)
                .bind("sequenceNumber", task.sequenceNumber)
                .bind("status", task.status.name)
                .bind("handlerKey", task.handlerKey)
            bindNullableClob(batch, "taskPayload", task.taskPayload)
            bindNullableClob(batch, "result", task.resultJson)
            bindNullableClob(batch, "fanOutPayloads", task.fanOutPayloadsJson)
            batch.bind("claimedBy", task.claimedBy)
            bindNullableTimestamp(batch, "claimedAt", task.claimedAt)
            bindNullableTimestamp(batch, "completedAt", task.completedAt)
            batch
                .bind("retryCount", task.retryCount)
                .bind("maxRetries", task.maxRetries)
            bindNullableTimestamp(batch, "deadlineAt", task.deadlineAt)
            bindNullableTimestamp(batch, "notBefore", task.notBefore)
            batch
                .bind("backoffBase", task.backoffBase)
                .bind("backoffCap", task.backoffCap)
                .bind("queueName", task.queueName)
            if (task.triggerType != null) batch.bind("triggerType", task.triggerType) else batch.bindNull("triggerType", Types.VARCHAR)
            bindNullableClob(batch, "triggerMeta", task.triggerMeta)
            batch.bind("staleThresholdSecs", task.staleThresholdSecs)
            batch.add()
        }
        batch.execute()
    }

    override fun replayDeadLetterBatchWithHandle(handle: Handle, workflowId: String): Int =
        handle
            .createUpdate(
                """
            UPDATE task
            SET status = 'PENDING', retry_count = 0,
                claimed_by = NULL, claimed_at = NULL, stale_at = NULL,
                completed_at = NULL, result = NULL, not_before = NULL
            WHERE workflow_id = :workflowId AND status IN ('DEAD_LETTER', 'FAILED')
            """,
            ).bind("workflowId", workflowId)
            .execute()

    override fun countAllNonTerminalWithHandle(handle: Handle, workflowId: String): Int =
        handle
            .createQuery(
                """
            SELECT COUNT(*) FROM task
            WHERE workflow_id = :workflowId
              AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
            """,
            ).bind("workflowId", workflowId)
            .mapTo(Int::class.java)
            .one()

    override fun findDistinctQueuesByWorkflowId(handle: Handle, workflowId: String, statuses: List<String>): List<String> =
        handle
            .createQuery(
                """
            SELECT DISTINCT queue_name FROM task
            WHERE workflow_id = :workflowId AND status IN (<statuses>)
            """,
            ).bind("workflowId", workflowId)
            .bindList("statuses", statuses)
            .mapTo(String::class.java)
            .list()

    override fun countStatusSummariesByWorkflowWithHandle(handle: Handle, workflowId: String): Map<Int, TaskStatusCounts> =
        handle
            .createQuery(
                """
            SELECT sequence_number,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
                   SUM(CASE WHEN status NOT IN ('COMPLETED','FAILED','TIMED_OUT','DEAD_LETTER','CANCELLED','SKIPPED') THEN 1 ELSE 0 END) AS non_terminal,
                   SUM(CASE WHEN status IN ('FAILED','TIMED_OUT','DEAD_LETTER') THEN 1 ELSE 0 END) AS failed
            FROM task
            WHERE workflow_id = :workflowId
            GROUP BY sequence_number
            """,
            ).bind("workflowId", workflowId)
            .mapToMap()
            .list()
            .associate { rawRow ->
                val row = caseInsensitive(rawRow)
                val seq = (row["SEQUENCE_NUMBER"] as Number).toInt()
                seq to TaskStatusCounts(
                    total = (row["TOTAL"] as Number).toInt(),
                    completed = (row["COMPLETED"] as Number).toInt(),
                    nonTerminal = (row["NON_TERMINAL"] as Number).toInt(),
                    failed = (row["FAILED"] as Number).toInt(),
                )
            }

    override fun findByWorkflowIdWithHandle(handle: Handle, workflowId: String): List<Task> =
        handle
            .createQuery("SELECT * FROM task WHERE workflow_id = :workflowId")
            .bind("workflowId", workflowId)
            .mapToMap()
            .list()
            .map(::mapTaskRow)

    // Cancels PENDING and DEFERRED tasks. PROCESSING tasks are left alone —
    // they will be handled by subsequent expireOverdueTasks/reclaimStaleTasks sweeps.
    override fun cancelTasksForOverdueWorkflowsWithHandle(handle: Handle, now: LocalDateTime): Int =
        handle.createUpdate(
            """
            UPDATE task SET status = 'CANCELLED', completed_at = :now
            WHERE status IN ('PENDING', 'DEFERRED')
              AND workflow_id IN (
                SELECT id FROM workflow WHERE status = 'RUNNING' AND deadline_at < :now
              )
            """,
        ).bind("now", now).execute()

    // -- Private helpers --

    private fun bindNullableClob(
        stmt: org.jdbi.v3.core.statement.SqlStatement<*>,
        name: String,
        value: String?,
    ) {
        if (value != null) {
            stmt.bind(name, value)
        } else {
            stmt.bindNull(name, Types.CLOB)
        }
    }

    private fun bindNullableTimestamp(
        stmt: org.jdbi.v3.core.statement.SqlStatement<*>,
        name: String,
        value: Instant?,
    ) {
        if (value != null) {
            stmt.bind(name, LocalDateTime.ofInstant(value, ZoneOffset.UTC))
        } else {
            stmt.bindNull(name, java.sql.Types.TIMESTAMP)
        }
    }

    private fun mapTaskRow(row: Map<String, Any?>): Task {
        val ci = caseInsensitive(row)
        return Task(
            id = ci["ID"] as String,
            workflowId = ci["WORKFLOW_ID"] as String,
            activityName = (ci["ACTIVITY_NAME"] as? String) ?: "",
            sequenceNumber = (ci["SEQUENCE_NUMBER"] as Number).toInt(),
            status = TaskStatus.valueOf(ci["STATUS"] as String),
            handlerKey = ci["HANDLER_KEY"] as String,
            taskPayload = ci["TASK_PAYLOAD"]?.let { readClob(it) },
            resultJson = ci["RESULT"]?.let { readClob(it) },
            claimedBy = ci["CLAIMED_BY"] as String?,
            claimedAt = readNullableTimestamp(ci["CLAIMED_AT"]),
            completedAt = readNullableTimestamp(ci["COMPLETED_AT"]),
            retryCount = (ci["RETRY_COUNT"] as Number).toInt(),
            maxRetries = (ci["MAX_RETRIES"] as Number).toInt(),
            deadlineAt = readNullableTimestamp(ci["DEADLINE_AT"]),
            notBefore = readNullableTimestamp(ci["NOT_BEFORE"]),
            backoffBase = (ci["BACKOFF_BASE"] as Number).toInt(),
            backoffCap = (ci["BACKOFF_CAP"] as Number).toInt(),
            enqueuedAt = readTimestamp(ci["ENQUEUED_AT"]),
            queueName = (ci["QUEUE_NAME"] as? String) ?: "default",
            triggerType = ci["TRIGGER_TYPE"] as? String,
            triggerMeta = ci["TRIGGER_META"]?.let { readClob(it) },
            fanOutPayloadsJson = ci["FAN_OUT_PAYLOADS"]?.let { readClob(it) },
            staleThresholdSecs = (ci["STALE_THRESHOLD_SECS"] as Number).toInt(),
            staleAt = readNullableTimestamp(ci["STALE_AT"]),
        )
    }
}
