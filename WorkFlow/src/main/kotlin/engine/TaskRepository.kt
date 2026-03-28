package com.workflow.engine

import com.workflow.extension.inTransactionSuspend
import com.workflow.extension.withHandleSuspend
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.sql.Types
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@ApplicationScoped
class TaskRepository(
    private val jdbi: Jdbi,
) {
    // ── Suspend methods (open own connection) ──

    suspend fun insertBatch(tasks: List<Task>) {
        jdbi.inTransactionSuspend<Unit, Exception> { h: Handle -> insertBatchWithHandle(h, tasks) }
    }

    suspend fun claimNext(
        workerId: String,
        limit: Int,
        queueName: String = "default",
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
                UPDATE task SET status = 'PROCESSING', claimed_by = :workerId, claimed_at = SYSTIMESTAMP
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

    suspend fun updateStatus(
        id: String,
        newStatus: TaskStatus,
        resultJson: String? = null,
    ): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            updateStatusWithHandle(h, id, newStatus, resultJson)
        }

    suspend fun countNonTerminal(
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        jdbi.withHandleSuspend<Int, Exception> { h: Handle ->
            countNonTerminalWithHandle(h, workflowId, sequenceNumber)
        }

    suspend fun countFailed(
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        jdbi.withHandleSuspend<Int, Exception> { h: Handle ->
            countFailedWithHandle(h, workflowId, sequenceNumber)
        }

    suspend fun countTotal(
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        jdbi.withHandleSuspend<Int, Exception> { h: Handle ->
            countTotalWithHandle(h, workflowId, sequenceNumber)
        }

    suspend fun findByWorkflowAndSequence(
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

    suspend fun resetForRetry(
        id: String,
        newRetryCount: Int,
    ) {
        jdbi.inTransactionSuspend<Unit, Exception> { h: Handle ->
            h
                .createUpdate(
                    """
                UPDATE task
                SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
                    retry_count = :newRetryCount,
                    not_before = :now + NUMTODSINTERVAL(LEAST(backoff_base * POWER(2, :newRetryCount), backoff_cap), 'SECOND')
                WHERE id = :id
                """,
                ).bind("id", id)
                .bind("newRetryCount", newRetryCount)
                .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
                .execute()
        }
    }

    suspend fun replayDeadLetterTask(taskId: String): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            val count = h
                .createUpdate(
                    """
                UPDATE task
                SET status = 'PENDING', retry_count = 0,
                    claimed_by = NULL, claimed_at = NULL,
                    completed_at = NULL, result = NULL, not_before = NULL
                WHERE id = :taskId AND status = 'DEAD_LETTER'
                """,
                ).bind("taskId", taskId)
                .execute()
            count > 0
        }

    suspend fun replayDeadLetterBatch(workflowId: String): Int =
        jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
            replayDeadLetterBatchWithHandle(h, workflowId)
        }

    suspend fun findExpired(now: Instant): List<Task> =
        jdbi.withHandleSuspend<List<Task>, Exception> { h: Handle ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE status = 'PROCESSING' AND deadline_at < :now",
                ).bind("now", LocalDateTime.ofInstant(now, ZoneOffset.UTC))
                .mapToMap()
                .list()
                .map(::mapTaskRow)
        }

    suspend fun resetStaleTasks(staleThreshold: Instant): Int =
        jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
            h
                .createUpdate(
                    """
                UPDATE task
                SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
                    retry_count = retry_count + 1,
                    not_before = :now + NUMTODSINTERVAL(LEAST(backoff_base * POWER(2, retry_count + 1), backoff_cap), 'SECOND')
                WHERE status = 'PROCESSING' AND claimed_at < :threshold AND retry_count < max_retries
                """,
                ).bind("threshold", LocalDateTime.ofInstant(staleThreshold, ZoneOffset.UTC))
                .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
                .execute()
        }

    suspend fun deadLetterExhaustedTasks(staleThreshold: Instant): Int =
        jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
            h
                .createUpdate(
                    """
                UPDATE task SET status = 'DEAD_LETTER', completed_at = :now
                WHERE status = 'PROCESSING' AND claimed_at < :threshold AND retry_count >= max_retries
                """,
                ).bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
                .bind("threshold", LocalDateTime.ofInstant(staleThreshold, ZoneOffset.UTC))
                .execute()
        }

    // ── Handle methods (for barrier transaction) ──

    fun updateStatusWithHandle(
        handle: Handle,
        id: String,
        newStatus: TaskStatus,
        resultJson: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
    ): Boolean {
        val count =
            if (newStatus.isTerminal) {
                handle
                    .createUpdate(
                        """
                UPDATE task SET status = :status, result = :result, completed_at = :now
                WHERE id = :id
                  AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED')
                  AND (claimed_by = :claimedBy AND claimed_at = :claimedAt OR :claimedBy IS NULL)
                """,
                    ).bind("id", id)
                    .bind("status", newStatus.name)
                    .let { if (resultJson != null) it.bind("result", resultJson) else it.bindNull("result", Types.CLOB) }
                    .bind("now", LocalDateTime.now(ZoneOffset.UTC))
                    .let { if (claimedBy != null) it.bind("claimedBy", claimedBy) else it.bindNull("claimedBy", Types.VARCHAR) }
                    .let {
                        if (claimedAt != null) {
                            it.bind("claimedAt", LocalDateTime.ofInstant(claimedAt, ZoneOffset.UTC))
                        } else {
                            it.bindNull("claimedAt", Types.TIMESTAMP)
                        }
                    }.execute()
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

    fun countNonTerminalWithHandle(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        handle
            .createQuery(
                """
            SELECT COUNT(*) FROM task
            WHERE workflow_id = :workflowId AND sequence_number = :seq
              AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED')
            """,
            ).bind("workflowId", workflowId)
            .bind("seq", sequenceNumber)
            .mapTo(Int::class.java)
            .one()

    fun countFailedWithHandle(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        handle
            .createQuery(
                """
            SELECT COUNT(*) FROM task
            WHERE workflow_id = :workflowId AND sequence_number = :seq
              AND status IN ('FAILED', 'TIMED_OUT', 'DEAD_LETTER')
            """,
            ).bind("workflowId", workflowId)
            .bind("seq", sequenceNumber)
            .mapTo(Int::class.java)
            .one()

    fun countTotalWithHandle(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): Int =
        handle
            .createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :workflowId AND sequence_number = :seq",
            ).bind("workflowId", workflowId)
            .bind("seq", sequenceNumber)
            .mapTo(Int::class.java)
            .one()

    fun findByWorkflowAndSequenceWithHandle(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): List<Task> =
        handle
            .createQuery(
                "SELECT * FROM task WHERE workflow_id = :workflowId AND sequence_number = :seq",
            ).bind("workflowId", workflowId)
            .bind("seq", sequenceNumber)
            .mapToMap()
            .list()
            .map(::mapTaskRow)

    fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int {
        return handle.createUpdate(
            """
            UPDATE task SET status = 'CANCELLED', completed_at = :now
            WHERE workflow_id = :workflowId AND status IN ('PENDING', 'WAITING_FOR_SIGNAL')
            """,
        )
            .bind("workflowId", workflowId)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
            .execute()
    }

    fun insertBatchWithHandle(
        handle: Handle,
        tasks: List<Task>,
    ) {
        if (tasks.isEmpty()) return
        val batch =
            handle.prepareBatch(
                """
            INSERT INTO task (id, workflow_id, sequence_number, status, handler_key,
                              payload, result, claimed_by, claimed_at, completed_at,
                              retry_count, max_retries, deadline_at, not_before, backoff_base, backoff_cap, queue_name)
            VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey,
                    :payload, :result, :claimedBy, :claimedAt, :completedAt,
                    :retryCount, :maxRetries, :deadlineAt, :notBefore, :backoffBase, :backoffCap, :queueName)
            """,
            )
        for (task in tasks) {
            batch
                .bind("id", task.id)
                .bind("workflowId", task.workflowId)
                .bind("sequenceNumber", task.sequenceNumber)
                .bind("status", task.status.name)
                .bind("handlerKey", task.handlerKey)
            bindNullableClob(batch, "payload", task.payloadJson)
            bindNullableClob(batch, "result", task.resultJson)
            batch
                .bind("claimedBy", task.claimedBy)
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
            batch.add()
        }
        batch.execute()
    }

    fun replayDeadLetterBatchWithHandle(handle: Handle, workflowId: String): Int =
        handle
            .createUpdate(
                """
            UPDATE task
            SET status = 'PENDING', retry_count = 0,
                claimed_by = NULL, claimed_at = NULL,
                completed_at = NULL, result = NULL, not_before = NULL
            WHERE workflow_id = :workflowId AND status IN ('DEAD_LETTER', 'FAILED')
            """,
            ).bind("workflowId", workflowId)
            .execute()

    // ── Private helpers ──

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
            sequenceNumber = (ci["SEQUENCE_NUMBER"] as Number).toInt(),
            status = TaskStatus.valueOf(ci["STATUS"] as String),
            handlerKey = ci["HANDLER_KEY"] as String,
            payloadJson = ci["PAYLOAD"]?.let { readClob(it) },
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
        )
    }
}
