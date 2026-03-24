package com.workflow.engine

import com.workflow.extension.inTransactionSuspend
import com.workflow.extension.withHandleSuspend
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
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
                      AND (deadline_at IS NULL OR deadline_at > :now)
                    ORDER BY claimed_at NULLS FIRST, id
                    FETCH FIRST :limit ROWS ONLY
                )
                FOR UPDATE SKIP LOCKED
                """,
                    ).bind("limit", limit)
                    .bind("now", now)
                    .mapToMap()
                    .list()

            if (rows.isEmpty()) return@inTransactionSuspend emptyList()

            val ids = rows.map { caseInsensitive(it)["ID"] as String }
            h
                .createUpdate(
                    """
                UPDATE task SET status = 'PROCESSING', claimed_by = :workerId, claimed_at = :now
                WHERE id IN (<ids>)
                """,
                ).bind("workerId", workerId)
                .bind("now", now)
                .bindList("ids", ids)
                .execute()

            val nowInstant = now.toInstant(ZoneOffset.UTC)
            rows.map { row ->
                mapTaskRow(row).copy(
                    status = TaskStatus.PROCESSING,
                    claimedBy = workerId,
                    claimedAt = nowInstant,
                )
            }
        }

    suspend fun updateStatus(
        id: String,
        newStatus: TaskStatus,
        resultJson: String? = null,
    ) {
        jdbi.inTransactionSuspend<Unit, Exception> { h: Handle ->
            updateStatusWithHandle(h, id, newStatus, resultJson)
        }
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
                SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, retry_count = :newRetryCount
                WHERE id = :id
                """,
                ).bind("id", id)
                .bind("newRetryCount", newRetryCount)
                .execute()
        }
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
                SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, retry_count = retry_count + 1
                WHERE status = 'PROCESSING' AND claimed_at < :threshold AND retry_count < max_retries
                """,
                ).bind("threshold", LocalDateTime.ofInstant(staleThreshold, ZoneOffset.UTC))
                .execute()
        }

    suspend fun findStale(staleThreshold: Instant): List<Task> =
        jdbi.withHandleSuspend<List<Task>, Exception> { h: Handle ->
            h
                .createQuery(
                    "SELECT * FROM task WHERE status = 'PROCESSING' AND claimed_at < :threshold",
                ).bind("threshold", LocalDateTime.ofInstant(staleThreshold, ZoneOffset.UTC))
                .mapToMap()
                .list()
                .map(::mapTaskRow)
        }

    // ── Handle methods (for barrier transaction) ──

    fun updateStatusWithHandle(
        handle: Handle,
        id: String,
        newStatus: TaskStatus,
        resultJson: String? = null,
    ) {
        if (newStatus.isTerminal) {
            handle
                .createUpdate(
                    """
                UPDATE task SET status = :status, result = :result, completed_at = :now
                WHERE id = :id
                """,
                ).bind("id", id)
                .bind("status", newStatus.name)
                .bind("result", resultJson)
                .bind("now", LocalDateTime.now(ZoneOffset.UTC))
                .execute()
        } else {
            handle
                .createUpdate("UPDATE task SET status = :status, result = :result WHERE id = :id")
                .bind("id", id)
                .bind("status", newStatus.name)
                .bind("result", resultJson)
                .execute()
        }
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
              AND status NOT IN ('COMPLETED', 'FAILED')
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
              AND status = 'FAILED'
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
                              retry_count, max_retries, deadline_at)
            VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey,
                    :payload, :result, :claimedBy, :claimedAt, :completedAt,
                    :retryCount, :maxRetries, :deadlineAt)
            """,
            )
        for (task in tasks) {
            batch
                .bind("id", task.id)
                .bind("workflowId", task.workflowId)
                .bind("sequenceNumber", task.sequenceNumber)
                .bind("status", task.status.name)
                .bind("handlerKey", task.handlerKey)
                .bind("payload", task.payloadJson)
                .bind("result", task.resultJson)
                .bind("claimedBy", task.claimedBy)
            bindNullableTimestamp(batch, "claimedAt", task.claimedAt)
            bindNullableTimestamp(batch, "completedAt", task.completedAt)
            batch
                .bind("retryCount", task.retryCount)
                .bind("maxRetries", task.maxRetries)
            bindNullableTimestamp(batch, "deadlineAt", task.deadlineAt)
            batch.add()
        }
        batch.execute()
    }

    // ── Private helpers ──

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
        )
    }
}
