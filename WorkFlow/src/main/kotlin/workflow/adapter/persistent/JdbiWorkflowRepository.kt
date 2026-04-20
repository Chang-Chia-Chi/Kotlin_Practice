package com.workflow.workflow.adapter.persistent

import com.workflow.infrastructure.persistence.DB_ZONE
import com.workflow.infrastructure.persistence.caseInsensitive
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.persistence.readClob
import com.workflow.infrastructure.persistence.readTimestamp
import com.workflow.infrastructure.persistence.withHandleSuspend
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

@ApplicationScoped
class JdbiWorkflowRepository(private val jdbi: Jdbi) : WorkflowRepository {

    override suspend fun insert(run: WorkflowRun) {
        jdbi.withHandleSuspend<Unit, Exception> { h: Handle -> insertWithHandle(h, run) }
    }

    override suspend fun findById(id: String): WorkflowRun? =
        jdbi.withHandleSuspend<WorkflowRun?, Exception> { h: Handle -> findByIdWithHandle(h, id) }

    override suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            updateStatusWithHandle(h, id, newStatus, expectedStatus)
        }

    override suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun> =
        jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
            val cutoff = LocalDateTime.ofInstant(Instant.now().minus(gracePeriod), DB_ZONE)
            h.createQuery(
                """
                WITH max_seq AS (
                    SELECT t.workflow_id, MAX(t.sequence_number) AS max_seq_num
                    FROM task t
                    WHERE EXISTS (
                        SELECT 1 FROM workflow w
                        WHERE w.id = t.workflow_id
                          AND w.status = 'RUNNING'
                          AND w.updated_at < :cutoff
                    )
                    GROUP BY t.workflow_id
                )
                SELECT w.*
                FROM workflow w
                JOIN max_seq ms ON ms.workflow_id = w.id
                WHERE w.status = 'RUNNING'
                  AND w.updated_at < :cutoff
                  AND NOT EXISTS (
                    SELECT 1 FROM task t
                    WHERE t.workflow_id = w.id
                      AND t.sequence_number = ms.max_seq_num
                      AND t.status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
                  )
                """,
            )
                .bind("cutoff", cutoff)
                .mapToMap()
                .list()
                .map(::mapWorkflowRow)
        }

    override fun insertWithHandle(handle: Handle, run: WorkflowRun) {
        handle.createUpdate(
            """
            INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at)
            VALUES (:id, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)
            """,
        )
            .bind("id", run.id)
            .bind("definition", run.definitionJson)
            .bind("version", run.version)
            .bind("status", run.status.name)
            .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, DB_ZONE))
            .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, DB_ZONE))
            .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, DB_ZONE))
            .execute()
    }

    override fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun? =
        handle.createQuery("SELECT * FROM workflow WHERE id = :id")
            .bind("id", id)
            .mapToMap()
            .findOne()
            .map(::mapWorkflowRow)
            .orElse(null)

    override fun findByIdForUpdate(handle: Handle, id: String): WorkflowRun? =
        handle.createQuery("SELECT * FROM workflow WHERE id = :id FOR UPDATE")
            .bind("id", id)
            .mapToMap()
            .findOne()
            .map(::mapWorkflowRow)
            .orElse(null)

    override fun incrementVersionWithHandle(handle: Handle, id: String) {
        handle.createUpdate(
            "UPDATE workflow SET version = version + 1, updated_at = :now WHERE id = :id",
        )
            .bind("id", id)
            .bind("now", LocalDateTime.now(DB_ZONE).truncatedTo(ChronoUnit.MICROS))
            .execute()
    }

    override fun updateStatusWithHandle(
        handle: Handle,
        id: String,
        newStatus: WorkflowStatus,
        expectedStatus: WorkflowStatus,
    ): Boolean {
        WorkflowStatus.requireTransition(expectedStatus, newStatus)
        val count = handle.createUpdate(
            "UPDATE workflow SET status = :status, updated_at = :now WHERE id = :id AND status = :expectedStatus",
        )
            .bind("id", id)
            .bind("status", newStatus.name)
            .bind("expectedStatus", expectedStatus.name)
            .bind("now", LocalDateTime.now(DB_ZONE).truncatedTo(ChronoUnit.MICROS))
            .execute()
        return count == 1
    }

    override fun mergeIdempotentWithHandle(handle: Handle, run: WorkflowRun, idempotencyKey: String): Pair<String, Boolean> {
        val count = handle.createUpdate(
            """
            MERGE INTO workflow w
            USING (SELECT :idemKey AS idem_key FROM dual) src
            ON (w.idempotency_key = src.idem_key)
            WHEN NOT MATCHED THEN INSERT
                (id, idempotency_key, definition, version, status, created_at, updated_at, deadline_at)
            VALUES (:id, :idemKey, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)
            """,
        )
            .bind("idemKey", idempotencyKey)
            .bind("id", run.id)
            .bind("definition", run.definitionJson)
            .bind("version", run.version)
            .bind("status", run.status.name)
            .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, DB_ZONE))
            .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, DB_ZONE))
            .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, DB_ZONE))
            .execute()

        if (count == 1) return run.id to true

        val existingId = handle.createQuery("SELECT id FROM workflow WHERE idempotency_key = :key")
            .bind("key", idempotencyKey)
            .mapTo(String::class.java)
            .one()
        return existingId to false
    }

    override fun expireOverdueWithHandle(handle: Handle, now: LocalDateTime): Int =
        handle.createUpdate(
            """
            UPDATE workflow SET status = 'TIMED_OUT', updated_at = :now
            WHERE status = 'RUNNING' AND deadline_at < :now
            """,
        ).bind("now", now).execute()

    private fun mapWorkflowRow(row: Map<String, Any?>): WorkflowRun {
        val ci = caseInsensitive(row)
        return WorkflowRun(
            id = ci["ID"] as String,
            definitionJson = readClob(ci["DEFINITION"]),
            version = (ci["VERSION"] as Number).toInt(),
            status = WorkflowStatus.valueOf(ci["STATUS"] as String),
            createdAt = readTimestamp(ci["CREATED_AT"]),
            updatedAt = readTimestamp(ci["UPDATED_AT"]),
            deadlineAt = readTimestamp(ci["DEADLINE_AT"]),
        )
    }
}
