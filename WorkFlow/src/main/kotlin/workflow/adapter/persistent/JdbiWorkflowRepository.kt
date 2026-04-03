package com.workflow.workflow.adapter.persistent

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
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

@ApplicationScoped
class JdbiWorkflowRepository(private val jdbi: Jdbi) : WorkflowRepository {

    override suspend fun insert(run: WorkflowRun) {
        jdbi.withHandleSuspend<Unit, Exception> { h: Handle -> insertWithHandle(h, run) }
    }

    override suspend fun findById(id: String): WorkflowRun? =
        jdbi.withHandleSuspend<WorkflowRun?, Exception> { h: Handle -> findByIdWithHandle(h, id) }

    override suspend fun casVersion(id: String, expectedVersion: Int): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            casVersionWithHandle(h, id, expectedVersion)
        }

    override suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            updateStatusWithHandle(h, id, newStatus, expectedStatus)
        }

    override suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun> =
        jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
            // Placeholder: full DAG-aware stuck detection implemented in Plan 5
            emptyList()
        }

    override suspend fun findTimedOut(): List<WorkflowRun> =
        jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
            h.createQuery("SELECT * FROM workflow WHERE status = 'RUNNING' AND deadline_at < :now")
                .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS))
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
            .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
            .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
            .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, ZoneOffset.UTC))
            .execute()
    }

    override fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun? =
        handle.createQuery("SELECT * FROM workflow WHERE id = :id")
            .bind("id", id)
            .mapToMap()
            .findOne()
            .map(::mapWorkflowRow)
            .orElse(null)

    override fun casVersionWithHandle(handle: Handle, id: String, expectedVersion: Int): Boolean {
        val count = handle.createUpdate(
            """
            UPDATE workflow
            SET version = version + 1, updated_at = :now
            WHERE id = :id AND version = :expectedVersion AND status = 'RUNNING'
            """,
        )
            .bind("id", id)
            .bind("expectedVersion", expectedVersion)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS))
            .execute()
        return count == 1
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
            .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS))
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
            .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
            .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
            .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, ZoneOffset.UTC))
            .execute()

        if (count == 1) return run.id to true

        val existingId = handle.createQuery("SELECT id FROM workflow WHERE idempotency_key = :key")
            .bind("key", idempotencyKey)
            .mapTo(String::class.java)
            .one()
        return existingId to false
    }

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
