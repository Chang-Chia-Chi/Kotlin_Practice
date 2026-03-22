package com.workflow.engine

import com.workflow.extension.inTransactionSuspend
import com.workflow.extension.withHandleSuspend
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.sql.Clob
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@ApplicationScoped
class WorkflowRepository(private val jdbi: Jdbi) {

    // ── Suspend methods (open own connection) ──

    suspend fun insert(run: WorkflowRun) {
        jdbi.withHandleSuspend<Unit, Exception> { h: Handle -> insertWithHandle(h, run) }
    }

    suspend fun findById(id: String): WorkflowRun? =
        jdbi.withHandleSuspend<WorkflowRun?, Exception> { h: Handle -> findByIdWithHandle(h, id) }

    suspend fun casAdvance(
        id: String, expectedSequence: Int, nextSequence: Int, expectedVersion: Int,
    ): Boolean = jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
        casAdvanceWithHandle(h, id, expectedSequence, nextSequence, expectedVersion)
    }

    suspend fun updateStatus(id: String, newStatus: WorkflowStatus): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
            updateStatusWithHandle(h, id, newStatus)
        }

    suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun> =
        jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
            val cutoff = LocalDateTime.ofInstant(Instant.now().minus(gracePeriod), ZoneOffset.UTC)
            h.createQuery(
                """
                SELECT w.* FROM workflow w
                WHERE w.status = 'RUNNING'
                  AND w.updated_at < :cutoff
                  AND NOT EXISTS (
                    SELECT 1 FROM task t
                    WHERE t.workflow_id = w.id
                      AND t.sequence_number = w.current_sequence
                      AND t.status NOT IN ('COMPLETED', 'FAILED')
                  )
                """,
            )
                .bind("cutoff", cutoff)
                .mapToMap()
                .list()
                .map(::mapWorkflowRow)
        }

    // ── Handle methods (for barrier transaction) ──

    fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun? =
        handle.createQuery("SELECT * FROM workflow WHERE id = :id")
            .bind("id", id)
            .mapToMap()
            .findOne()
            .map(::mapWorkflowRow)
            .orElse(null)

    fun casAdvanceWithHandle(
        handle: Handle, id: String, expectedSequence: Int, nextSequence: Int, expectedVersion: Int,
    ): Boolean {
        val count = handle.createUpdate(
            """
            UPDATE workflow
            SET current_sequence = :nextSequence, version = version + 1, updated_at = :now
            WHERE id = :id AND current_sequence = :expectedSequence AND version = :expectedVersion
            """,
        )
            .bind("id", id)
            .bind("nextSequence", nextSequence)
            .bind("expectedSequence", expectedSequence)
            .bind("expectedVersion", expectedVersion)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC))
            .execute()
        return count == 1
    }

    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus): Boolean {
        val count = handle.createUpdate(
            "UPDATE workflow SET status = :status, updated_at = :now WHERE id = :id",
        )
            .bind("id", id)
            .bind("status", newStatus.name)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC))
            .execute()
        return count == 1
    }

    // ── Private helpers ──

    fun insertWithHandle(handle: Handle, run: WorkflowRun) {
        handle.createUpdate(
            """
            INSERT INTO workflow (id, definition, current_sequence, version, status, created_at, updated_at)
            VALUES (:id, :definition, :currentSequence, :version, :status, :createdAt, :updatedAt)
            """,
        )
            .bind("id", run.id)
            .bind("definition", run.definitionJson)
            .bind("currentSequence", run.currentSequence)
            .bind("version", run.version)
            .bind("status", run.status.name)
            .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
            .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
            .execute()
    }

    private fun mapWorkflowRow(row: Map<String, Any?>): WorkflowRun {
        val ci = caseInsensitive(row)
        return WorkflowRun(
            id = ci["ID"] as String,
            definitionJson = readClob(ci["DEFINITION"]),
            currentSequence = (ci["CURRENT_SEQUENCE"] as Number).toInt(),
            version = (ci["VERSION"] as Number).toInt(),
            status = WorkflowStatus.valueOf(ci["STATUS"] as String),
            createdAt = readTimestamp(ci["CREATED_AT"]),
            updatedAt = readTimestamp(ci["UPDATED_AT"]),
        )
    }
}

// ── Shared mapping utilities ──

internal fun readClob(value: Any?): String = when (value) {
    is Clob -> value.characterStream.readText()
    null -> ""
    else -> value.toString()
}

internal fun readTimestamp(value: Any?): Instant = when (value) {
    is LocalDateTime -> value.toInstant(ZoneOffset.UTC)
    is java.sql.Timestamp -> value.toLocalDateTime().toInstant(ZoneOffset.UTC)
    else -> {
        // Oracle JDBC returns oracle.sql.TIMESTAMP — convert via timestampValue()
        val clazz = value?.javaClass
        if (clazz?.name == "oracle.sql.TIMESTAMP") {
            val sqlTs = clazz.getMethod("timestampValue").invoke(value) as java.sql.Timestamp
            sqlTs.toLocalDateTime().toInstant(ZoneOffset.UTC)
        } else {
            throw IllegalStateException("Unexpected timestamp type: $clazz")
        }
    }
}

internal fun readNullableTimestamp(value: Any?): Instant? = when (value) {
    null -> null
    else -> readTimestamp(value)
}

internal fun caseInsensitive(row: Map<String, Any?>): Map<String, Any?> =
    java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER).apply { putAll(row) }
