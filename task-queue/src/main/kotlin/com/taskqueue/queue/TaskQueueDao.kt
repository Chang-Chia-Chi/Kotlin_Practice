package com.taskqueue.queue

import jakarta.inject.Singleton
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jboss.logging.Logger
import java.security.MessageDigest
import java.sql.SQLIntegrityConstraintViolationException
import java.time.Instant
import kotlin.math.min
import kotlin.random.Random

/**
 * Data-access layer for TASK_QUEUE. All SQL lives here — no other class touches the table directly.
 *
 * Key design decisions:
 * - Claim uses Oracle MERGE with FOR UPDATE SKIP LOCKED for atomic lock+transition (2 round-trips).
 * - Status transitions are single-row UPDATEs with WHERE guards on current status to enforce the state machine.
 * - Bulk insert uses batched PreparedStatement for child tasks (O(1) round-trips via JDBI batch).
 * - Exponential backoff: attempt^4 seconds, capped at 1 hour, +/-10% jitter.
 * - Error history: JSON array appended on each retry/discard for diagnostics.
 * - Unique jobs: SHA-256 deduplication key prevents duplicate active tasks.
 */
@Singleton
class TaskQueueDao(private val jdbi: Jdbi) {

    private val log = Logger.getLogger(TaskQueueDao::class.java)

    // ────────────────────────── Consumer: Claim (Atomic MERGE) ──────────────────────────

    /**
     * Atomically claim up to [batchSize] PENDING tasks using Oracle MERGE.
     *
     * 1. MERGE: SELECT FOR UPDATE SKIP LOCKED → lock rows + flip to PROCESSING in one statement
     * 2. SELECT: fetch full task data for claimed rows
     *
     * 2 round-trips (down from 3). Lock acquisition and state transition are atomic.
     */
    fun claimBatch(batchSize: Int): List<TaskContext> {
        return jdbi.withHandleUnchecked { handle ->
            handle.begin()
            try {
                // Step 1: Atomic lock + state transition via MERGE
                handle.createUpdate(
                    """
                    MERGE INTO TASK_QUEUE tgt
                    USING (
                        SELECT TASK_ID FROM (
                            SELECT TASK_ID
                            FROM TASK_QUEUE
                            WHERE STATUS = 'PENDING'
                              AND (DEADLINE_AT IS NULL OR DEADLINE_AT > SYSTIMESTAMP)
                            ORDER BY PRIORITY, CREATED_AT
                            FOR UPDATE SKIP LOCKED
                        ) WHERE ROWNUM <= :batchSize
                    ) src
                    ON (tgt.TASK_ID = src.TASK_ID)
                    WHEN MATCHED THEN UPDATE SET
                        tgt.STATUS = 'PROCESSING',
                        tgt.STARTED_AT = SYSTIMESTAMP,
                        tgt.UPDATED_AT = SYSTIMESTAMP
                    """.trimIndent()
                )
                    .bind("batchSize", batchSize)
                    .execute()

                // Step 2: Fetch the rows we just claimed
                val tasks = handle.createQuery(
                    """
                    SELECT TASK_ID, PARENT_TASK_ID, TASK_TYPE, PAYLOAD, PRIORITY,
                           RETRY_COUNT, MAX_RETRIES, DEADLINE_AT, SCHEDULED_AT, CREATED_AT
                    FROM TASK_QUEUE
                    WHERE STATUS = 'PROCESSING'
                      AND STARTED_AT >= SYSTIMESTAMP - NUMTODSINTERVAL(5, 'SECOND')
                    ORDER BY PRIORITY, CREATED_AT
                    """.trimIndent()
                ).map { rs, _ ->
                    TaskContext(
                        taskId = rs.getLong("TASK_ID"),
                        parentTaskId = rs.getLong("PARENT_TASK_ID").takeIf { !rs.wasNull() },
                        taskType = rs.getString("TASK_TYPE"),
                        payload = rs.getString("PAYLOAD"),
                        priority = rs.getInt("PRIORITY"),
                        retryCount = rs.getInt("RETRY_COUNT"),
                        maxRetries = rs.getInt("MAX_RETRIES"),
                        deadlineAt = rs.getTimestamp("DEADLINE_AT")?.toInstant(),
                        scheduledAt = rs.getTimestamp("SCHEDULED_AT")?.toInstant(),
                        createdAt = rs.getTimestamp("CREATED_AT").toInstant(),
                    )
                }.list()

                handle.commit()
                tasks
            } catch (e: Exception) {
                handle.rollback()
                throw e
            }
        }
    }

    // ────────────────────── Status Transitions ──────────────────────

    /** PROCESSING → DONE. Returns true if the update matched (idempotent guard). */
    fun markDone(taskId: Long): Boolean = transitionStatus(
        taskId = taskId,
        fromStatus = TaskStatus.PROCESSING,
        toStatus = TaskStatus.DONE,
        extraSetClauses = "COMPLETED_AT = SYSTIMESTAMP",
    )

    /** PROCESSING → CANCELLED. Explicit cancel by handler via [TaskResult.Cancel]. */
    fun markCancelled(taskId: Long, reason: String?): Boolean = transitionStatus(
        taskId = taskId,
        fromStatus = TaskStatus.PROCESSING,
        toStatus = TaskStatus.CANCELLED,
        extraSetClauses = "COMPLETED_AT = SYSTIMESTAMP, ERROR_MESSAGE = :errorMessage",
        extraBinds = mapOf("errorMessage" to truncate(reason, 4000)),
    )

    /** PROCESSING → DISCARDED. Retries exhausted. Captures final error. */
    fun markDiscarded(taskId: Long, errorMessage: String?, retryCount: Int): Boolean {
        val historyEntry = buildErrorHistoryEntry(retryCount, errorMessage)
        return transitionStatus(
            taskId = taskId,
            fromStatus = TaskStatus.PROCESSING,
            toStatus = TaskStatus.DISCARDED,
            extraSetClauses = """
                COMPLETED_AT = SYSTIMESTAMP,
                ERROR_MESSAGE = :errorMessage,
                ERROR_HISTORY = CASE
                    WHEN ERROR_HISTORY IS NULL THEN '[' || :historyEntry || ']'
                    ELSE SUBSTR(ERROR_HISTORY, 1, LENGTH(ERROR_HISTORY) - 1) || ',' || :historyEntry || ']'
                END
            """.trimIndent(),
            extraBinds = mapOf(
                "errorMessage" to truncate(errorMessage, 4000),
                "historyEntry" to historyEntry,
            ),
        )
    }

    /**
     * PROCESSING → RETRYABLE. Sets SCHEDULED_AT with exponential backoff.
     * Formula: attempt^4 seconds, capped at 1 hour, +/-10% jitter.
     */
    fun markRetryable(taskId: Long, errorMessage: String?, retryCount: Int): Boolean {
        val backoffSeconds = computeBackoffSeconds(retryCount + 1)
        val historyEntry = buildErrorHistoryEntry(retryCount, errorMessage)
        return transitionStatus(
            taskId = taskId,
            fromStatus = TaskStatus.PROCESSING,
            toStatus = TaskStatus.RETRYABLE,
            extraSetClauses = """
                RETRY_COUNT = RETRY_COUNT + 1,
                STARTED_AT = NULL,
                ERROR_MESSAGE = :errorMessage,
                SCHEDULED_AT = SYSTIMESTAMP + NUMTODSINTERVAL(:backoffSeconds, 'SECOND'),
                ERROR_HISTORY = CASE
                    WHEN ERROR_HISTORY IS NULL THEN '[' || :historyEntry || ']'
                    ELSE SUBSTR(ERROR_HISTORY, 1, LENGTH(ERROR_HISTORY) - 1) || ',' || :historyEntry || ']'
                END
            """.trimIndent(),
            extraBinds = mapOf(
                "errorMessage" to truncate(errorMessage, 4000),
                "backoffSeconds" to backoffSeconds,
                "historyEntry" to historyEntry,
            ),
        )
    }

    /** PROCESSING → SCHEDULED. Handler returned [TaskResult.Snooze]. */
    fun markSnoozed(taskId: Long, snoozeSeconds: Long): Boolean = transitionStatus(
        taskId = taskId,
        fromStatus = TaskStatus.PROCESSING,
        toStatus = TaskStatus.SCHEDULED,
        extraSetClauses = """
            STARTED_AT = NULL,
            SCHEDULED_AT = SYSTIMESTAMP + NUMTODSINTERVAL(:snoozeSeconds, 'SECOND')
        """.trimIndent(),
        extraBinds = mapOf("snoozeSeconds" to snoozeSeconds),
    )

    /** PROCESSING or PENDING → EXPIRED. Used by consumer (pre-handler check) and leader housekeeping. */
    fun markExpired(taskId: Long): Boolean {
        return jdbi.withHandleUnchecked { handle ->
            handle.createUpdate(
                """
                UPDATE TASK_QUEUE
                SET STATUS = 'EXPIRED',
                    COMPLETED_AT = SYSTIMESTAMP,
                    UPDATED_AT = SYSTIMESTAMP
                WHERE TASK_ID = :taskId
                  AND STATUS IN ('PENDING', 'PROCESSING')
                """.trimIndent()
            )
                .bind("taskId", taskId)
                .execute() > 0
        }
    }

    // ────────────────────── Scheduled Task Promotion ──────────────────────

    /**
     * Promote RETRYABLE/SCHEDULED tasks to PENDING when their SCHEDULED_AT has arrived.
     * Called by leader cron on a fixed interval.
     */
    fun promoteScheduledTasks(): Int {
        return jdbi.withHandleUnchecked { handle ->
            handle.createUpdate(
                """
                UPDATE TASK_QUEUE
                SET STATUS = 'PENDING',
                    SCHEDULED_AT = NULL,
                    UPDATED_AT = SYSTIMESTAMP
                WHERE STATUS IN ('RETRYABLE', 'SCHEDULED')
                  AND SCHEDULED_AT IS NOT NULL
                  AND SCHEDULED_AT <= SYSTIMESTAMP
                """.trimIndent()
            ).execute()
        }
    }

    // ────────────────────── Child Task Insert ──────────────────────

    /**
     * Bulk-insert child tasks emitted by a handler.
     * Uses JDBI batch for efficiency — single round-trip regardless of child count.
     */
    fun insertChildren(parentTaskId: Long, children: List<TaskEmitter.PendingTask>) {
        if (children.isEmpty()) return

        jdbi.withHandleUnchecked { handle ->
            val batch = handle.prepareBatch(
                """
                INSERT INTO TASK_QUEUE (PARENT_TASK_ID, TASK_TYPE, PAYLOAD, PRIORITY, DEADLINE_AT, UNIQUE_KEY)
                VALUES (:parentTaskId, :taskType, :payload, :priority, :deadlineAt, :uniqueKey)
                """.trimIndent()
            )

            for (child in children) {
                batch
                    .bind("parentTaskId", parentTaskId)
                    .bind("taskType", child.taskType)
                    .bind("payload", child.payload)
                    .bind("priority", child.priority)
                    .bind("deadlineAt", child.deadlineAt?.let { java.sql.Timestamp.from(it) })
                    .bind("uniqueKey", child.uniqueKey)
                    .add()
            }

            val counts = batch.execute()
            log.debugf("Inserted %d children for parent task %d", counts.size, parentTaskId)
        }
    }

    // ────────────────────── Root Task Production ──────────────────────

    /** Insert a root task (no parent). Used by leader cron jobs. Returns the generated TASK_ID. */
    fun insertRootTask(
        taskType: String,
        payload: String? = null,
        priority: Int = 5,
        deadlineAt: Instant? = null,
    ): Long {
        return jdbi.withHandleUnchecked { handle ->
            handle.createUpdate(
                """
                INSERT INTO TASK_QUEUE (TASK_TYPE, PAYLOAD, PRIORITY, DEADLINE_AT)
                VALUES (:taskType, :payload, :priority, :deadlineAt)
                """.trimIndent()
            )
                .bind("taskType", taskType)
                .bind("payload", payload)
                .bind("priority", priority)
                .bind("deadlineAt", deadlineAt?.let { java.sql.Timestamp.from(it) })
                .executeAndReturnGeneratedKeys("TASK_ID")
                .mapTo(Long::class.java)
                .one()
        }
    }

    /**
     * Insert a root task with deduplication. If a non-terminal task with the same [uniqueKey]
     * already exists, the insert is silently skipped (returns null).
     *
     * The UNIQUE_KEY is enforced by a function-based unique index that only covers
     * non-terminal statuses, so completed/cancelled/discarded/expired tasks don't block re-insertion.
     */
    fun insertRootTaskUnique(
        taskType: String,
        payload: String? = null,
        priority: Int = 5,
        deadlineAt: Instant? = null,
        uniqueKey: String,
    ): Long? {
        return try {
            jdbi.withHandleUnchecked { handle ->
                handle.createUpdate(
                    """
                    INSERT INTO TASK_QUEUE (TASK_TYPE, PAYLOAD, PRIORITY, DEADLINE_AT, UNIQUE_KEY)
                    VALUES (:taskType, :payload, :priority, :deadlineAt, :uniqueKey)
                    """.trimIndent()
                )
                    .bind("taskType", taskType)
                    .bind("payload", payload)
                    .bind("priority", priority)
                    .bind("deadlineAt", deadlineAt?.let { java.sql.Timestamp.from(it) })
                    .bind("uniqueKey", uniqueKey)
                    .executeAndReturnGeneratedKeys("TASK_ID")
                    .mapTo(Long::class.java)
                    .one()
            }
        } catch (e: Exception) {
            if (isDuplicateKeyException(e)) {
                log.debugf("Duplicate task skipped: uniqueKey=%s, taskType=%s", uniqueKey, taskType)
                null
            } else {
                throw e
            }
        }
    }

    // ────────────────────── Housekeeping ──────────────────────

    /**
     * Reclaim tasks stuck in PROCESSING beyond [staleMinutes].
     *
     * A task is "stale" when its pod crashed (or was evicted) mid-processing.
     * - Past deadline → EXPIRED
     * - Otherwise → PENDING (will be re-consumed)
     *
     * Returns the number of reclaimed rows.
     */
    fun reclaimStaleTasks(staleMinutes: Int): Int {
        return jdbi.withHandleUnchecked { handle ->
            handle.createUpdate(
                """
                UPDATE TASK_QUEUE
                SET STATUS = CASE
                        WHEN DEADLINE_AT IS NOT NULL AND DEADLINE_AT < SYSTIMESTAMP THEN 'EXPIRED'
                        ELSE 'PENDING'
                    END,
                    UPDATED_AT = SYSTIMESTAMP,
                    STARTED_AT = NULL
                WHERE STATUS = 'PROCESSING'
                  AND UPDATED_AT < SYSTIMESTAMP - NUMTODSINTERVAL(:staleMinutes, 'MINUTE')
                """.trimIndent()
            )
                .bind("staleMinutes", staleMinutes)
                .execute()
        }
    }

    /**
     * Expire PENDING tasks whose deadline has passed.
     * These tasks were never claimed before their deadline — mark them EXPIRED.
     */
    fun expireOverdueTasks(): Int {
        return jdbi.withHandleUnchecked { handle ->
            handle.createUpdate(
                """
                UPDATE TASK_QUEUE
                SET STATUS = 'EXPIRED',
                    COMPLETED_AT = SYSTIMESTAMP,
                    UPDATED_AT = SYSTIMESTAMP
                WHERE STATUS = 'PENDING'
                  AND DEADLINE_AT IS NOT NULL
                  AND DEADLINE_AT < SYSTIMESTAMP
                """.trimIndent()
            ).execute()
        }
    }

    /** Purge terminal tasks older than [retentionDays]. Returns deleted count. */
    fun purgeOldTasks(retentionDays: Int): Int {
        return jdbi.withHandleUnchecked { handle ->
            handle.createUpdate(
                """
                DELETE FROM TASK_QUEUE
                WHERE STATUS IN ('DONE', 'CANCELLED', 'DISCARDED', 'EXPIRED')
                  AND UPDATED_AT < SYSTIMESTAMP - NUMTODSINTERVAL(:retentionDays, 'DAY')
                """.trimIndent()
            )
                .bind("retentionDays", retentionDays)
                .execute()
        }
    }

    // ────────────────────── Monitoring ──────────────────────

    /** Counts by status — for dashboards, health checks, alerting. */
    fun countByStatus(): Map<String, Long> {
        return jdbi.withHandleUnchecked { handle ->
            handle.createQuery(
                "SELECT STATUS, COUNT(*) AS CNT FROM TASK_QUEUE GROUP BY STATUS"
            ).map { rs, _ ->
                rs.getString("STATUS") to rs.getLong("CNT")
            }.list().toMap()
        }
    }

    // ────────────────────── Internal Helpers ──────────────────────

    /**
     * Generic single-row status transition with WHERE guard on current status.
     *
     * The WHERE guard enforces the state machine: if the row is no longer in [fromStatus]
     * (e.g., another pod already handled it), the UPDATE is a no-op and returns false.
     */
    private fun transitionStatus(
        taskId: Long,
        fromStatus: TaskStatus,
        toStatus: TaskStatus,
        extraSetClauses: String = "",
        extraBinds: Map<String, Any?> = emptyMap(),
    ): Boolean {
        return jdbi.withHandleUnchecked { handle ->
            val setClauses = buildString {
                append("STATUS = :toStatus, UPDATED_AT = SYSTIMESTAMP")
                if (extraSetClauses.isNotBlank()) {
                    append(", ")
                    append(extraSetClauses)
                }
            }

            val update = handle.createUpdate(
                """
                UPDATE TASK_QUEUE
                SET $setClauses
                WHERE TASK_ID = :taskId
                  AND STATUS = :fromStatus
                """.trimIndent()
            )
                .bind("taskId", taskId)
                .bind("toStatus", toStatus.name)
                .bind("fromStatus", fromStatus.name)

            extraBinds.forEach { (key, value) -> update.bind(key, value) }

            update.execute() > 0
        }
    }

    private fun truncate(value: String?, maxLength: Int): String? =
        value?.take(maxLength)

    /**
     * Exponential backoff: attempt^4 seconds, capped at 1 hour, +/-10% jitter.
     * Produces: attempt 1 ~1s, 2 ~16s, 3 ~81s, 4 ~256s, 5+ capped at ~3600s.
     */
    internal fun computeBackoffSeconds(attempt: Int): Long {
        val base = attempt.toLong() * attempt * attempt * attempt // attempt^4
        val capped = min(base, 3600L) // cap at 1 hour
        val jitter = (capped * 0.1 * (Random.nextDouble() * 2 - 1)).toLong() // +/-10%
        return maxOf(1L, capped + jitter)
    }

    private fun buildErrorHistoryEntry(attempt: Int, errorMessage: String?): String {
        val escapedError = truncate(errorMessage, 1000)
            ?.replace("\\", "\\\\")
            ?.replace("\"", "\\\"")
            ?.replace("\n", "\\n")
            ?: ""
        return """{"attempt":$attempt,"at":"${Instant.now()}","error":"$escapedError"}"""
    }

    private fun isDuplicateKeyException(e: Exception): Boolean {
        var cause: Throwable? = e
        while (cause != null) {
            if (cause is SQLIntegrityConstraintViolationException) return true
            // Oracle ORA-00001: unique constraint violated
            if (cause.message?.contains("ORA-00001") == true) return true
            cause = cause.cause
        }
        return false
    }

    companion object {
        /** Generate a SHA-256 deduplication key from taskType + payload. */
        fun generateUniqueKey(taskType: String, payload: String?): String {
            val input = "$taskType:${payload ?: ""}"
            val digest = MessageDigest.getInstance("SHA-256").digest(input.toByteArray())
            return digest.joinToString("") { "%02x".format(it) }
        }
    }
}
