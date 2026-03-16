package com.mapreduce.deadletter.repository

import com.mapreduce.TestH2Factory
import com.mapreduce.queue.model.TaskStatus
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class DeadLetterRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var repo: DeadLetterRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        repo = DeadLetterRepository(jdbi)
    }

    // ── findDeadLetters ─────────────────────────────────────────────

    @Test
    fun `findDeadLetters returns only DEAD_LETTER tasks`() {
        insertTask("t-dl-1", handler = "h1", status = "DEAD_LETTER")
        insertTask("t-dl-2", handler = "h1", status = "DEAD_LETTER")
        insertTask("t-pending", handler = "h1", status = "PENDING")
        insertTask("t-completed", handler = "h1", status = "COMPLETED")

        val results = repo.findDeadLetters()

        assertEquals(2, results.size)
        assertTrue(results.all { it.status == TaskStatus.DEAD_LETTER })
    }

    @Test
    fun `findDeadLetters filters by handler`() {
        insertTask("t-1", handler = "email.send", status = "DEAD_LETTER")
        insertTask("t-2", handler = "order.process", status = "DEAD_LETTER")

        val results = repo.findDeadLetters(handler = "email.send")

        assertEquals(1, results.size)
        assertEquals("email.send", results.first().handler)
    }

    @Test
    fun `findDeadLetters paginates with limit and offset`() {
        repeat(5) { i ->
            insertTask("t-$i", handler = "h1", status = "DEAD_LETTER",
                createdAt = Instant.now().minusSeconds((10 - i).toLong()))
        }

        val page1 = repo.findDeadLetters(limit = 2, offset = 0)
        val page2 = repo.findDeadLetters(limit = 2, offset = 2)

        assertEquals(2, page1.size)
        assertEquals(2, page2.size)
        // Pages should not overlap
        val allIds = (page1 + page2).map { it.taskId }.toSet()
        assertEquals(4, allIds.size)
    }

    // ── findDeadLetterById ──────────────────────────────────────────

    @Test
    fun `findDeadLetterById returns null for non-existent task`() {
        assertNull(repo.findDeadLetterById("nonexistent"))
    }

    @Test
    fun `findDeadLetterById returns null for non-DEAD_LETTER task`() {
        insertTask("t-pending", handler = "h1", status = "PENDING")

        assertNull(repo.findDeadLetterById("t-pending"))
    }

    // ── replaySingle ────────────────────────────────────────────────

    @Test
    fun `replaySingle transitions task to PENDING and resets retry_count`() {
        insertTask("t-1", handler = "h1", status = "DEAD_LETTER", retryCount = 3,
            errorMessage = "timeout", claimedBy = "worker-1")

        val success = repo.replaySingle("t-1", null, null)

        assertTrue(success)
        val row = queryTask("t-1")
        assertEquals("PENDING", row["status"])
        assertEquals(0, (row["retry_count"] as Number).toInt())
        assertNull(row["error_message"])
        assertNull(row["claimed_by"])
        assertNull(row["claimed_at"])
    }

    @Test
    fun `replaySingle returns false for non-DEAD_LETTER task`() {
        insertTask("t-pending", handler = "h1", status = "PENDING")

        assertFalse(repo.replaySingle("t-pending", null, null))
    }

    @Test
    fun `replaySingle returns false for nonexistent task`() {
        assertFalse(repo.replaySingle("nonexistent", null, null))
    }

    // ── replayByFilter ──────────────────────────────────────────────

    @Test
    fun `replayByFilter replays matching tasks`() {
        insertTask("t-1", handler = "email.send", status = "DEAD_LETTER", retryCount = 3)
        insertTask("t-2", handler = "email.send", status = "DEAD_LETTER", retryCount = 2)
        insertTask("t-3", handler = "order.process", status = "DEAD_LETTER", retryCount = 1)

        val count = repo.replayByFilter(handler = "email.send")

        assertEquals(2, count)
        assertEquals("PENDING", queryTask("t-1")["status"])
        assertEquals("PENDING", queryTask("t-2")["status"])
        assertEquals("DEAD_LETTER", queryTask("t-3")["status"])
    }

    // ── replayJob ───────────────────────────────────────────────────

    @Test
    fun `replayJob replays tasks and adjusts mr_job failed_tasks`() {
        insertJob("job-1", status = "FAILED", failedTasks = 3)
        insertTask("t-1", handler = "h1", status = "DEAD_LETTER", groupId = "job-1")
        insertTask("t-2", handler = "h1", status = "DEAD_LETTER", groupId = "job-1")

        val count = repo.replayJob("job-1")

        assertEquals(2, count)
        assertEquals("PENDING", queryTask("t-1")["status"])
        assertEquals("PENDING", queryTask("t-2")["status"])

        val job = queryJob("job-1")
        assertEquals(1L, (job["failed_tasks"] as Number).toLong())  // 3 - 2, clamped at >= 0
    }

    @Test
    fun `replayJob transitions FAILED job back to RUNNING`() {
        insertJob("job-1", status = "FAILED", failedTasks = 2)
        insertTask("t-1", handler = "h1", status = "DEAD_LETTER", groupId = "job-1")

        repo.replayJob("job-1")

        assertEquals("RUNNING", queryJob("job-1")["status"])
    }

    @Test
    fun `replayJob returns -1 for COMPLETED job without force`() {
        insertJob("job-1", status = "COMPLETED", failedTasks = 0)
        insertTask("t-1", handler = "h1", status = "DEAD_LETTER", groupId = "job-1")

        assertEquals(-1, repo.replayJob("job-1", force = false))
        // Task should remain DEAD_LETTER
        assertEquals("DEAD_LETTER", queryTask("t-1")["status"])
    }

    @Test
    fun `replayJob with force=true replays COMPLETED job`() {
        insertJob("job-1", status = "COMPLETED", failedTasks = 1)
        insertTask("t-1", handler = "h1", status = "DEAD_LETTER", groupId = "job-1")

        val count = repo.replayJob("job-1", force = true)

        assertEquals(1, count)
        assertEquals("PENDING", queryTask("t-1")["status"])
    }

    @Test
    fun `replayJob returns 0 when no dead-letter tasks for job`() {
        insertJob("job-1", status = "RUNNING", failedTasks = 0)

        assertEquals(0, repo.replayJob("job-1"))
    }

    @Test
    fun `replayJob returns 0 for nonexistent job`() {
        assertEquals(0, repo.replayJob("nonexistent"))
    }

    // ── deleteOlderThan ─────────────────────────────────────────────

    @Test
    fun `deleteOlderThan deletes old tasks and keeps recent ones`() {
        val old = Instant.now().minus(60, ChronoUnit.DAYS)
        val recent = Instant.now().minus(1, ChronoUnit.DAYS)
        val cutoff = Instant.now().minus(30, ChronoUnit.DAYS)

        insertTask("t-old", handler = "h1", status = "DEAD_LETTER", createdAt = old)
        insertTask("t-recent", handler = "h1", status = "DEAD_LETTER", createdAt = recent)

        val deleted = repo.deleteOlderThan(cutoff)

        assertEquals(1, deleted)
        assertNull(queryTaskOrNull("t-old"))
        assertEquals("DEAD_LETTER", queryTask("t-recent")["status"])
    }

    @Test
    fun `deleteOlderThan only affects DEAD_LETTER tasks`() {
        val old = Instant.now().minus(60, ChronoUnit.DAYS)
        val cutoff = Instant.now().minus(30, ChronoUnit.DAYS)

        insertTask("t-dl", handler = "h1", status = "DEAD_LETTER", createdAt = old)
        insertTask("t-pending", handler = "h1", status = "PENDING", createdAt = old)

        val deleted = repo.deleteOlderThan(cutoff)

        assertEquals(1, deleted)
        assertNull(queryTaskOrNull("t-dl"))
        assertEquals("PENDING", queryTask("t-pending")["status"])
    }

    // ── countAll / countByHandler ────────────────────────────────────

    @Test
    fun `countAll returns count of dead-letter tasks`() {
        insertTask("t-1", handler = "h1", status = "DEAD_LETTER")
        insertTask("t-2", handler = "h2", status = "DEAD_LETTER")
        insertTask("t-3", handler = "h1", status = "PENDING")

        assertEquals(2, repo.countAll())
    }

    @Test
    fun `countByHandler returns count for specific handler`() {
        insertTask("t-1", handler = "email.send", status = "DEAD_LETTER")
        insertTask("t-2", handler = "email.send", status = "DEAD_LETTER")
        insertTask("t-3", handler = "order.process", status = "DEAD_LETTER")

        assertEquals(2, repo.countByHandler("email.send"))
        assertEquals(1, repo.countByHandler("order.process"))
        assertEquals(0, repo.countByHandler("nonexistent"))
    }

    // ── helpers ─────────────────────────────────────────────────────

    private fun insertTask(
        taskId: String,
        handler: String = "test-handler",
        status: String = "DEAD_LETTER",
        queue: String = "default",
        groupId: String? = null,
        retryCount: Int = 0,
        errorMessage: String? = null,
        claimedBy: String? = null,
        createdAt: Instant? = null,
    ) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, status, group_id,
                    retry_count, error_message, claimed_by, created_at)
                VALUES (:taskId, :handler, :queue, :status, :groupId,
                    :retryCount, :errorMessage, :claimedBy, :createdAt)
                """,
            )
                .bind("taskId", taskId)
                .bind("handler", handler)
                .bind("queue", queue)
                .bind("status", status)
                .bind("groupId", groupId)
                .bind("retryCount", retryCount)
                .bind("errorMessage", errorMessage)
                .bind("claimedBy", claimedBy)
                .bind("createdAt", createdAt ?: Instant.now())
                .execute()
        }
    }

    private fun insertJob(
        jobId: String,
        status: String = "RUNNING",
        failedTasks: Int = 0,
    ) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO mr_job (job_id, job_type, status, failed_tasks, total_tasks, version)
                VALUES (:jobId, 'test-type', :status, :failedTasks, 10, 0)
                """,
            )
                .bind("jobId", jobId)
                .bind("status", status)
                .bind("failedTasks", failedTasks)
                .execute()
        }
    }

    private fun queryTask(taskId: String): Map<String, Any?> =
        queryTaskOrNull(taskId) ?: error("Task $taskId not found")

    private fun queryTaskOrNull(taskId: String): Map<String, Any?>? =
        jdbi.withHandle<Map<String, Any?>?, Exception> { h ->
            h.createQuery("SELECT * FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapToMap()
                .findOne()
                .orElse(null)
                ?.mapKeys { it.key.lowercase() }
        }

    private fun queryJob(jobId: String): Map<String, Any?> =
        jdbi.withHandle<Map<String, Any?>, Exception> { h ->
            h.createQuery("SELECT * FROM mr_job WHERE job_id = :jobId")
                .bind("jobId", jobId)
                .mapToMap()
                .one()
                .mapKeys { it.key.lowercase() }
        }
}
