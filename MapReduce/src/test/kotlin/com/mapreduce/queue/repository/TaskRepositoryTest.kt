package com.mapreduce.queue.repository

import com.mapreduce.TestH2Factory
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.TaskStatus
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue

class TaskRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var repo: TaskRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        repo = TaskRepository(jdbi)
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** Insert a task row directly, bypassing enqueue(). Useful for CLAIMED-state setup. */
    private fun insertTask(
        taskId: String,
        handler: String = "test.handler",
        queue: String = "default",
        payload: String = "{}",
        status: TaskStatus = TaskStatus.PENDING,
        priority: Int = 0,
        groupId: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        claimToken: String? = null,
        lastEpoch: Long = 0,
        scheduledAt: Instant? = null,
        errorMessage: String? = null,
    ) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, claimed_by, claimed_at, retry_count, max_retries, execution_generation,
                    last_epoch, scheduled_at, error_message, created_at)
                VALUES (:taskId, :handler, :queue, :payload, :status, :priority,
                    :groupId, :claimedBy, :claimedAt, :retryCount, :maxRetries, :gen,
                    :epoch, :scheduledAt, :errorMessage, CURRENT_TIMESTAMP)
                """
            )
                .bind("taskId", taskId)
                .bind("handler", handler)
                .bind("queue", queue)
                .bind("payload", payload)
                .bind("status", status.name)
                .bind("priority", priority)
                .bind("groupId", groupId)
                .bind("claimedBy", claimedBy)
                .bind("claimedAt", claimedAt)
                .bind("retryCount", retryCount)
                .bind("maxRetries", maxRetries)
                .bind("gen", claimToken)
                .bind("epoch", lastEpoch)
                .bind("scheduledAt", scheduledAt)
                .bind("errorMessage", errorMessage)
                .execute()
        }
    }

    private fun readStatus(taskId: String): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT status FROM task WHERE task_id = :id")
                .bind("id", taskId)
                .mapTo(String::class.java)
                .one()
        }

    private fun readRetryCount(taskId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT retry_count FROM task WHERE task_id = :id")
                .bind("id", taskId)
                .mapTo(Int::class.java)
                .one()
        }

    // ── Enqueue ─────────────────────────────────────────────────────────

    @Nested
    inner class Enqueue {

        @Test
        fun `creates a PENDING task with correct fields`() {
            val request = EnqueueRequest(
                handler = "word.count",
                payload = """{"text":"hello"}""",
                queue = "high",
                maxRetries = 5,
                priority = 10,
                groupId = "job-1",
                metadata = """{"source":"test"}""",
            )
            val id = repo.enqueue(request)

            val task = repo.findById(id)!!
            assertEquals(TaskStatus.PENDING, task.status)
            assertEquals("word.count", task.handler)
            assertEquals("""{"text":"hello"}""", task.payload)
            assertEquals("high", task.queue)
            assertEquals(5, task.maxRetries)
            assertEquals(10, task.priority)
            assertEquals("job-1", task.groupId)
            assertEquals("""{"source":"test"}""", task.metadata)
            assertEquals(0, task.retryCount)
            assertNotNull(task.createdAt)
            assertNull(task.claimedBy)
            assertNull(task.claimedAt)
            assertNull(task.completedAt)
        }

        @Test
        fun `generates unique task IDs`() {
            val req = EnqueueRequest(handler = "h", payload = "{}")
            val ids = (1..20).map { repo.enqueue(req) }.toSet()
            assertEquals(20, ids.size)
        }
    }

    // ── Claim ───────────────────────────────────────────────────────────

    @Nested
    inner class Claim {

        @Test
        fun `returns null when no tasks available`() {
            assertNull(repo.claim("worker-1", listOf("default")))
        }

        @Test
        fun `returns null when empty queues list`() {
            repo.enqueue(EnqueueRequest(handler = "h", payload = "{}"))
            assertNull(repo.claim("worker-1", emptyList()))
        }

        @Test
        fun `returns task and sets CLAIMED status`() {
            val taskId = repo.enqueue(EnqueueRequest(handler = "h", payload = """{"k":1}"""))

            val claimed = repo.claim("worker-1", listOf("default"))!!
            assertEquals(taskId, claimed.taskId)
            assertEquals(TaskStatus.CLAIMED, claimed.status)
            assertEquals("worker-1", claimed.claimedBy)
            assertNotNull(claimed.claimToken)

            // Verify persisted state
            val persisted = repo.findById(taskId)!!
            assertEquals(TaskStatus.CLAIMED, persisted.status)
            assertEquals("worker-1", persisted.claimedBy)
            assertNotNull(persisted.claimedAt)
        }

        @Test
        fun `respects queue filter`() {
            repo.enqueue(EnqueueRequest(handler = "h", payload = "{}", queue = "alpha"))
            repo.enqueue(EnqueueRequest(handler = "h", payload = "{}", queue = "beta"))

            val claimed = repo.claim("w", listOf("beta"))!!
            assertEquals("beta", claimed.queue)
        }

        @Test
        fun `respects priority ordering -- higher priority first`() {
            repo.enqueue(EnqueueRequest(handler = "low", payload = "{}", priority = 1))
            repo.enqueue(EnqueueRequest(handler = "high", payload = "{}", priority = 10))
            repo.enqueue(EnqueueRequest(handler = "mid", payload = "{}", priority = 5))

            val first = repo.claim("w", listOf("default"))!!
            assertEquals("high", first.handler)

            val second = repo.claim("w", listOf("default"))!!
            assertEquals("mid", second.handler)
        }

        @Test
        fun `does not claim future-scheduled tasks`() {
            val futureTime = Instant.now().plus(1, ChronoUnit.HOURS)
            repo.enqueue(
                EnqueueRequest(handler = "future", payload = "{}", scheduledAt = futureTime)
            )

            assertNull(repo.claim("w", listOf("default")))
        }

        @Test
        fun `sets execution_generation on claim`() {
            repo.enqueue(EnqueueRequest(handler = "h", payload = "{}"))

            val claimed = repo.claim("w", listOf("default"))!!
            assertNotNull(claimed.claimToken)
            assertEquals(36, claimed.claimToken!!.length) // UUID format
        }
    }

    // ── Complete ─────────────────────────────────────────────────────────

    @Nested
    inner class Complete {

        @Test
        fun `sets status to COMPLETED`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w")

            repo.complete("t-1")

            val task = repo.findById("t-1")!!
            assertEquals(TaskStatus.COMPLETED, task.status)
            assertNotNull(task.completedAt)
        }

        @Test
        fun `succeeds when execution_generation matches`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", claimToken = "gen-A")

            repo.complete("t-1", claimToken = "gen-A")

            assertEquals("COMPLETED", readStatus("t-1"))
        }

        @Test
        fun `no-op when execution_generation mismatches -- zombie protection`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", claimToken = "gen-A")

            repo.complete("t-1", claimToken = "gen-WRONG")

            // Should still be CLAIMED -- the zombie's complete was rejected
            assertEquals("CLAIMED", readStatus("t-1"))
        }
    }

    // ── Fail ────────────────────────────────────────────────────────────

    @Nested
    inner class Fail {

        @Test
        fun `increments retry_count`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", retryCount = 0, maxRetries = 3)

            repo.fail("t-1", "boom")

            assertEquals(1, readRetryCount("t-1"))
        }

        @Test
        fun `returns false when retries remaining -- task goes to PENDING`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", retryCount = 0, maxRetries = 3)

            val deadLettered = repo.fail("t-1", "transient error")

            assertFalse(deadLettered)
            assertEquals("PENDING", readStatus("t-1"))

            val task = repo.findById("t-1")!!
            assertNull(task.claimedBy)
            assertNull(task.claimedAt)
        }

        @Test
        fun `returns true when retries exhausted -- task goes to DEAD_LETTER`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", retryCount = 2, maxRetries = 3)

            val deadLettered = repo.fail("t-1", "fatal error")

            assertTrue(deadLettered)
            assertEquals("DEAD_LETTER", readStatus("t-1"))
        }

        @Test
        fun `no-op when execution_generation mismatches`() {
            insertTask(
                "t-1", status = TaskStatus.CLAIMED, claimedBy = "w",
                claimToken = "gen-A", retryCount = 0, maxRetries = 3,
            )

            val result = repo.fail("t-1", "error", claimToken = "gen-WRONG")

            // Returns false because 0 rows updated (early return)
            assertFalse(result)
            // Status unchanged
            assertEquals("CLAIMED", readStatus("t-1"))
            assertEquals(0, readRetryCount("t-1"))
        }

        @Test
        fun `sets error_message`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", retryCount = 0, maxRetries = 3)

            repo.fail("t-1", "NullPointerException at line 42")

            val task = repo.findById("t-1")!!
            assertEquals("NullPointerException at line 42", task.errorMessage)
        }

        @Test
        fun `with matching execution_generation succeeds`() {
            insertTask(
                "t-1", status = TaskStatus.CLAIMED, claimedBy = "w",
                claimToken = "gen-A", retryCount = 0, maxRetries = 3,
            )

            val deadLettered = repo.fail("t-1", "error", claimToken = "gen-A")

            assertFalse(deadLettered)
            assertEquals("PENDING", readStatus("t-1"))
            assertEquals(1, readRetryCount("t-1"))
        }
    }

    // ── Requeue ─────────────────────────────────────────────────────────

    @Nested
    inner class Requeue {

        @Test
        fun `moves task to PENDING without incrementing retry_count`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", retryCount = 1, maxRetries = 3)

            repo.requeue("t-1")

            val task = repo.findById("t-1")!!
            assertEquals(TaskStatus.PENDING, task.status)
            assertEquals(1, task.retryCount) // unchanged
            assertNull(task.claimedBy)
            assertNull(task.claimedAt)
        }

        @Test
        fun `with matching execution_generation succeeds`() {
            insertTask(
                "t-1", status = TaskStatus.CLAIMED, claimedBy = "w",
                claimToken = "gen-A", retryCount = 2, maxRetries = 3,
            )

            repo.requeue("t-1", claimToken = "gen-A")

            assertEquals("PENDING", readStatus("t-1"))
            assertEquals(2, readRetryCount("t-1")) // unchanged
        }

        @Test
        fun `no-op when execution_generation mismatches`() {
            insertTask(
                "t-1", status = TaskStatus.CLAIMED, claimedBy = "w",
                claimToken = "gen-A",
            )

            repo.requeue("t-1", claimToken = "gen-WRONG")

            assertEquals("CLAIMED", readStatus("t-1"))
        }
    }

    // ── Dead Letter ─────────────────────────────────────────────────────

    @Nested
    inner class DeadLetter {

        @Test
        fun `sets status to DEAD_LETTER with reason and returns true`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w")

            val result = repo.deadLetter("t-1", "unrecognized handler: bad.handler")

            assertTrue(result)
            val task = repo.findById("t-1")!!
            assertEquals(TaskStatus.DEAD_LETTER, task.status)
            assertEquals("unrecognized handler: bad.handler", task.errorMessage)
        }

        @Test
        fun `with matching claimToken succeeds`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", claimToken = "gen-A")

            val result = repo.deadLetter("t-1", "bad handler", claimToken = "gen-A")

            assertTrue(result)
            assertEquals("DEAD_LETTER", readStatus("t-1"))
        }

        @Test
        fun `with mismatching claimToken returns false -- zombie rejected`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "w", claimToken = "gen-A")

            val result = repo.deadLetter("t-1", "bad handler", claimToken = "gen-WRONG")

            assertFalse(result)
            assertEquals("CLAIMED", readStatus("t-1"))
        }

        @Test
        fun `no-op on PENDING task -- status guard`() {
            insertTask("t-1", status = TaskStatus.PENDING)

            val result = repo.deadLetter("t-1", "poison pill")

            assertFalse(result)
            assertEquals("PENDING", readStatus("t-1"))
        }

        @Test
        fun `no-op on COMPLETED task -- status guard`() {
            insertTask("t-1", status = TaskStatus.COMPLETED)

            val result = repo.deadLetter("t-1", "zombie attempt")

            assertFalse(result)
            assertEquals("COMPLETED", readStatus("t-1"))
        }

        @Test
        fun `no-op on already DEAD_LETTER task -- status guard`() {
            insertTask("t-1", status = TaskStatus.DEAD_LETTER, errorMessage = "original reason")

            val result = repo.deadLetter("t-1", "zombie attempt")

            assertFalse(result)
            assertEquals("DEAD_LETTER", readStatus("t-1"))
            // Original error message preserved
            assertEquals("original reason", repo.findById("t-1")!!.errorMessage)
        }

        @Test
        fun `zombie lifecycle -- reclaim then re-claim then zombie deadLetter rejected`() {
            val taskId = repo.enqueue(EnqueueRequest(handler = "h", payload = "{}", maxRetries = 3))

            // Worker A claims
            val claimA = repo.claim("worker-A", listOf("default"))!!
            val genA = claimA.claimToken!!

            // Reaper reclaims
            repo.reclaimStaleTask(taskId, "worker-A presumed dead")

            // Worker B claims
            val claimB = repo.claim("worker-B", listOf("default"))!!

            // Zombie worker A tries to deadLetter with stale generation
            val zombieResult = repo.deadLetter(taskId, "zombie deadLetter", claimToken = genA)
            assertFalse(zombieResult)
            assertEquals("CLAIMED", readStatus(taskId))

            // Worker B can still operate on the task
            repo.complete(taskId, claimB.claimToken)
            assertEquals("COMPLETED", readStatus(taskId))
        }
    }

    // ── Find Stale Tasks ────────────────────────────────────────────────

    @Nested
    inner class FindStaleTasks {

        @Test
        fun `returns tasks with old claimed_at`() {
            val staleTime = Instant.now().minus(10, ChronoUnit.MINUTES)
            insertTask("stale-1", status = TaskStatus.CLAIMED, claimedBy = "w", claimedAt = staleTime)
            insertTask("stale-2", status = TaskStatus.CLAIMED, claimedBy = "w", claimedAt = staleTime)

            val threshold = Instant.now().minus(5, ChronoUnit.MINUTES)
            val stale = repo.findStaleTasks(threshold)

            assertEquals(2, stale.size)
            assertTrue(stale.map { it.taskId }.containsAll(listOf("stale-1", "stale-2")))
        }

        @Test
        fun `does not return tasks with recent claimed_at`() {
            insertTask("fresh", status = TaskStatus.CLAIMED, claimedBy = "w", claimedAt = Instant.now())

            val threshold = Instant.now().minus(5, ChronoUnit.MINUTES)
            val stale = repo.findStaleTasks(threshold)

            assertTrue(stale.isEmpty())
        }

        @Test
        fun `respects batchSize limit`() {
            val staleTime = Instant.now().minus(10, ChronoUnit.MINUTES)
            repeat(5) { i ->
                insertTask("stale-$i", status = TaskStatus.CLAIMED, claimedBy = "w", claimedAt = staleTime)
            }

            val stale = repo.findStaleTasks(Instant.now(), batchSize = 2)

            assertEquals(2, stale.size)
        }

        @Test
        fun `ignores non-CLAIMED tasks`() {
            val staleTime = Instant.now().minus(10, ChronoUnit.MINUTES)
            insertTask("pending", status = TaskStatus.PENDING)
            insertTask("completed", status = TaskStatus.COMPLETED)
            insertTask("dead", status = TaskStatus.DEAD_LETTER)
            insertTask("claimed", status = TaskStatus.CLAIMED, claimedBy = "w", claimedAt = staleTime)

            val threshold = Instant.now()
            val stale = repo.findStaleTasks(threshold)

            assertEquals(1, stale.size)
            assertEquals("claimed", stale[0].taskId)
        }
    }

    // ── Reclaim Stale Task ──────────────────────────────────────────────

    @Nested
    inner class ReclaimStaleTask {

        @Test
        fun `with retries remaining -- reclaims to PENDING and returns false`() {
            insertTask(
                "t-1", status = TaskStatus.CLAIMED, claimedBy = "dead-pod",
                retryCount = 0, maxRetries = 3,
            )

            val result = repo.reclaimStaleTask("t-1", errorMessage = "pod died")

            assertNotNull(result)
            assertFalse(result!!)

            val task = repo.findById("t-1")!!
            assertEquals(TaskStatus.PENDING, task.status)
            assertEquals(1, task.retryCount)
            assertNull(task.claimedBy)
            assertNull(task.claimedAt)
            assertEquals("pod died", task.errorMessage)
        }

        @Test
        fun `with retries exhausted -- dead-letters and returns true`() {
            insertTask(
                "t-1", status = TaskStatus.CLAIMED, claimedBy = "dead-pod",
                retryCount = 2, maxRetries = 3,
            )

            val result = repo.reclaimStaleTask("t-1", errorMessage = "pod died")

            assertNotNull(result)
            assertTrue(result!!)

            val task = repo.findById("t-1")!!
            assertEquals(TaskStatus.DEAD_LETTER, task.status)
            assertEquals(3, task.retryCount)
        }

        @Test
        fun `returns null when task is not CLAIMED`() {
            insertTask("t-1", status = TaskStatus.PENDING)

            val result = repo.reclaimStaleTask("t-1", errorMessage = "reclaim")

            assertNull(result) // 0 rows updated — status CAS failed
        }

        @Test
        fun `idempotent -- second reclaim returns null`() {
            insertTask(
                "t-1", status = TaskStatus.CLAIMED, claimedBy = "dead-pod",
                retryCount = 0, maxRetries = 3,
            )

            val first = repo.reclaimStaleTask("t-1", errorMessage = "reclaim")
            val second = repo.reclaimStaleTask("t-1", errorMessage = "reclaim again")

            assertNotNull(first)
            assertNull(second) // already PENDING, not CLAIMED
        }
    }

    // ── Count and Find ──────────────────────────────────────────────────

    @Nested
    inner class CountAndFind {

        @Test
        fun `countByGroupAndStatus returns correct count`() {
            insertTask("t-1", groupId = "job-1", status = TaskStatus.PENDING)
            insertTask("t-2", groupId = "job-1", status = TaskStatus.PENDING)
            insertTask("t-3", groupId = "job-1", status = TaskStatus.COMPLETED)
            insertTask("t-4", groupId = "job-2", status = TaskStatus.PENDING)

            assertEquals(2, repo.countByGroupAndStatus("job-1", TaskStatus.PENDING))
            assertEquals(1, repo.countByGroupAndStatus("job-1", TaskStatus.COMPLETED))
            assertEquals(0, repo.countByGroupAndStatus("job-1", TaskStatus.DEAD_LETTER))
            assertEquals(1, repo.countByGroupAndStatus("job-2", TaskStatus.PENDING))
        }

        @Test
        fun `findById returns task`() {
            insertTask("t-1", handler = "my.handler", payload = """{"x":1}""", priority = 7)

            val task = repo.findById("t-1")!!

            assertEquals("t-1", task.taskId)
            assertEquals("my.handler", task.handler)
            assertEquals("""{"x":1}""", task.payload)
            assertEquals(7, task.priority)
        }

        @Test
        fun `findById returns null for non-existent task`() {
            assertNull(repo.findById("does-not-exist"))
        }

        @Test
        fun `releaseTasksByPod releases CLAIMED tasks for pod`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "pod-A")
            insertTask("t-2", status = TaskStatus.CLAIMED, claimedBy = "pod-A")
            insertTask("t-3", status = TaskStatus.CLAIMED, claimedBy = "pod-B")
            insertTask("t-4", status = TaskStatus.COMPLETED, claimedBy = "pod-A") // not CLAIMED

            val released = repo.releaseTasksByPod("pod-A")

            assertEquals(2, released)

            // Released tasks should be PENDING with cleared fields
            val t1 = repo.findById("t-1")!!
            assertEquals(TaskStatus.PENDING, t1.status)
            assertNull(t1.claimedBy)
            assertNull(t1.claimedAt)
            assertNull(t1.scheduledAt)

            // pod-B task unaffected
            assertEquals("CLAIMED", readStatus("t-3"))
        }

        @Test
        fun `releaseTasksByPod returns 0 when pod has no claimed tasks`() {
            insertTask("t-1", status = TaskStatus.CLAIMED, claimedBy = "pod-B")

            assertEquals(0, repo.releaseTasksByPod("pod-A"))
        }
    }

    // ── Integration: enqueue then claim then complete ────────────────────

    @Nested
    inner class EndToEnd {

        @Test
        fun `enqueue - claim - complete lifecycle`() {
            val taskId = repo.enqueue(EnqueueRequest(handler = "h", payload = """{"v":1}"""))

            val claimed = repo.claim("worker-1", listOf("default"))!!
            assertEquals(taskId, claimed.taskId)
            assertEquals(TaskStatus.CLAIMED, claimed.status)

            repo.complete(taskId, claimed.claimToken)

            val completed = repo.findById(taskId)!!
            assertEquals(TaskStatus.COMPLETED, completed.status)
            assertNotNull(completed.completedAt)
        }

        @Test
        fun `enqueue - claim - fail - re-claim lifecycle`() {
            val taskId = repo.enqueue(
                EnqueueRequest(handler = "h", payload = "{}", maxRetries = 3)
            )

            // First attempt: claim and fail
            val first = repo.claim("w", listOf("default"))!!
            val dead = repo.fail(taskId, "oops", claimToken = first.claimToken)
            assertFalse(dead)
            assertEquals(1, readRetryCount(taskId))

            // Second attempt: should be claimable again
            val second = repo.claim("w", listOf("default"))!!
            assertEquals(taskId, second.taskId)
            assertNotEquals(first.claimToken, second.claimToken)
        }
    }
}
