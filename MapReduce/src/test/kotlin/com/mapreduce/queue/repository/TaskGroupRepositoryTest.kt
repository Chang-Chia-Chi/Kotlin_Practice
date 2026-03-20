package com.mapreduce.queue.repository

import com.mapreduce.TestH2Factory
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.model.TaskStatus
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

class TaskGroupRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var taskRepo: TaskRepository
    private lateinit var groupRepo: TaskGroupRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        taskRepo = TaskRepository(jdbi)
        val leaderManager = mock<LeaderManager>()
        whenever(leaderManager.isActive).thenReturn(false)
        groupRepo = TaskGroupRepository(jdbi, leaderManager)
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private fun createGroupWithTasks(
        groupId: String,
        taskCount: Int,
        maxRetries: Int = 3,
        onCompleteHandler: String? = "test.__phase_complete",
    ): List<EnqueueRequest> {
        val group = TaskGroup(
            groupId = groupId,
            groupType = "test",
            status = GroupStatus.ACTIVE,
            params = "{}",
            queue = "default",
            phase = "map",
            phaseTotal = taskCount,
            onCompleteHandler = onCompleteHandler,
        )
        val tasks = (0 until taskCount).map {
            EnqueueRequest(
                handler = "test.map", payload = "{\"i\":$it}",
                queue = "default", groupId = groupId, maxRetries = maxRetries,
            )
        }
        groupRepo.submitGroup(group, tasks)
        return tasks
    }

    private suspend fun claimAll(count: Int, queue: String = "default"): List<com.mapreduce.queue.model.Task> =
        (0 until count).map { taskRepo.claim("worker-$it", listOf(queue))!! }

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

    private fun callbackCount(groupId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'test.__phase_complete' AND payload = :gid",
            ).bind("gid", groupId).mapTo(Int::class.java).one()
        }

    // ── failGroupTask ───────────────────────────────────────────────────

    @Nested
    inner class FailGroupTask {

        @Test
        fun `retry remaining -- task goes PENDING, no group counter change`() = runTest {
            val groupId = "fg-retry"
            createGroupWithTasks(groupId, 3, maxRetries = 3)
            val claimed = claimAll(3)

            val result = groupRepo.failGroupTask(
                claimed[0].taskId, groupId, "transient error",
            )

            assertTrue(result.taskUpdated)
            assertFalse(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("PENDING", readStatus(claimed[0].taskId))
            assertEquals(1, readRetryCount(claimed[0].taskId))
            // Group counter unchanged (still 3 pending — task retried, not terminal)
            assertEquals(3, groupRepo.findGroup(groupId)!!.tasksPending)
            assertEquals(0, groupRepo.findGroup(groupId)!!.tasksFailed)
        }

        @Test
        fun `retry exhausted -- atomic dead-letter and group decrement`() = runTest {
            val groupId = "fg-exhaust"
            createGroupWithTasks(groupId, 2, maxRetries = 1)
            val claimed = claimAll(2)

            val result = groupRepo.failGroupTask(
                claimed[0].taskId, groupId, "fatal error",
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertFalse(result.barrierMet) // still 1 pending
            assertEquals("DEAD_LETTER", readStatus(claimed[0].taskId))
            assertEquals(1, readRetryCount(claimed[0].taskId))
            assertEquals(1, groupRepo.findGroup(groupId)!!.tasksPending)
            assertEquals(1, groupRepo.findGroup(groupId)!!.tasksFailed)
        }

        @Test
        fun `retry exhausted on last task -- barrier met`() = runTest {
            val groupId = "fg-barrier"
            createGroupWithTasks(groupId, 1, maxRetries = 1)
            val claimed = claimAll(1)

            val result = groupRepo.failGroupTask(
                claimed[0].taskId, groupId, "fatal",
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertTrue(result.barrierMet)
            assertEquals(0, groupRepo.findGroup(groupId)!!.tasksPending)
            assertEquals(1, callbackCount(groupId))
        }

        @Test
        fun `fenced out -- returns taskUpdated false`() = runTest {
            val groupId = "fg-fenced"
            createGroupWithTasks(groupId, 2, maxRetries = 1)
            val claimed = claimAll(2)

            val result = groupRepo.failGroupTask(
                claimed[0].taskId, groupId, "error",
                claimToken = "wrong-gen",
            )

            assertFalse(result.taskUpdated)
            assertFalse(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("CLAIMED", readStatus(claimed[0].taskId))
            assertEquals(2, groupRepo.findGroup(groupId)!!.tasksPending)
        }

        @Test
        fun `respects retryDelay`() = runTest {
            val groupId = "fg-delay"
            createGroupWithTasks(groupId, 1, maxRetries = 3)
            val claimed = claimAll(1)

            groupRepo.failGroupTask(
                claimed[0].taskId, groupId, "transient",
                retryDelay = Duration.ofSeconds(30),
            )

            val task = taskRepo.findById(claimed[0].taskId)!!
            assertEquals(TaskStatus.PENDING, task.status)
            assertNotNull(task.scheduledAt)
            assertTrue(task.scheduledAt!!.isAfter(Instant.now().plusSeconds(20)))
        }
    }

    // ── deadLetterGroupTask ─────────────────────────────────────────────

    @Nested
    inner class DeadLetterGroupTask {

        @Test
        fun `atomic dead-letter and group decrement`() = runTest {
            val groupId = "dl-basic"
            createGroupWithTasks(groupId, 3)
            val claimed = claimAll(3)

            val result = groupRepo.deadLetterGroupTask(
                claimed[0].taskId, groupId, "no handler",
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("DEAD_LETTER", readStatus(claimed[0].taskId))
            assertEquals("no handler", taskRepo.findById(claimed[0].taskId)!!.errorMessage)
            assertEquals(2, groupRepo.findGroup(groupId)!!.tasksPending)
            assertEquals(1, groupRepo.findGroup(groupId)!!.tasksFailed)
        }

        @Test
        fun `fenced out -- zombie rejected`() = runTest {
            val groupId = "dl-fenced"
            createGroupWithTasks(groupId, 2)
            val claimed = claimAll(2)

            val result = groupRepo.deadLetterGroupTask(
                claimed[0].taskId, groupId, "zombie",
                claimToken = "wrong-gen",
            )

            assertFalse(result.taskUpdated)
            assertFalse(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("CLAIMED", readStatus(claimed[0].taskId))
            assertEquals(2, groupRepo.findGroup(groupId)!!.tasksPending)
        }

        @Test
        fun `barrier fires when last task dead-lettered`() = runTest {
            val groupId = "dl-barrier"
            createGroupWithTasks(groupId, 2)
            val claimed = claimAll(2)

            // First task: success
            groupRepo.resolveGroupTask(
                taskId = claimed[0].taskId, groupId = groupId,
                claimToken = claimed[0].claimToken,
            )
            assertEquals(1, groupRepo.findGroup(groupId)!!.tasksPending)

            // Second task: dead-letter — should trigger barrier
            val result = groupRepo.deadLetterGroupTask(
                claimed[1].taskId, groupId, "poison pill",
                claimToken = claimed[1].claimToken,
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertTrue(result.barrierMet)
            assertEquals(0, groupRepo.findGroup(groupId)!!.tasksPending)
            assertEquals(1, callbackCount(groupId))
        }
    }

    // ── reclaimGroupTask ────────────────────────────────────────────────

    @Nested
    inner class ReclaimGroupTask {

        @Test
        fun `retry remaining -- task goes PENDING, no group counter change`() = runTest {
            val groupId = "rg-retry"
            createGroupWithTasks(groupId, 2, maxRetries = 3)
            val claimed = claimAll(2)

            val result = groupRepo.reclaimGroupTask(
                claimed[0].taskId, groupId, "pod died",
            )

            assertNotNull(result)
            assertTrue(result!!.taskUpdated)
            assertFalse(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("PENDING", readStatus(claimed[0].taskId))
            assertEquals(1, readRetryCount(claimed[0].taskId))
            // Claim fields cleared
            val task = taskRepo.findById(claimed[0].taskId)!!
            assertNull(task.claimedBy)
            assertNull(task.claimedAt)
            // Group unchanged
            assertEquals(2, groupRepo.findGroup(groupId)!!.tasksPending)
        }

        @Test
        fun `retry exhausted -- atomic dead-letter and group decrement`() = runTest {
            val groupId = "rg-exhaust"
            createGroupWithTasks(groupId, 2, maxRetries = 1)
            val claimed = claimAll(2)

            val result = groupRepo.reclaimGroupTask(
                claimed[0].taskId, groupId, "pod died",
            )

            assertNotNull(result)
            assertTrue(result!!.taskUpdated)
            assertTrue(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("DEAD_LETTER", readStatus(claimed[0].taskId))
            assertEquals(1, groupRepo.findGroup(groupId)!!.tasksPending)
            assertEquals(1, groupRepo.findGroup(groupId)!!.tasksFailed)
        }

        @Test
        fun `returns null when task already handled`() = runTest {
            val groupId = "rg-null"
            createGroupWithTasks(groupId, 1, maxRetries = 3)
            val claimed = claimAll(1)

            // Complete the task first
            groupRepo.resolveGroupTask(
                taskId = claimed[0].taskId, groupId = groupId,
                claimToken = claimed[0].claimToken,
            )
            assertEquals("COMPLETED", readStatus(claimed[0].taskId))

            // Reclaim should return null (not CLAIMED)
            val result = groupRepo.reclaimGroupTask(
                claimed[0].taskId, groupId, "pod died",
            )

            assertNull(result)
        }

        @Test
        fun `clears claim fields on reclaim`() = runTest {
            val groupId = "rg-clear"
            createGroupWithTasks(groupId, 1, maxRetries = 3)
            val claimed = claimAll(1)

            // Verify claim fields are set
            assertNotNull(taskRepo.findById(claimed[0].taskId)!!.claimedBy)

            groupRepo.reclaimGroupTask(claimed[0].taskId, groupId, "pod died")

            val task = taskRepo.findById(claimed[0].taskId)!!
            assertNull(task.claimedBy)
            assertNull(task.claimedAt)
            assertEquals(TaskStatus.PENDING, task.status)
        }

        @Test
        fun `reclaim exhausted on last task -- barrier met`() = runTest {
            val groupId = "rg-barrier"
            createGroupWithTasks(groupId, 1, maxRetries = 1)
            val claimed = claimAll(1)

            val result = groupRepo.reclaimGroupTask(
                claimed[0].taskId, groupId, "pod died",
            )

            assertNotNull(result)
            assertTrue(result!!.deadLettered)
            assertTrue(result.barrierMet)
            assertEquals(0, groupRepo.findGroup(groupId)!!.tasksPending)
            assertEquals(1, callbackCount(groupId))
        }
    }
}
