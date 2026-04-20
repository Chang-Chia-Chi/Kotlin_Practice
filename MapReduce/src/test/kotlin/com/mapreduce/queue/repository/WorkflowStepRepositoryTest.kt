package com.mapreduce.queue.repository

import com.mapreduce.TestH2Factory
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.StepStatus
import com.mapreduce.queue.model.WorkflowStep
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

class WorkflowStepRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var taskRepo: TaskRepository
    private lateinit var stepRepo: WorkflowStepRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        taskRepo = TaskRepository(jdbi)
        val leaderManager = mock<LeaderManager>()
        whenever(leaderManager.isActive).thenReturn(false)
        stepRepo = WorkflowStepRepository(jdbi, leaderManager)
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private fun createStepWithTasks(
        stepId: String,
        taskCount: Int,
        maxRetries: Int = 3,
        onCompleteHandler: String? = "test.__step_transition",
    ): List<EnqueueRequest> {
        val step = WorkflowStep(
            stepId = stepId,
            workflowName = "test",
            runId = stepId,
            status = StepStatus.ACTIVE,
            params = "{}",
            queue = "default",
            stepLabel = "map",
            stepTotal = taskCount,
            onCompleteHandler = onCompleteHandler,
        )
        val tasks = (0 until taskCount).map {
            EnqueueRequest(
                handler = "test.map", payload = "{\"i\":$it}",
                queue = "default", stepId = stepId, maxRetries = maxRetries,
            )
        }
        stepRepo.submitStep(step, tasks)
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

    private fun callbackCount(stepId: String): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'test.__step_transition' AND payload = :gid",
            ).bind("gid", stepId).mapTo(Int::class.java).one()
        }

    private fun getAllTaskIds(stepId: String): List<String> =
        jdbi.withHandle<List<String>, Exception> { h ->
            h.createQuery("SELECT task_id FROM task WHERE step_id = :stepId ORDER BY created_at")
                .bind("stepId", stepId)
                .mapTo(String::class.java).list()
        }

    // ── OptimisticBarrier ─────────────────────────────────────────────

    @Nested
    inner class OptimisticBarrier {

        @Test
        fun `tryDispatchBarrier returns true when all tasks completed`() {
            val stepId = "opt-all-done"
            createStepWithTasks(stepId, 2)
            // Manually mark all tasks COMPLETED (bypassing old barrier)
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'COMPLETED' WHERE step_id = :stepId")
                    .bind("stepId", stepId).execute()
            }

            val result = stepRepo.tryDispatchBarrier(stepId)

            assertTrue(result)
            assertEquals(1, callbackCount(stepId))
        }

        @Test
        fun `tryDispatchBarrier returns false when tasks still pending`() {
            val stepId = "opt-pending"
            createStepWithTasks(stepId, 2)

            val result = stepRepo.tryDispatchBarrier(stepId)

            assertFalse(result)
            assertEquals(0, callbackCount(stepId))
        }

        @Test
        fun `tryDispatchBarrier is idempotent — second call returns false`() {
            val stepId = "opt-idempotent"
            createStepWithTasks(stepId, 1)
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'COMPLETED' WHERE step_id = :stepId")
                    .bind("stepId", stepId).execute()
            }

            assertTrue(stepRepo.tryDispatchBarrier(stepId))
            assertFalse(stepRepo.tryDispatchBarrier(stepId))
            assertEquals(1, callbackCount(stepId))
        }

        @Test
        fun `tryDispatchBarrier returns false for non-ACTIVE step`() {
            val stepId = "opt-completed"
            createStepWithTasks(stepId, 1)
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'COMPLETED' WHERE step_id = :stepId")
                    .bind("stepId", stepId).execute()
                h.createUpdate("UPDATE workflow_step SET status = 'COMPLETED' WHERE step_id = :stepId")
                    .bind("stepId", stepId).execute()
            }

            assertFalse(stepRepo.tryDispatchBarrier(stepId))
        }

        @Test
        fun `tryDispatchBarrier returns false when step has no on_complete_handler`() {
            val stepId = "opt-no-handler"
            createStepWithTasks(stepId, 1, onCompleteHandler = null)
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'COMPLETED' WHERE step_id = :stepId")
                    .bind("stepId", stepId).execute()
            }

            assertFalse(stepRepo.tryDispatchBarrier(stepId))
        }

        @Test
        fun `tryDispatchBarrier returns true when mix of COMPLETED and DEAD_LETTER`() = runTest {
            val stepId = "opt-mixed"
            createStepWithTasks(stepId, 2)
            val taskIds = claimAll(2).map { it.taskId }
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'COMPLETED' WHERE task_id = :id")
                    .bind("id", taskIds[0]).execute()
                h.createUpdate("UPDATE task SET status = 'DEAD_LETTER' WHERE task_id = :id")
                    .bind("id", taskIds[1]).execute()
            }

            assertTrue(stepRepo.tryDispatchBarrier(stepId))
        }
    }

    // ── OnDemandCounts ────────────────────────────────────────────────

    @Nested
    inner class OnDemandCounts {

        @Test
        fun `countPendingTasks returns count of PENDING and CLAIMED tasks`() {
            val stepId = "count-pending"
            createStepWithTasks(stepId, 3)
            // 1 PENDING, 1 CLAIMED, 1 COMPLETED
            val taskIds = getAllTaskIds(stepId)
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'CLAIMED' WHERE task_id = :id")
                    .bind("id", taskIds[0]).execute()
                h.createUpdate("UPDATE task SET status = 'COMPLETED' WHERE task_id = :id")
                    .bind("id", taskIds[1]).execute()
            }

            assertEquals(2, stepRepo.countPendingTasks(stepId)) // 1 PENDING + 1 CLAIMED
        }

        @Test
        fun `countFailedTasks returns count of DEAD_LETTER tasks`() {
            val stepId = "count-failed"
            createStepWithTasks(stepId, 3)
            val taskIds = getAllTaskIds(stepId)
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'DEAD_LETTER' WHERE task_id = :id")
                    .bind("id", taskIds[0]).execute()
                h.createUpdate("UPDATE task SET status = 'DEAD_LETTER' WHERE task_id = :id")
                    .bind("id", taskIds[1]).execute()
            }

            assertEquals(2, stepRepo.countFailedTasks(stepId))
        }

        @Test
        fun `countPendingTasks returns zero for completed step`() {
            val stepId = "count-zero"
            createStepWithTasks(stepId, 2)
            jdbi.useHandle<Exception> { h ->
                h.createUpdate("UPDATE task SET status = 'COMPLETED' WHERE step_id = :stepId")
                    .bind("stepId", stepId).execute()
            }

            assertEquals(0, stepRepo.countPendingTasks(stepId))
        }

        @Test
        fun `countFailedTasks returns zero when no failures`() {
            val stepId = "count-no-fail"
            createStepWithTasks(stepId, 2)

            assertEquals(0, stepRepo.countFailedTasks(stepId))
        }
    }

    // ── failStepTask ───────────────────────────────────────────────────

    @Nested
    inner class FailStepTask {

        @Test
        fun `retry remaining -- task goes PENDING, no group counter change`() = runTest {
            val stepId = "fg-retry"
            createStepWithTasks(stepId, 3, maxRetries = 3)
            val claimed = claimAll(3)

            val result = stepRepo.failStepTask(
                claimed[0].taskId, stepId, "transient error",
            )

            assertTrue(result.taskUpdated)
            assertFalse(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("PENDING", readStatus(claimed[0].taskId))
            assertEquals(1, readRetryCount(claimed[0].taskId))
            // Pending unchanged (task retried, not terminal)
            assertEquals(3, stepRepo.countPendingTasks(stepId))
            assertEquals(0, stepRepo.countFailedTasks(stepId))
        }

        @Test
        fun `retry exhausted -- atomic dead-letter and group decrement`() = runTest {
            val stepId = "fg-exhaust"
            createStepWithTasks(stepId, 2, maxRetries = 1)
            val claimed = claimAll(2)

            val result = stepRepo.failStepTask(
                claimed[0].taskId, stepId, "fatal error",
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertFalse(result.barrierMet) // still 1 pending
            assertEquals("DEAD_LETTER", readStatus(claimed[0].taskId))
            assertEquals(1, readRetryCount(claimed[0].taskId))
            assertEquals(1, stepRepo.countPendingTasks(stepId))
            assertEquals(1, stepRepo.countFailedTasks(stepId))
        }

        @Test
        fun `retry exhausted on last task -- barrier met`() = runTest {
            val stepId = "fg-barrier"
            createStepWithTasks(stepId, 1, maxRetries = 1)
            val claimed = claimAll(1)

            val result = stepRepo.failStepTask(
                claimed[0].taskId, stepId, "fatal",
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertTrue(result.barrierMet)
            assertEquals(0, stepRepo.countPendingTasks(stepId))
            assertEquals(1, callbackCount(stepId))
        }

        @Test
        fun `fenced out -- returns taskUpdated false`() = runTest {
            val stepId = "fg-fenced"
            createStepWithTasks(stepId, 2, maxRetries = 1)
            val claimed = claimAll(2)

            val result = stepRepo.failStepTask(
                claimed[0].taskId, stepId, "error",
                claimToken = "wrong-gen",
            )

            assertFalse(result.taskUpdated)
            assertFalse(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("CLAIMED", readStatus(claimed[0].taskId))
            assertEquals(2, stepRepo.countPendingTasks(stepId))
        }

        @Test
        fun `respects retryDelay`() = runTest {
            val stepId = "fg-delay"
            createStepWithTasks(stepId, 1, maxRetries = 3)
            val claimed = claimAll(1)

            stepRepo.failStepTask(
                claimed[0].taskId, stepId, "transient",
                retryDelay = Duration.ofSeconds(30),
            )

            val task = taskRepo.findById(claimed[0].taskId)!!
            assertEquals(TaskStatus.PENDING, task.status)
            assertNotNull(task.scheduledAt)
            assertTrue(task.scheduledAt!!.isAfter(Instant.now().plusSeconds(20)))
        }
    }

    // ── deadLetterStepTask ─────────────────────────────────────────────

    @Nested
    inner class DeadLetterStepTask {

        @Test
        fun `atomic dead-letter and group decrement`() = runTest {
            val stepId = "dl-basic"
            createStepWithTasks(stepId, 3)
            val claimed = claimAll(3)

            val result = stepRepo.deadLetterStepTask(
                claimed[0].taskId, stepId, "no handler",
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("DEAD_LETTER", readStatus(claimed[0].taskId))
            assertEquals("no handler", taskRepo.findById(claimed[0].taskId)!!.errorMessage)
            assertEquals(2, stepRepo.countPendingTasks(stepId))
            assertEquals(1, stepRepo.countFailedTasks(stepId))
        }

        @Test
        fun `fenced out -- zombie rejected`() = runTest {
            val stepId = "dl-fenced"
            createStepWithTasks(stepId, 2)
            val claimed = claimAll(2)

            val result = stepRepo.deadLetterStepTask(
                claimed[0].taskId, stepId, "zombie",
                claimToken = "wrong-gen",
            )

            assertFalse(result.taskUpdated)
            assertFalse(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("CLAIMED", readStatus(claimed[0].taskId))
            assertEquals(2, stepRepo.countPendingTasks(stepId))
        }

        @Test
        fun `barrier fires when last task dead-lettered`() = runTest {
            val stepId = "dl-barrier"
            createStepWithTasks(stepId, 2)
            val claimed = claimAll(2)

            // First task: success
            stepRepo.resolveStepTask(
                taskId = claimed[0].taskId, stepId = stepId,
                claimToken = claimed[0].claimToken,
            )
            assertEquals(1, stepRepo.countPendingTasks(stepId))

            // Second task: dead-letter — should trigger barrier
            val result = stepRepo.deadLetterStepTask(
                claimed[1].taskId, stepId, "poison pill",
                claimToken = claimed[1].claimToken,
            )

            assertTrue(result.taskUpdated)
            assertTrue(result.deadLettered)
            assertTrue(result.barrierMet)
            assertEquals(0, stepRepo.countPendingTasks(stepId))
            assertEquals(1, callbackCount(stepId))
        }
    }

    // ── reclaimStepTask ────────────────────────────────────────────────

    @Nested
    inner class ReclaimStepTask {

        @Test
        fun `retry remaining -- task goes PENDING, no group counter change`() = runTest {
            val stepId = "rg-retry"
            createStepWithTasks(stepId, 2, maxRetries = 3)
            val claimed = claimAll(2)

            val result = stepRepo.reclaimStepTask(
                claimed[0].taskId, stepId, "pod died",
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
            // Pending unchanged
            assertEquals(2, stepRepo.countPendingTasks(stepId))
        }

        @Test
        fun `retry exhausted -- atomic dead-letter and group decrement`() = runTest {
            val stepId = "rg-exhaust"
            createStepWithTasks(stepId, 2, maxRetries = 1)
            val claimed = claimAll(2)

            val result = stepRepo.reclaimStepTask(
                claimed[0].taskId, stepId, "pod died",
            )

            assertNotNull(result)
            assertTrue(result!!.taskUpdated)
            assertTrue(result.deadLettered)
            assertFalse(result.barrierMet)
            assertEquals("DEAD_LETTER", readStatus(claimed[0].taskId))
            assertEquals(1, stepRepo.countPendingTasks(stepId))
            assertEquals(1, stepRepo.countFailedTasks(stepId))
        }

        @Test
        fun `returns null when task already handled`() = runTest {
            val stepId = "rg-null"
            createStepWithTasks(stepId, 1, maxRetries = 3)
            val claimed = claimAll(1)

            // Complete the task first
            stepRepo.resolveStepTask(
                taskId = claimed[0].taskId, stepId = stepId,
                claimToken = claimed[0].claimToken,
            )
            assertEquals("COMPLETED", readStatus(claimed[0].taskId))

            // Reclaim should return null (not CLAIMED)
            val result = stepRepo.reclaimStepTask(
                claimed[0].taskId, stepId, "pod died",
            )

            assertNull(result)
        }

        @Test
        fun `clears claim fields on reclaim`() = runTest {
            val stepId = "rg-clear"
            createStepWithTasks(stepId, 1, maxRetries = 3)
            val claimed = claimAll(1)

            // Verify claim fields are set
            assertNotNull(taskRepo.findById(claimed[0].taskId)!!.claimedBy)

            stepRepo.reclaimStepTask(claimed[0].taskId, stepId, "pod died")

            val task = taskRepo.findById(claimed[0].taskId)!!
            assertNull(task.claimedBy)
            assertNull(task.claimedAt)
            assertEquals(TaskStatus.PENDING, task.status)
        }

        @Test
        fun `reclaim exhausted on last task -- barrier met`() = runTest {
            val stepId = "rg-barrier"
            createStepWithTasks(stepId, 1, maxRetries = 1)
            val claimed = claimAll(1)

            val result = stepRepo.reclaimStepTask(
                claimed[0].taskId, stepId, "pod died",
            )

            assertNotNull(result)
            assertTrue(result!!.deadLettered)
            assertTrue(result.barrierMet)
            assertEquals(0, stepRepo.countPendingTasks(stepId))
            assertEquals(1, callbackCount(stepId))
        }
    }
}
