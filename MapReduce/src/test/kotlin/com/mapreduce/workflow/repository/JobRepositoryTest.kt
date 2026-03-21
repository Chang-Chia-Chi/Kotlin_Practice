package com.mapreduce.workflow.repository

import com.mapreduce.TestH2Factory
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.StepStatus
import com.mapreduce.queue.model.WorkflowStep
import com.mapreduce.queue.repository.WorkflowStepRepository
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class JobRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var repo: WorkflowStepRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        val leaderManager = mock<LeaderManager>()
        whenever(leaderManager.isActive).thenReturn(false)
        repo = WorkflowStepRepository(jdbi, leaderManager)
    }

    // ── submitStep ───────────────────────────────────────────────

    @Test
    fun `submitStep inserts step and task rows atomically`() = runTest {
        val step = testStep("s-1", stepTotal = 3)
        val tasks = (0 until 3).map { testTask("s-1", "wc.map", "input-$it") }

        repo.submitStep(step, tasks)

        val found = repo.findStep("s-1")
        assertNotNull(found)
        assertEquals("wc", found!!.workflowName)
        assertEquals(StepStatus.ACTIVE, found.status)
        assertEquals(3, found.stepTotal)

        val taskCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE step_id = 's-1'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(3, taskCount)
    }

    @Test
    fun `submitStep creates tasks with correct handler name`() = runTest {
        val step = testStep("s-h", stepTotal = 1)
        val tasks = listOf(testTask("s-h", "email.map", "a"))

        repo.submitStep(step, tasks)

        val handler = jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT handler FROM task WHERE step_id = 's-h'")
                .mapTo(String::class.java).one()
        }
        assertEquals("email.map", handler)
    }

    // ── findStep ─────────────────────────────────────────────────

    @Test
    fun `findStep returns null for nonexistent step`() = runTest {
        assertNull(repo.findStep("nonexistent"))
    }

    @Test
    fun `findStep returns the step with all fields`() = runTest {
        val step = testStep("s-f", failurePolicy = "THRESHOLD", failureThreshold = 0.5)
        repo.submitStep(step, listOf(testTask("s-f", "wc.map", "i")))

        val found = repo.findStep("s-f")
        assertNotNull(found)
        assertEquals("s-f", found!!.stepId)
        assertEquals("THRESHOLD", found.failurePolicy)
        assertEquals(0.5, found.failureThreshold)
    }

    // ── casStepStatus ────────────────────────────────────────────

    @Test
    fun `casStepStatus succeeds with correct version and status`() = runTest {
        val step = testStep("s-cas")
        repo.submitStep(step, listOf(testTask("s-cas", "wc.map", "a")))

        val result = repo.casStepStatus("s-cas", StepStatus.ACTIVE, StepStatus.COMPLETED, 0)
        assertTrue(result)

        val updated = repo.findStep("s-cas")!!
        assertEquals(StepStatus.COMPLETED, updated.status)
        assertEquals(1, updated.version)
    }

    @Test
    fun `casStepStatus fails with wrong version`() = runTest {
        val step = testStep("s-cv")
        repo.submitStep(step, listOf(testTask("s-cv", "wc.map", "a")))

        val result = repo.casStepStatus("s-cv", StepStatus.ACTIVE, StepStatus.COMPLETED, 999)
        assertFalse(result)

        assertEquals(StepStatus.ACTIVE, repo.findStep("s-cv")!!.status)
    }

    @Test
    fun `casStepStatus fails with wrong expected status`() = runTest {
        val step = testStep("s-cs")
        repo.submitStep(step, listOf(testTask("s-cs", "wc.map", "a")))

        val result = repo.casStepStatus("s-cs", StepStatus.COMPLETED, StepStatus.FAILED, 0)
        assertFalse(result)
    }

    // ── resolveStepTask (success path) ────────────────────────────

    @Test
    fun `resolveStepTask marks task completed and detects barrier`() = runTest {
        val step = testStep("s-cgt", stepTotal = 1, onCompleteHandler = "wc.__step_transition")
        repo.submitStep(step, listOf(testTask("s-cgt", "wc.map", "a")))

        val taskId = getFirstTaskId("s-cgt")
        claimTask(taskId, "gen-1")

        val result = repo.resolveStepTask(taskId, "s-cgt", "gen-1", outputUri = "blob://test")

        assertTrue(result.updated)
        assertTrue(result.barrierMet)

        assertEquals("COMPLETED", getTaskStatus(taskId))
    }

    @Test
    fun `resolveStepTask zombie detection -- zero rows when execution_generation mismatches`() = runTest {
        val step = testStep("s-zombie", stepTotal = 1)
        repo.submitStep(step, listOf(testTask("s-zombie", "wc.map", "a")))

        val taskId = getFirstTaskId("s-zombie")
        claimTask(taskId, "gen-correct")

        val result = repo.resolveStepTask(taskId, "s-zombie", "gen-wrong", outputUri = "blob://zombie")

        assertFalse(result.updated)
        assertFalse(result.barrierMet)

        assertEquals(1, repo.countPendingTasks("s-zombie"))
        assertEquals("CLAIMED", getTaskStatus(taskId))
    }

    @Test
    fun `resolveStepTask stores output_uri on task row`() = runTest {
        val step = testStep("s-out", stepTotal = 1)
        repo.submitStep(step, listOf(testTask("s-out", "wc.map", "a")))

        val taskId = getFirstTaskId("s-out")
        claimTask(taskId, "gen-1")

        repo.resolveStepTask(taskId, "s-out", "gen-1", outputUri = "blob://my-output", outputMetadata = """{"key":"val"}""")

        val outputUri = jdbi.withHandle<String?, Exception> { h ->
            h.createQuery("SELECT output_uri FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java).one()
        }
        assertEquals("blob://my-output", outputUri)
    }

    @Test
    fun `resolveStepTask creates callback task when barrier is met`() = runTest {
        val step = testStep("s-barrier", stepTotal = 1, onCompleteHandler = "wc.__step_transition")
        repo.submitStep(step, listOf(testTask("s-barrier", "wc.map", "a")))

        val taskId = getFirstTaskId("s-barrier")
        claimTask(taskId, "gen-1")

        val result = repo.resolveStepTask(taskId, "s-barrier", "gen-1", outputUri = "blob://x")

        assertTrue(result.barrierMet)

        val callbackCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'wc.__step_transition' AND step_id IS NULL",
            ).mapTo(Int::class.java).one()
        }
        assertEquals(1, callbackCount)
    }

    // ── deadLetterStepTask (failure path) ───────────────────────

    @Test
    fun `deadLetterStepTask increments dead-letter count`() = runTest {
        val step = testStep("s-fail", stepTotal = 2)
        repo.submitStep(step, (0 until 2).map { testTask("s-fail", "wc.map", "i-$it") })
        val taskId = getFirstTaskId("s-fail")
        claimTask(taskId, "gen-1")

        repo.deadLetterStepTask(taskId = taskId, stepId = "s-fail", reason = "test failure", claimToken = "gen-1")

        assertEquals(1, repo.countFailedTasks("s-fail"))
    }

    @Test
    fun `deadLetterStepTask creates callback when barrier met`() = runTest {
        val step = testStep("s-fail-barrier", stepTotal = 1, onCompleteHandler = "wc.__step_transition")
        repo.submitStep(step, listOf(testTask("s-fail-barrier", "wc.map", "a")))
        val taskId = getFirstTaskId("s-fail-barrier")
        claimTask(taskId, "gen-1")

        repo.deadLetterStepTask(taskId = taskId, stepId = "s-fail-barrier", reason = "test", claimToken = "gen-1")

        val callbackCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'wc.__step_transition' AND step_id IS NULL AND payload = 's-fail-barrier'",
            ).mapTo(Int::class.java).one()
        }
        assertEquals(1, callbackCount)
    }

    // ── createNextStep ───────────────────────────────────────────

    @Test
    fun `createNextStep atomically inserts new step and creates tasks`() = runTest {
        val step = testStep("s-tp", stepTotal = 2)
        repo.submitStep(step, (0 until 2).map { testTask("s-tp", "wc.map", "i-$it") })

        val newStep = WorkflowStep(
            stepId = "s-tp-next",
            workflowName = "wc",
            runId = "s-tp",
            status = StepStatus.ACTIVE,
            params = "{}",
            queue = "mr",
            stepLabel = "reduce",
            stepTotal = 2,
            onCompleteHandler = "wc.__step_transition",
        )
        val reduceTasks = (0 until 2).map { testTask("s-tp-next", "wc.reduce", "{}") }
        val result = repo.createNextStep("s-tp", 0, newStep, reduceTasks)
        assertTrue(result)

        // Previous step should be COMPLETED
        val prev = repo.findStep("s-tp")!!
        assertEquals(StepStatus.COMPLETED, prev.status)
        assertEquals(1, prev.version)

        // New step should be ACTIVE
        val next = repo.findStep("s-tp-next")!!
        assertEquals("reduce", next.stepLabel)
        assertEquals(2, next.stepTotal)

        val reduceCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE step_id = 's-tp-next' AND handler = 'wc.reduce'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(2, reduceCount)
    }

    @Test
    fun `createNextStep fails with wrong version and creates no tasks`() = runTest {
        val step = testStep("s-tp-fail", stepTotal = 1)
        repo.submitStep(step, listOf(testTask("s-tp-fail", "wc.map", "a")))

        val newStep = WorkflowStep(
            stepId = "s-tp-fail-next",
            workflowName = "wc",
            runId = "s-tp-fail",
            status = StepStatus.ACTIVE,
            params = "{}",
            queue = "mr",
            stepLabel = "reduce",
            stepTotal = 1,
            onCompleteHandler = "wc.__step_transition",
        )
        val result = repo.createNextStep("s-tp-fail", 999, newStep,
            listOf(testTask("s-tp-fail-next", "wc.reduce", "{}")))
        assertFalse(result)

        val found = repo.findStep("s-tp-fail")!!
        assertEquals(StepStatus.ACTIVE, found.status)
        assertEquals(0, found.version)
    }

    // ── streamTaskOutputs ─────────────────────────────────────────

    @Test
    fun `streamTaskOutputs returns output URIs from completed tasks`() = runTest {
        val step = testStep("s-stream", stepTotal = 2)
        repo.submitStep(step, (0 until 2).map { testTask("s-stream", "wc.map", "i-$it") })

        val taskIds = getAllTaskIds("s-stream")
        taskIds.forEachIndexed { i, taskId ->
            claimTask(taskId, "gen-$i")
            repo.resolveStepTask(taskId, "s-stream", "gen-$i", outputUri = "blob://$i")
        }

        val outputs = repo.streamTaskOutputs("s-stream", "wc.map").toList()
        assertEquals(2, outputs.size)
        assertTrue(outputs.map { it.uri }.containsAll(listOf("blob://0", "blob://1")))
    }

    // ── Helpers ──────────────────────────────────────────────────

    private fun testStep(
        stepId: String,
        stepTotal: Int = 1,
        failurePolicy: String = "FAIL_STEP",
        failureThreshold: Double = 0.0,
        onCompleteHandler: String? = null,
    ) = WorkflowStep(
        stepId = stepId,
        workflowName = "wc",
        runId = stepId,
        status = StepStatus.ACTIVE,
        params = "{}",
        queue = "mr",
        stepLabel = "map",
        stepTotal = stepTotal,
        onCompleteHandler = onCompleteHandler,
        failurePolicy = failurePolicy,
        failureThreshold = failureThreshold,
    )

    private fun testTask(stepId: String, handler: String, payload: String) =
        EnqueueRequest(
            handler = handler,
            payload = payload,
            queue = "mr",
            stepId = stepId,
            maxRetries = 3,
        )

    private fun getFirstTaskId(stepId: String): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT task_id FROM task WHERE step_id = :stepId")
                .bind("stepId", stepId)
                .mapTo(String::class.java).first()
        }

    private fun getAllTaskIds(stepId: String): List<String> =
        jdbi.withHandle<List<String>, Exception> { h ->
            h.createQuery("SELECT task_id FROM task WHERE step_id = :stepId ORDER BY created_at")
                .bind("stepId", stepId)
                .mapTo(String::class.java).list()
        }

    private fun getTaskStatus(taskId: String): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT status FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java).one()
        }

    private fun claimTask(taskId: String, generation: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE task SET status = 'CLAIMED', execution_generation = :gen WHERE task_id = :taskId",
            ).bind("taskId", taskId).bind("gen", generation).execute()
        }
    }
}
