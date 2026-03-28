package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.WorkflowDefinition
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ScatterPhaseStrategyTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    private val strategy = ScatterPhaseStrategy(objectMapper)
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private val fanOutActivity = ActivityDefinition(
        name = "scatter-activity",
        transition = "scatter.handler",
        fanOut = FanOutDefinition(transition = "parallel.handler", retries = 2),
    )

    private fun scatterTask(
        status: TaskStatus = TaskStatus.COMPLETED,
        resultJson: String? = null,
    ) = Task(
        id = "t1", workflowId = "wf1", sequenceNumber = 1, status = status,
        handlerKey = "scatter.handler", resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun context(
        failedCount: Int = 0,
        tasks: List<Task> = listOf(scatterTask(resultJson = """["a","b","c"]""")),
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    ): PhaseContext {
        val act = fanOutActivity.copy(failurePolicy = failurePolicy)
        val scatterSeq = SequenceInfo(1, 0, act, PhaseType.SCATTER, nextSequence = 2)
        val parallelSeq = SequenceInfo(2, 0, act, PhaseType.PARALLEL, nextSequence = null)
        val sequenceMap = mapOf(1 to scatterSeq, 2 to parallelSeq)
        val def = WorkflowDefinition(activities = listOf(act))
        val wf = WorkflowRun("wf1", "{}", 1, 0, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, scatterSeq, sequenceMap, failedCount, tasks.size, tasks)
    }

    @Test
    fun `success creates fan-out tasks from scatter result`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(2, advance.nextSequence)
        assertEquals(3, advance.tasks.size)
        advance.tasks.forEach { task ->
            assertEquals("parallel.handler", task.handlerKey)
            assertEquals(2, task.sequenceNumber)
            assertEquals(TaskStatus.PENDING, task.status)
            assertEquals(2, task.maxRetries)
        }
        assertEquals("a", advance.tasks[0].item)
        assertEquals("b", advance.tasks[1].item)
        assertEquals("c", advance.tasks[2].item)
    }

    @Test
    fun `failure with ABORT returns Abort`() {
        val ctx = context(failedCount = 1, tasks = listOf(scatterTask(status = TaskStatus.FAILED)))
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with BEST_EFFORT returns Advance to parallel with empty task list`() {
        val ctx = context(
            failedCount = 1,
            tasks = listOf(scatterTask(status = TaskStatus.FAILED)),
            failurePolicy = FailurePolicy.BEST_EFFORT,
        )
        // BEST_EFFORT on SCATTER with no result: advance to next sequence
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(1, advance.tasks.size)
    }

    @Test
    fun `scatter result with null resultJson returns Abort`() {
        val ctx = context(tasks = listOf(scatterTask(resultJson = null)))
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }
}
