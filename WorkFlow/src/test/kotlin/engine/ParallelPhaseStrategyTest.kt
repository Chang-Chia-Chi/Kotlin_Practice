package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ParallelPhaseStrategyTest {

    private val strategy = ParallelPhaseStrategy()
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun parallelTask(status: TaskStatus = TaskStatus.COMPLETED, resultJson: String? = null) = Task(
        id = "t-${System.nanoTime()}", workflowId = "wf1", sequenceNumber = 2, status = status,
        handlerKey = "parallel.handler", resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun context(
        joinPolicy: JoinPolicy = JoinPolicy.All,
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
        nextSequence: Int? = 3,
        failedCount: Int = 0,
        tasks: List<Task> = listOf(parallelTask(), parallelTask(), parallelTask()),
    ): PhaseContext {
        val act = ActivityDefinition(
            name = "scatter-activity",
            transition = "scatter.handler",
            failurePolicy = failurePolicy,
            fanOut = FanOutDefinition(transition = "parallel.handler", joinPolicy = joinPolicy),
        )
        val nextAct = ActivityDefinition(name = "final", transition = "final.handler")
        val parallelSeq = SequenceInfo(2, 0, act, PhaseType.PARALLEL, nextSequence)
        val sequenceMap = mutableMapOf(2 to parallelSeq)
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 1, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(act, nextAct))
        val wf = WorkflowRun("wf1", "{}", 2, 1, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, parallelSeq, sequenceMap, failedCount, tasks.size, tasks)
    }

    @Test
    fun `JoinPolicy All success advances to next sequence`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(3, advance.nextSequence)
        assertEquals(1, advance.tasks.size)
        assertEquals("final.handler", advance.tasks[0].handlerKey)
    }

    @Test
    fun `JoinPolicy All success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy All with failure returns Abort`() {
        val tasks = listOf(parallelTask(), parallelTask(status = TaskStatus.FAILED), parallelTask())
        val ctx = context(failedCount = 1, tasks = tasks)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold success when enough tasks succeed`() {
        val tasks = listOf(
            parallelTask(), parallelTask(), parallelTask(status = TaskStatus.FAILED),
        )
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 1, tasks = tasks)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold failure when not enough succeed`() {
        val tasks = listOf(
            parallelTask(), parallelTask(status = TaskStatus.FAILED), parallelTask(status = TaskStatus.FAILED),
        )
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 2, tasks = tasks)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage success at boundary`() {
        // 95 completed, 5 failed out of 100 = 95% >= 95 -> success
        val tasks = (1..95).map { parallelTask() } + (1..5).map { parallelTask(status = TaskStatus.FAILED) }
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 5, tasks = tasks)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage failure below boundary`() {
        // 94 completed, 6 failed out of 100 = 94% < 95 -> failure
        val tasks = (1..94).map { parallelTask() } + (1..6).map { parallelTask(status = TaskStatus.FAILED) }
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 6, tasks = tasks)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with BEST_EFFORT advances to next sequence`() {
        val tasks = listOf(parallelTask(), parallelTask(status = TaskStatus.FAILED))
        val ctx = context(failedCount = 1, tasks = tasks, failurePolicy = FailurePolicy.BEST_EFFORT)
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(3, advance.nextSequence)
    }

}
