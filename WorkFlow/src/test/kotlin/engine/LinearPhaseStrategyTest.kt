package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.WorkflowDefinition
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class LinearPhaseStrategyTest {

    private val strategy = LinearPhaseStrategy()
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun activity(name: String = "step1", failurePolicy: FailurePolicy = FailurePolicy.ABORT) =
        ActivityDefinition(name = name, transition = "$name.handler", failurePolicy = failurePolicy)

    private fun task(
        status: TaskStatus = TaskStatus.COMPLETED,
        resultJson: String? = null,
    ) = Task(
        id = "t1", workflowId = "wf1", sequenceNumber = 1, status = status,
        handlerKey = "step1.handler", resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun context(
        activity: ActivityDefinition = activity(),
        nextSequence: Int? = 2,
        failedCount: Int = 0,
        tasks: List<Task> = listOf(task()),
    ): PhaseContext {
        val seqInfo = SequenceInfo(1, 0, activity, PhaseType.LINEAR, nextSequence)
        val nextAct = ActivityDefinition(name = "step2", transition = "step2.handler")
        val sequenceMap = mutableMapOf(1 to seqInfo)
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 1, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(activity, nextAct))
        val wf = WorkflowRun("wf1", "{}", 1, 0, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, seqInfo, sequenceMap, failedCount, tasks.size, tasks)
    }

    @Test
    fun `success with next sequence returns Advance`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(2, advance.nextSequence)
        assertEquals(1, advance.tasks.size)
        assertEquals("step2.handler", advance.tasks[0].handlerKey)
    }

    @Test
    fun `success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with ABORT returns Abort`() {
        val ctx = context(failedCount = 1, tasks = listOf(task(status = TaskStatus.FAILED)))
        val fail = assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
        assert(fail.reason.contains("1 task(s) failed"))
    }

    @Test
    fun `failure with BEST_EFFORT advances to next sequence`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            failedCount = 1,
            tasks = listOf(task(status = TaskStatus.FAILED)),
        )
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(2, advance.nextSequence)
        assertTrue(advance.tasks.isNotEmpty())
    }

    @Test
    fun `failure with BEST_EFFORT at last sequence returns Complete`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            nextSequence = null,
            failedCount = 1,
            tasks = listOf(task(status = TaskStatus.FAILED)),
        )
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }
}
