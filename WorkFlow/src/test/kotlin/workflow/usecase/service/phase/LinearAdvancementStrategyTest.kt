package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.PhaseContext
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.service.phase.LinearAdvancementStrategy
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class LinearAdvancementStrategyTest {

    private val strategy = LinearAdvancementStrategy()
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun activity(name: String = "step1", failurePolicy: FailurePolicy = FailurePolicy.ABORT) =
        ActivityDefinition(name = name, transition = "$name.handler", failurePolicy = failurePolicy)

    private fun context(
        activity: ActivityDefinition = activity(),
        nextSequence: Int? = 2,
        failedCount: Int = 0,
        totalCount: Int = 1,
    ): PhaseContext {
        val seqInfo = SequenceInfo(1, 0, activity, PhaseType.LINEAR, nextSequence)
        val nextAct = ActivityDefinition(name = "step2", transition = "step2.handler")
        val sequenceMap = mutableMapOf(1 to seqInfo)
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 1, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(activity, nextAct))
        val wf = WorkflowRun("wf1", "{}", 1, 0, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, seqInfo, sequenceMap, failedCount, totalCount)
    }

    @Test
    fun `success with next sequence returns Advance`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(2, advance.nextSequence)
    }

    @Test
    fun `success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with ABORT returns Abort`() {
        val ctx = context(failedCount = 1)
        val fail = assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
        assert(fail.reason.contains("1 task(s) failed"))
    }

    @Test
    fun `failure with BEST_EFFORT advances to next sequence`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            failedCount = 1,
        )
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(2, advance.nextSequence)
    }

    @Test
    fun `failure with BEST_EFFORT at last sequence returns Complete`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            nextSequence = null,
            failedCount = 1,
        )
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }
}
