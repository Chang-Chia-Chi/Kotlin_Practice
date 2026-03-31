package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseContext
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.service.phase.ParallelPhaseStrategy
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ParallelPhaseStrategyTest {

    private val strategy = ParallelPhaseStrategy()
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun context(
        joinPolicy: JoinPolicy = JoinPolicy.All,
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
        nextSequence: Int? = 3,
        failedCount: Int = 0,
        totalCount: Int = 3,
    ): PhaseContext {
        val scatterAct = ActivityDefinition(
            name = "scatter", transition = "scatter.handler", fanOut = "parallel",
        )
        val parallelAct = ActivityDefinition(
            name = "parallel", transition = "parallel.handler",
            failurePolicy = failurePolicy, joinPolicy = joinPolicy,
        )
        val nextAct = ActivityDefinition(name = "final", transition = "final.handler")
        val parallelSeq = SequenceInfo(2, 1, parallelAct, PhaseType.PARALLEL, nextSequence)
        val sequenceMap = mutableMapOf(
            1 to SequenceInfo(1, 0, scatterAct, PhaseType.LINEAR, 2),
            2 to parallelSeq,
        )
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 2, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(scatterAct, parallelAct, nextAct))
        val wf = WorkflowRun("wf1", "{}", 2, 1, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, parallelSeq, sequenceMap, failedCount, totalCount)
    }

    @Test
    fun `JoinPolicy All success advances to next sequence`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(3, advance.nextSequence)
    }

    @Test
    fun `JoinPolicy All success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy All with failure returns Abort`() {
        val ctx = context(failedCount = 1, totalCount = 3)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold success when enough tasks succeed`() {
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 1, totalCount = 3)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold failure when not enough succeed`() {
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 2, totalCount = 3)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage success at boundary`() {
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 5, totalCount = 100)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage failure below boundary`() {
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 6, totalCount = 100)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with BEST_EFFORT advances to next sequence`() {
        val ctx = context(failedCount = 1, totalCount = 2, failurePolicy = FailurePolicy.BEST_EFFORT)
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(3, advance.nextSequence)
    }
}
