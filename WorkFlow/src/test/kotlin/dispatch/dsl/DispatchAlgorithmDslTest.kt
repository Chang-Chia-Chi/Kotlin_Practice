package com.workflow.dispatch.dsl

import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.model.SimulationContext
import com.workflow.dispatch.model.TerminationDecision
import com.workflow.dispatch.usecase.port.inbound.algorithm.TerminationStrategy
import com.workflow.dispatch.usecase.service.algorithm.FailFastTermination
import com.workflow.dispatch.usecase.service.algorithm.FirstFitCandidateMatcher
import com.workflow.dispatch.usecase.service.algorithm.GapBasedDispatchAlgorithm
import com.workflow.dispatch.usecase.service.algorithm.QtyCandidateMatcher
import com.workflow.dispatch.usecase.service.algorithm.QtyGapComputer
import com.workflow.dispatch.usecase.service.algorithm.RatioGapComputer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs

class DispatchAlgorithmDslTest {

    @Test
    fun `QTY mode creates algorithm with QtyGapComputer and QtyCandidateMatcher`() {
        val algo = dispatchAlgorithm(DispatchMode.QTY) as GapBasedDispatchAlgorithm
        assertIs<QtyGapComputer>(algo.gapComputer)
        assertIs<QtyCandidateMatcher>(algo.candidateMatcher)
        assertIs<FailFastTermination>(algo.terminationStrategy)
    }

    @Test
    fun `RATIO mode creates algorithm with FirstFitCandidateMatcher`() {
        val algo = dispatchAlgorithm(DispatchMode.RATIO) as GapBasedDispatchAlgorithm
        assertIs<RatioGapComputer>(algo.gapComputer)
        assertIs<FirstFitCandidateMatcher>(algo.candidateMatcher)
    }

    @Test
    fun `DSL allows overriding termination strategy`() {
        val customStrategy = object : TerminationStrategy {
            override fun onNoCandidate(siteId: String, targetBomId: String?,
                context: SimulationContext,
            ) = TerminationDecision.STOP
        }
        val algo = dispatchAlgorithm(DispatchMode.QTY) {
            terminationStrategy = customStrategy
        } as GapBasedDispatchAlgorithm
        assertIs<QtyCandidateMatcher>(algo.candidateMatcher)
        assertFalse(algo.terminationStrategy is FailFastTermination)
        assertEquals(customStrategy, algo.terminationStrategy)
    }
}
