package com.workflow.dispatch.dsl

import com.workflow.dispatch.dsl.dispatchAlgorithm
import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.model.SimulationContext
import com.workflow.dispatch.model.TerminationDecision
import com.workflow.dispatch.usecase.port.inbound.algorithm.TerminationStrategy
import com.workflow.dispatch.usecase.service.algorithm.DefaultCandidateMatcher
import com.workflow.dispatch.usecase.service.algorithm.DefaultDispatchAlgorithm
import com.workflow.dispatch.usecase.service.algorithm.FailFastTermination
import com.workflow.dispatch.usecase.service.algorithm.QtyCandidateMatcher
import org.junit.jupiter.api.Test
import kotlin.test.assertIs

class DispatchAlgorithmDslTest {

    @Test
    fun `QTY mode creates algorithm with QtyGapComputer and QtyCandidateMatcher`() {
        val algo = dispatchAlgorithm(DispatchMode.QTY) as DefaultDispatchAlgorithm
        assertIs<QtyCandidateMatcher>(algo.candidateMatcher)
        assertIs<FailFastTermination>(algo.terminationStrategy)
    }

    @Test
    fun `RATIO mode creates algorithm with DefaultCandidateMatcher`() {
        val algo = dispatchAlgorithm(DispatchMode.RATIO) as DefaultDispatchAlgorithm
        assertIs<DefaultCandidateMatcher>(algo.candidateMatcher)
    }

    @Test
    fun `DSL allows overriding termination strategy`() {
        val algo = dispatchAlgorithm(DispatchMode.QTY) {
            terminationStrategy = object : TerminationStrategy {
                override fun onNoCandidate(siteId: String, targetBomId: String?,
                    context: SimulationContext,
                ) = TerminationDecision.SKIP_SITE
            }
        } as DefaultDispatchAlgorithm
        assertIs<QtyCandidateMatcher>(algo.candidateMatcher)
        assert(algo.terminationStrategy !is FailFastTermination)
        val decision = algo.terminationStrategy.onNoCandidate(
            "site-1", "bom-1",
            SimulationContext(
                siteCurrents = mutableMapOf(),
                bomCurrents = mutableMapOf(),
                total = java.math.BigDecimal.ZERO,
            ),
        )
        kotlin.test.assertEquals(TerminationDecision.SKIP_SITE, decision)
    }
}
