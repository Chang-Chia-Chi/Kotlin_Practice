package com.workflow.dispatch.algorithm

import com.workflow.dispatch.simulation.SimulationContext

enum class TerminationDecision { STOP, SKIP_SITE }

interface TerminationStrategy {
    fun onNoCandidate(
        siteId: String,
        targetBomId: String?,
        context: SimulationContext,
    ): TerminationDecision
}

class FailFastTermination : TerminationStrategy {
    override fun onNoCandidate(
        siteId: String,
        targetBomId: String?,
        context: SimulationContext,
    ): TerminationDecision = TerminationDecision.STOP
}
