package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.model.SimulationContext
import com.workflow.dispatch.model.TerminationDecision
import com.workflow.dispatch.usecase.port.inbound.algorithm.TerminationStrategy

class FailFastTermination : TerminationStrategy {
    override fun onNoCandidate(
        siteId: String,
        targetBomId: String?,
        context: SimulationContext,
    ): TerminationDecision = TerminationDecision.STOP
}
