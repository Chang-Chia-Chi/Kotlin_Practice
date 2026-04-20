package com.workflow.dispatch.usecase.port.inbound.algorithm

import com.workflow.dispatch.model.SimulationContext
import com.workflow.dispatch.model.TerminationDecision

interface TerminationStrategy {
    fun onNoCandidate(
        siteId: String,
        targetBomId: String?,
        context: SimulationContext,
    ): TerminationDecision
}
