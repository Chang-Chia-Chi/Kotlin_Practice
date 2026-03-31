package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.model.CandidateIndex
import com.workflow.dispatch.model.SimulationContext
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation
import com.workflow.dispatch.usecase.port.inbound.algorithm.CandidateMatcher

class DefaultCandidateMatcher : CandidateMatcher {
    override fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
        bomTarget: TargetBomAllocation?,
    ): Int? = index.findFirst(sourceBomConstraint)
}
