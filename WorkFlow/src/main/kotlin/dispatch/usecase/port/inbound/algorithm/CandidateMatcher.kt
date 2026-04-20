package com.workflow.dispatch.usecase.port.inbound.algorithm

import com.workflow.dispatch.model.CandidateIndex
import com.workflow.dispatch.model.SimulationContext
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation

interface CandidateMatcher {
    fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
        bomTarget: TargetBomAllocation? = null,
    ): Int?
}
