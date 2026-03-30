package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation
import com.workflow.dispatch.simulation.CandidateIndex
import com.workflow.dispatch.simulation.SimulationContext
import java.math.BigDecimal

interface CandidateMatcher {
    fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
        bomTarget: TargetBomAllocation? = null,
    ): Int?
}

class DefaultCandidateMatcher : CandidateMatcher {
    override fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
        bomTarget: TargetBomAllocation?,
    ): Int? = index.findFirst(sourceBomConstraint)
}

class QtyCandidateMatcher : CandidateMatcher {
    override fun findCandidate(
        index: CandidateIndex,
        sourceBomConstraint: String?,
        context: SimulationContext,
        siteTarget: SiteTarget,
        bomTarget: TargetBomAllocation?,
    ): Int? {
        val currentSiteQty = context.siteCurrents[siteTarget.siteId] ?: BigDecimal.ZERO
        val currentBomQty = if (bomTarget != null) {
            context.bomCurrents[SiteBomKey(siteTarget.siteId, bomTarget.targetBomId)]
                ?: BigDecimal.ZERO
        } else null

        return index.findFirst(sourceBomConstraint) { candidate ->
            val qty = candidate.qty.toBigDecimal()
            val siteFits = currentSiteQty + qty <= siteTarget.target
            val bomFits = currentBomQty == null || currentBomQty + qty <= bomTarget!!.target
            siteFits && bomFits
        }
    }
}
