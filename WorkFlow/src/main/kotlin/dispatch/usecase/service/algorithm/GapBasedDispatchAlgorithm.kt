package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetSelection
import com.workflow.dispatch.usecase.port.inbound.algorithm.CandidateMatcher
import com.workflow.dispatch.usecase.port.inbound.algorithm.DispatchAlgorithm
import com.workflow.dispatch.usecase.port.inbound.algorithm.GapComputer
import com.workflow.dispatch.usecase.port.inbound.algorithm.TerminationStrategy
import java.math.BigDecimal

class GapBasedDispatchAlgorithm(
    private val gapComputer: GapComputer,
    override val candidateMatcher: CandidateMatcher,
    override val terminationStrategy: TerminationStrategy,
) : DispatchAlgorithm {

    override fun selectTarget(
        siteTargets: List<SiteTarget>,
        siteCurrents: Map<String, BigDecimal>,
        bomMappings: Map<String, BomMapping>?,
        bomCurrents: Map<SiteBomKey, BigDecimal>,
        lastSiteId: String?,
        lastBomId: String?,
        total: BigDecimal,
    ): TargetSelection {
        val siteEntries = siteTargets.map { st ->
            val current = siteCurrents[st.siteId] ?: BigDecimal.ZERO
            GapEntry(st.siteId, gapComputer.computeGap(current, st.target, total), st.target)
        }
        val siteEntry = selectByGap(siteEntries, lastSiteId) ?: return TargetSelection.NoTarget

        val bomMapping = bomMappings?.get(siteEntry.id)
            ?: return TargetSelection.Selected(siteEntry.id, null, null, siteEntry.gap, null)

        val bomTotal = siteCurrents[siteEntry.id] ?: BigDecimal.ZERO
        val bomEntries = bomMapping.targetAllocations.map { alloc ->
            val bomCurrent = bomCurrents[SiteBomKey(siteEntry.id, alloc.targetBomId)] ?: BigDecimal.ZERO
            GapEntry(alloc.targetBomId, gapComputer.computeGap(bomCurrent, alloc.target, bomTotal), alloc.target)
        }
        val bomEntry = selectByGap(bomEntries, lastBomId) ?: return TargetSelection.NoTarget

        return TargetSelection.Selected(siteEntry.id, bomEntry.id, bomMapping.sourceBomId, siteEntry.gap, bomEntry.gap)
    }
}
