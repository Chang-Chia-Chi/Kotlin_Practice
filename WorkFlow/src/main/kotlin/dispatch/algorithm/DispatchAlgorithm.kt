package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import java.math.BigDecimal

sealed interface TargetSelection {
    data class Selected(
        val siteId: String,
        val targetBomId: String?,
        val sourceBomConstraint: String?,
        val siteGap: BigDecimal,
        val bomGap: BigDecimal?,
    ) : TargetSelection

    data object NoTarget : TargetSelection
}

interface DispatchAlgorithm {
    val candidateMatcher: CandidateMatcher
    val terminationStrategy: TerminationStrategy

    fun selectTarget(
        siteTargets: List<SiteTarget>,
        siteCurrents: Map<String, BigDecimal>,
        bomMappings: Map<String, BomMapping>?,
        bomCurrents: Map<SiteBomKey, BigDecimal>,
        lastSiteId: String?,
        lastBomId: String?,
        total: BigDecimal,
    ): TargetSelection
}

class DefaultDispatchAlgorithm(
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
            SelectionEntry(st.siteId, gapComputer.computeGap(current, st.target, total), st.target)
        }
        val siteId = selectByGap(siteEntries, lastSiteId) ?: return TargetSelection.NoTarget
        val siteGap = siteEntries.first { it.id == siteId }.gap

        val bomMapping = bomMappings?.get(siteId)
            ?: return TargetSelection.Selected(siteId, null, null, siteGap, null)

        val bomTotal = siteCurrents[siteId] ?: BigDecimal.ZERO
        val bomEntries = bomMapping.targetAllocations.map { alloc ->
            val bomCurrent = bomCurrents[SiteBomKey(siteId, alloc.targetBomId)] ?: BigDecimal.ZERO
            SelectionEntry(alloc.targetBomId, gapComputer.computeGap(bomCurrent, alloc.target, bomTotal), alloc.target)
        }
        val targetBomId = selectByGap(bomEntries, lastBomId) ?: return TargetSelection.NoTarget
        val bomGap = bomEntries.first { it.id == targetBomId }.gap

        return TargetSelection.Selected(siteId, targetBomId, bomMapping.sourceBomId, siteGap, bomGap)
    }
}
