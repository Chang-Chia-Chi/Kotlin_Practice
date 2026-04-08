package com.workflow.dispatch.usecase.service.simulation

import com.workflow.dispatch.model.*
import com.workflow.dispatch.usecase.port.inbound.algorithm.DispatchAlgorithmFactory
import jakarta.enterprise.context.ApplicationScoped
import java.math.BigDecimal

@ApplicationScoped
class SimulationEngine(
    private val algorithmFactory: DispatchAlgorithmFactory,
) {
    fun simulate(
        config: DispatchConfig,
        candidates: List<CandidateProduct>,
        baseline: Baseline,
    ): SimulationResult {
        config.bomMappings?.forEach { (siteId, mapping) ->
            require(mapping.targetAllocations.isNotEmpty()) {
                "Site $siteId has empty targetAllocations"
            }
            require(mapping.sourceBomId.startsWith(config.sourceBomPrefix)) {
                "Site $siteId LV2 sourceBomId '${mapping.sourceBomId}' " +
                    "must start with LV1 prefix '${config.sourceBomPrefix}'"
            }
        }

        val algorithm = algorithmFactory.create(config.mode, config.algorithmId)
        val index = CandidateIndex(candidates)
        val context = SimulationContext(
            siteCurrents = baseline.siteAllocations.toMutableMap(),
            bomCurrents = baseline.bomAllocations.toMutableMap(),
            total = baseline.siteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add),
        )

        val siteTargetMap = config.siteTargets.associateBy { it.siteId }
        val bomTargetMap: Map<SiteBomKey, TargetBomAllocation> = config.bomMappings
            ?.flatMap { (siteId, mapping) ->
                mapping.targetAllocations.map { alloc ->
                    SiteBomKey(siteId, alloc.targetBomId) to alloc
                }
            }?.toMap() ?: emptyMap()

        // Safety cap: accommodates future skip-site strategies that consume iterations without consuming a candidate.
        // With FailFastTermination (no skips), the real max is candidates.size.
        val maxIterations = candidates.size * config.siteTargets.size
        var iterations = 0

        while (index.hasUnconsumed()) {
            if (++iterations > maxIterations) break

            val selection = algorithm.selectTarget(
                config.siteTargets, context.siteCurrents,
                config.bomMappings, context.bomCurrents,
                context.lastSiteId, context.lastBomId, context.total,
            )
            if (selection !is TargetSelection.Selected) break

            val siteTarget = siteTargetMap.getValue(selection.siteId)
            val bomTarget = selection.targetBomId?.let { bomId ->
                bomTargetMap[SiteBomKey(selection.siteId, bomId)]
                    ?: error("BOM mapping inconsistency: siteId=${selection.siteId}, targetBomId=$bomId not found in bomTargetMap")
            }
            val idx = algorithm.candidateMatcher.findCandidate(
                index, selection.sourceBomConstraint, context, siteTarget, bomTarget,
            )

            if (idx == null) {
                val decision = algorithm.terminationStrategy
                    .onNoCandidate(selection.siteId, selection.targetBomId, context)
                when (decision) {
                    TerminationDecision.STOP -> break
                }
            }

            val candidate = index[idx]
            val qty = candidate.qty.toBigDecimal()

            index.consume(idx)
            context.siteCurrents.merge(selection.siteId, qty, BigDecimal::add)
            if (selection.targetBomId != null) {
                context.bomCurrents.merge(
                    SiteBomKey(selection.siteId, selection.targetBomId), qty, BigDecimal::add,
                )
            }
            context.total += qty
            // Reset BOM round-robin when the site changes so each site gets independent cycling.
            context.lastBomId = if (selection.siteId == context.lastSiteId) selection.targetBomId else null
            context.lastSiteId = selection.siteId

            context.decisions += DispatchDecision(
                dispatchOrder = context.decisions.size + 1,
                productId = candidate.productId,
                sourceBomId = candidate.sourceBomId,
                qty = candidate.qty,
                targetSiteId = selection.siteId,
                targetBomId = selection.targetBomId,
                siteGap = selection.siteGap,
                bomGap = selection.bomGap,
            )
        }

        return SimulationResult(
            decisions = context.decisions.toList(),
            finalSiteAllocations = context.siteCurrents.toMap(),
            finalBomAllocations = context.bomCurrents.toMap(),
        )
    }
}
