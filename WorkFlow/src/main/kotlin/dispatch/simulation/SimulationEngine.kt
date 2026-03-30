package com.workflow.dispatch.simulation

import com.workflow.dispatch.algorithm.DispatchAlgorithmFactory
import com.workflow.dispatch.algorithm.TargetSelection
import com.workflow.dispatch.algorithm.TerminationDecision
import com.workflow.dispatch.model.*
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

            val siteTarget = config.siteTargets.first { it.siteId == selection.siteId }
            val bomTarget = if (selection.targetBomId != null) {
                config.bomMappings?.get(selection.siteId)
                    ?.targetAllocations?.firstOrNull { it.targetBomId == selection.targetBomId }
            } else null
            val idx = algorithm.candidateMatcher.findCandidate(
                index, selection.sourceBomConstraint, context, siteTarget, bomTarget,
            )

            if (idx == null) {
                val decision = algorithm.terminationStrategy
                    .onNoCandidate(selection.siteId, selection.targetBomId, context)
                if (decision == TerminationDecision.STOP) break
                continue
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
            context.lastSiteId = selection.siteId
            context.lastBomId = selection.targetBomId

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
