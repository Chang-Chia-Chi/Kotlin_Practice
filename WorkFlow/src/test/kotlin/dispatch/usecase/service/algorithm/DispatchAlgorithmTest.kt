package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation
import com.workflow.dispatch.model.TargetSelection
import com.workflow.dispatch.usecase.service.algorithm.GapBasedDispatchAlgorithm
import com.workflow.dispatch.usecase.service.algorithm.FailFastTermination
import com.workflow.dispatch.usecase.service.algorithm.QtyCandidateMatcher
import com.workflow.dispatch.usecase.service.algorithm.QtyGapComputer
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class DispatchAlgorithmTest {

    private fun qtyAlgorithm(): GapBasedDispatchAlgorithm = GapBasedDispatchAlgorithm(
        gapComputer = QtyGapComputer(),
        candidateMatcher = QtyCandidateMatcher(),
        terminationStrategy = FailFastTermination(),
    )

    @Test
    fun `lv1 only selects site with lowest gap`() {
        val algo = qtyAlgorithm()
        val targets = listOf(
            SiteTarget("A", BigDecimal("100")),
            SiteTarget("B", BigDecimal("100")),
        )
        // A is at 80, B is at 60 -> B has lower gap (-40 vs -20)
        val currents = mapOf("A" to BigDecimal("80"), "B" to BigDecimal("60"))

        val result = algo.selectTarget(
            targets, currents, null, emptyMap(), null, null, BigDecimal("140"),
        )

        assertIs<TargetSelection.Selected>(result)
        assertEquals("B", result.siteId)
        assertNull(result.targetBomId)
        assertNull(result.sourceBomConstraint)
    }

    @Test
    fun `lv2 selects site and targetBomId`() {
        val algo = qtyAlgorithm()
        val targets = listOf(SiteTarget("A", BigDecimal("100")))
        val currents = mapOf("A" to BigDecimal("50"))
        val bomMappings = mapOf(
            "A" to BomMapping(
                sourceBomId = "src-bom-1",
                targetAllocations = listOf(
                    TargetBomAllocation("tgt-1", BigDecimal("60")),
                    TargetBomAllocation("tgt-2", BigDecimal("40")),
                ),
            ),
        )
        val bomCurrents = mapOf(
            SiteBomKey("A", "tgt-1") to BigDecimal("50"),
            SiteBomKey("A", "tgt-2") to BigDecimal("0"),
        )

        val result = algo.selectTarget(
            targets, currents, bomMappings, bomCurrents, null, null, BigDecimal("50"),
        )

        assertIs<TargetSelection.Selected>(result)
        assertEquals("A", result.siteId)
        assertEquals("tgt-2", result.targetBomId) // tgt-2 has gap -40, tgt-1 has gap -10
        assertEquals("src-bom-1", result.sourceBomConstraint)
    }

    @Test
    fun `returns NoTarget when no sites`() {
        val algo = qtyAlgorithm()
        val result = algo.selectTarget(
            emptyList(), emptyMap(), null, emptyMap(), null, null, BigDecimal.ZERO,
        )
        assertIs<TargetSelection.NoTarget>(result)
    }
}
