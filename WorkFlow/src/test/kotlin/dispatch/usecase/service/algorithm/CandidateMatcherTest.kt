package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.model.CandidateIndex
import com.workflow.dispatch.model.CandidateProduct
import com.workflow.dispatch.model.SimulationContext
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation
import com.workflow.dispatch.usecase.service.algorithm.FirstFitCandidateMatcher
import com.workflow.dispatch.usecase.service.algorithm.QtyCandidateMatcher
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CandidateMatcherTest {

    private fun makeContext(
        siteCurrents: Map<String, BigDecimal>,
        bomCurrents: Map<SiteBomKey, BigDecimal> = emptyMap(),
    ): SimulationContext =
        SimulationContext(
            siteCurrents = siteCurrents.toMutableMap(),
            bomCurrents = bomCurrents.toMutableMap(),
            total = BigDecimal.ZERO,
        )

    // ---- FirstFitCandidateMatcher (ratio mode) ----

    @Test
    fun `FirstFitCandidateMatcher returns first matching sourceBom`() {
        val candidates = listOf(
            CandidateProduct("p1", "bom-A", 5),
            CandidateProduct("p2", "bom-B", 3),
        )
        val index = CandidateIndex(candidates)
        val matcher = FirstFitCandidateMatcher()
        val ctx = makeContext(emptyMap())
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(1, matcher.findCandidate(index, "bom-B", ctx, target))
    }

    @Test
    fun `FirstFitCandidateMatcher returns first candidate when no constraint`() {
        val candidates = listOf(CandidateProduct("p1", "bom-A", 5))
        val index = CandidateIndex(candidates)
        val matcher = FirstFitCandidateMatcher()
        val ctx = makeContext(emptyMap())
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(0, matcher.findCandidate(index, null, ctx, target))
    }

    @Test
    fun `FirstFitCandidateMatcher ignores bomTarget - no capacity check in ratio mode`() {
        val candidates = listOf(CandidateProduct("p1", "src-1", 25))
        val index = CandidateIndex(candidates)
        val matcher = FirstFitCandidateMatcher()
        // BOM already at 95/100 but ratio mode doesn't enforce capacity
        val ctx = makeContext(
            siteCurrents = mapOf("s1" to BigDecimal("95")),
            bomCurrents = mapOf(SiteBomKey("s1", "tgt-1") to BigDecimal("95")),
        )
        val target = SiteTarget("s1", BigDecimal("100"))
        val bomTarget = TargetBomAllocation("tgt-1", BigDecimal("100"))

        assertEquals(0, matcher.findCandidate(index, "src-1", ctx, target, bomTarget))
    }

    // ---- QtyCandidateMatcher — site-level capacity ----

    @Test
    fun `QtyCandidateMatcher rejects candidate that exceeds site target`() {
        val candidates = listOf(CandidateProduct("p1", "bom-A", 10))
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        val ctx = makeContext(mapOf("s1" to BigDecimal("95")))
        val target = SiteTarget("s1", BigDecimal("100"))

        assertNull(matcher.findCandidate(index, null, ctx, target))
    }

    @Test
    fun `QtyCandidateMatcher accepts candidate within site capacity`() {
        val candidates = listOf(CandidateProduct("p1", "bom-A", 5))
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        val ctx = makeContext(mapOf("s1" to BigDecimal("90")))
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(0, matcher.findCandidate(index, null, ctx, target))
    }

    @Test
    fun `QtyCandidateMatcher skips large candidate and finds smaller one`() {
        val candidates = listOf(
            CandidateProduct("p1", "bom-A", 10), // too big for site
            CandidateProduct("p2", "bom-A", 3),  // fits
        )
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        val ctx = makeContext(mapOf("s1" to BigDecimal("95")))
        val target = SiteTarget("s1", BigDecimal("100"))

        assertEquals(1, matcher.findCandidate(index, null, ctx, target))
    }

    // ---- QtyCandidateMatcher — BOM-level capacity ----

    @Test
    fun `QtyCandidateMatcher rejects candidate that exceeds BOM target`() {
        val candidates = listOf(CandidateProduct("p1", "src-1", 10))
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        // Site has room (50/200) but BOM is nearly full (55/60)
        val ctx = makeContext(
            siteCurrents = mapOf("s1" to BigDecimal("50")),
            bomCurrents = mapOf(SiteBomKey("s1", "tgt-1") to BigDecimal("55")),
        )
        val target = SiteTarget("s1", BigDecimal("200"))
        val bomTarget = TargetBomAllocation("tgt-1", BigDecimal("60"))

        assertNull(matcher.findCandidate(index, "src-1", ctx, target, bomTarget))
    }

    @Test
    fun `QtyCandidateMatcher accepts candidate within both site and BOM capacity`() {
        val candidates = listOf(CandidateProduct("p1", "src-1", 5))
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        val ctx = makeContext(
            siteCurrents = mapOf("s1" to BigDecimal("50")),
            bomCurrents = mapOf(SiteBomKey("s1", "tgt-1") to BigDecimal("30")),
        )
        val target = SiteTarget("s1", BigDecimal("200"))
        val bomTarget = TargetBomAllocation("tgt-1", BigDecimal("60"))

        assertEquals(0, matcher.findCandidate(index, "src-1", ctx, target, bomTarget))
    }

    @Test
    fun `QtyCandidateMatcher skips BOM-exceeding candidate and finds next`() {
        val candidates = listOf(
            CandidateProduct("p1", "src-1", 20), // fits site but exceeds BOM (55+20=75 > 60)
            CandidateProduct("p2", "src-1", 4),  // fits both (55+4=59 <= 60)
        )
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        val ctx = makeContext(
            siteCurrents = mapOf("s1" to BigDecimal("50")),
            bomCurrents = mapOf(SiteBomKey("s1", "tgt-1") to BigDecimal("55")),
        )
        val target = SiteTarget("s1", BigDecimal("200"))
        val bomTarget = TargetBomAllocation("tgt-1", BigDecimal("60"))

        assertEquals(1, matcher.findCandidate(index, "src-1", ctx, target, bomTarget))
    }

    @Test
    fun `QtyCandidateMatcher no bomTarget - only checks site capacity`() {
        val candidates = listOf(CandidateProduct("p1", "bom-A", 5))
        val index = CandidateIndex(candidates)
        val matcher = QtyCandidateMatcher()
        val ctx = makeContext(mapOf("s1" to BigDecimal("90")))
        val target = SiteTarget("s1", BigDecimal("100"))

        // No bomTarget = LV1-only, site check only
        assertEquals(0, matcher.findCandidate(index, null, ctx, target, null))
    }
}
