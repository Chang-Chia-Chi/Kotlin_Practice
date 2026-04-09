package com.workflow.dispatch.usecase.service.simulation

import com.workflow.dispatch.model.*
import com.workflow.dispatch.usecase.service.algorithm.DefaultDispatchAlgorithmFactory
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SimulationEngineTest {

    private val factory = DefaultDispatchAlgorithmFactory()
    private val engine = SimulationEngine(factory)

    // =========================================================================
    // LV1-only, QTY mode
    // =========================================================================

    @Test
    fun `lv1 only QTY mode distributes to site with lowest gap`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("100")),
                SiteTarget("B", BigDecimal("100")),
            ),
            bomMappings = null,
        )
        val candidates = listOf(
            CandidateProduct("p1", "bom1", 10),
            CandidateProduct("p2", "bom1", 10),
        )
        val baseline = Baseline(
            siteAllocations = mapOf("A" to BigDecimal("80"), "B" to BigDecimal("60")),
            bomAllocations = emptyMap(),
        )

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(2, result.decisions.size)
        assertEquals("B", result.decisions[0].targetSiteId)
        assertEquals("B", result.decisions[1].targetSiteId)
    }

    @Test
    fun `empty candidates produces empty result`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = null,
        )

        val result = engine.simulate(config, emptyList(), Baseline(emptyMap(), emptyMap()))

        assertTrue(result.decisions.isEmpty())
    }

    @Test
    fun `QTY mode stops when candidate exceeds site capacity`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(SiteTarget("A", BigDecimal("10"))),
            bomMappings = null,
        )
        val candidates = listOf(
            CandidateProduct("p1", "bom1", 5),
            CandidateProduct("p2", "bom1", 5),
            CandidateProduct("p3", "bom1", 5),
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(2, result.decisions.size)
        assertEquals(BigDecimal("10"), result.finalSiteAllocations["A"])
    }

    @Test
    fun `dispatch order is 1-based sequential`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = null,
        )
        val candidates = listOf(
            CandidateProduct("p1", "bom1", 1),
            CandidateProduct("p2", "bom1", 1),
            CandidateProduct("p3", "bom1", 1),
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(listOf(1, 2, 3), result.decisions.map { it.dispatchOrder })
    }

    // =========================================================================
    // LV1-only, QTY mode — multi-qty, multi-site
    // =========================================================================

    @Test
    fun `QTY multi-qty products across 3 sites fill proportionally to gap`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("200")),
                SiteTarget("B", BigDecimal("150")),
                SiteTarget("C", BigDecimal("100")),
            ),
            bomMappings = null,
        )
        // 30 products with varying qty (2-25), total qty = sum of all
        val candidates = (1..30).map {
            CandidateProduct("p$it", "bom1", qty = ((it % 5) + 1) * 3) // 3,6,9,12,15 repeating
        }
        val totalQty = candidates.sumOf { it.qty }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // All products dispatched — total capacity (200+150+100=450) > totalQty
        assertEquals(30, result.decisions.size)
        // No product dispatched twice
        assertEquals(30, result.decisions.map { it.productId }.toSet().size)
        // Site allocations respect capacity
        assertTrue(result.finalSiteAllocations["A"]!! <= BigDecimal("200"))
        assertTrue(result.finalSiteAllocations["B"]!! <= BigDecimal("150"))
        assertTrue(result.finalSiteAllocations["C"]!! <= BigDecimal("100"))
        // Total dispatched matches candidate total
        val dispatched = result.finalSiteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add)
        assertEquals(BigDecimal(totalQty), dispatched)
    }

    @Test
    fun `QTY mode skips large candidates that exceed remaining capacity`() {
        // Site A target=20: can take qty=5 four times, but a qty=25 product won't fit
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(SiteTarget("A", BigDecimal("20"))),
            bomMappings = null,
        )
        val candidates = listOf(
            CandidateProduct("big", "bom1", 25),   // too big
            CandidateProduct("sm1", "bom1", 5),
            CandidateProduct("sm2", "bom1", 5),
            CandidateProduct("sm3", "bom1", 5),
            CandidateProduct("sm4", "bom1", 5),
            CandidateProduct("sm5", "bom1", 5),    // would exceed 20
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // "big" is skipped, sm1-sm4 dispatched (total=20), sm5 exceeds capacity
        assertEquals(4, result.decisions.size)
        assertEquals(BigDecimal("20"), result.finalSiteAllocations["A"])
        assertTrue(result.decisions.none { it.productId == "big" })
    }

    // =========================================================================
    // LV1-only, RATIO mode
    // =========================================================================

    @Test
    fun `RATIO mode distributes proportionally`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("60")),
                SiteTarget("B", BigDecimal("40")),
            ),
            bomMappings = null,
        )
        val candidates = (1..10).map { CandidateProduct("p$it", "bom1", 1) }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(10, result.decisions.size)
        assertEquals(6, result.decisions.count { it.targetSiteId == "A" })
        assertEquals(4, result.decisions.count { it.targetSiteId == "B" })
    }

    @Test
    fun `RATIO mode with multi-qty products distributes by quantity weight`() {
        // 3 sites: 50%, 30%, 20%. Products with qty 1-5.
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("X", BigDecimal("50")),
                SiteTarget("Y", BigDecimal("30")),
                SiteTarget("Z", BigDecimal("20")),
            ),
            bomMappings = null,
        )
        // 40 products with varying qty
        val candidates = (1..40).map {
            CandidateProduct("p$it", "bom1", qty = (it % 5) + 1) // 2,3,4,5,1 repeating
        }
        val totalQty = candidates.sumOf { it.qty }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // All candidates dispatched (ratio mode has no capacity cap)
        assertEquals(40, result.decisions.size)
        val dispatched = result.finalSiteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add)
        assertEquals(BigDecimal(totalQty), dispatched)

        // Verify ratio distribution is reasonable (within 10% tolerance)
        val xQty = result.finalSiteAllocations["X"]!!.toDouble()
        val yQty = result.finalSiteAllocations["Y"]!!.toDouble()
        val zQty = result.finalSiteAllocations["Z"]!!.toDouble()
        val total = totalQty.toDouble()
        assertTrue(xQty / total in 0.35..0.65, "X ratio ${xQty / total} not near 50%")
        assertTrue(yQty / total in 0.15..0.45, "Y ratio ${yQty / total} not near 30%")
        assertTrue(zQty / total in 0.05..0.35, "Z ratio ${zQty / total} not near 20%")
    }

    @Test
    fun `RATIO 80-20 with 100 products of varying qty`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("80")),
                SiteTarget("B", BigDecimal("20")),
            ),
            bomMappings = null,
        )
        val candidates = (1..100).map {
            CandidateProduct("p$it", "bom1", qty = (it % 10) + 1) // 2..10,1 repeating
        }
        val totalQty = candidates.sumOf { it.qty }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(100, result.decisions.size)
        val aQty = result.finalSiteAllocations["A"]!!.toDouble()
        val total = totalQty.toDouble()
        // 80/20 split — A should get roughly 80% of total qty
        assertTrue(aQty / total in 0.70..0.90, "A ratio ${aQty / total} not near 80%")
    }

    @Test
    fun `RATIO mode with existing baseline continues from prior state`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("50")),
                SiteTarget("B", BigDecimal("50")),
            ),
            bomMappings = null,
        )
        // Baseline: A has 100 already, B has 0 — B is massively behind
        val baseline = Baseline(
            siteAllocations = mapOf("A" to BigDecimal("100"), "B" to BigDecimal.ZERO),
            bomAllocations = emptyMap(),
        )
        // 20 new products with qty=3 each
        val candidates = (1..20).map { CandidateProduct("p$it", "bom1", 3) }

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(20, result.decisions.size)
        // B should get most/all products to close the gap
        val bCount = result.decisions.count { it.targetSiteId == "B" }
        assertTrue(bCount >= 15, "B should get most products to close gap, got $bCount")
    }

    // =========================================================================
    // LV1 + LV2, QTY mode
    // =========================================================================

    @Test
    fun `lv2 BOM mapping constrains sourceBomId`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "src",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "src-1",
                    targetAllocations = listOf(TargetBomAllocation("tgt-1", BigDecimal("100"))),
                ),
            ),
        )
        val candidates = listOf(
            CandidateProduct("p1", "src-1", 5),
            CandidateProduct("p2", "other-bom", 5),
            CandidateProduct("p3", "src-1", 5),
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(2, result.decisions.size)
        assertEquals("p1", result.decisions[0].productId)
        assertEquals("p3", result.decisions[1].productId)
    }

    @Test
    fun `QTY lv2 with multiple target BOMs distributes by BOM gap`() {
        // 1 site, 2 target BOMs with QTY targets: tgt-A=60, tgt-B=40
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "src",
            siteTargets = listOf(SiteTarget("factory1", BigDecimal("100"))),
            bomMappings = mapOf(
                "factory1" to BomMapping(
                    sourceBomId = "src-bom",
                    targetAllocations = listOf(
                        TargetBomAllocation("tgt-A", BigDecimal("60")),
                        TargetBomAllocation("tgt-B", BigDecimal("40")),
                    ),
                ),
            ),
        )
        // 20 candidates, all matching sourceBomId, mixed qty
        val candidates = (1..20).map {
            CandidateProduct("p$it", "src-bom", qty = (it % 3) + 1) // 2,3,1 repeating
        }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // All decisions should have targetBomId set
        assertTrue(result.decisions.all { it.targetBomId != null })
        // BOM allocations should exist for both targets
        val tgtA = result.finalBomAllocations[SiteBomKey("factory1", "tgt-A")] ?: BigDecimal.ZERO
        val tgtB = result.finalBomAllocations[SiteBomKey("factory1", "tgt-B")] ?: BigDecimal.ZERO
        // tgt-A should get more (target=60 vs 40)
        assertTrue(tgtA > tgtB, "tgt-A ($tgtA) should exceed tgt-B ($tgtB)")
        // Total should match site allocation
        assertEquals(result.finalSiteAllocations["factory1"], tgtA + tgtB)
    }

    @Test
    fun `QTY lv2 multi-site with different BOM mappings per site`() {
        // LV1 prefix "PFX", LV2 full sourceBomIds "PFX-A-001" and "PFX-B-001" contain the prefix
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "PFX",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("50")),
                SiteTarget("B", BigDecimal("50")),
            ),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "PFX-A-001",
                    targetAllocations = listOf(TargetBomAllocation("tgt-A1", BigDecimal("50"))),
                ),
                "B" to BomMapping(
                    sourceBomId = "PFX-B-001",
                    targetAllocations = listOf(TargetBomAllocation("tgt-B1", BigDecimal("50"))),
                ),
            ),
        )
        // Candidates: half match site A's BOM, half match site B's
        val candidatesA = (1..10).map { CandidateProduct("pA$it", "PFX-A-001", 3) }
        val candidatesB = (1..10).map { CandidateProduct("pB$it", "PFX-B-001", 3) }
        val candidates = (candidatesA + candidatesB).shuffled(java.util.Random(42))
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // Products with PFX-A-001 should go to site A, PFX-B-001 to site B
        for (d in result.decisions) {
            if (d.sourceBomId == "PFX-A-001") {
                assertEquals("A", d.targetSiteId, "PFX-A products should go to site A")
            } else {
                assertEquals("B", d.targetSiteId, "PFX-B products should go to site B")
            }
        }
    }

    // =========================================================================
    // LV1 + LV2, RATIO mode
    // =========================================================================

    @Test
    fun `RATIO lv2 distributes BOM targets by percentage within site`() {
        // 1 site, ratio mode, 2 target BOMs at 70%/30%
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "src",
            siteTargets = listOf(SiteTarget("factory1", BigDecimal("100"))),
            bomMappings = mapOf(
                "factory1" to BomMapping(
                    sourceBomId = "src-bom",
                    targetAllocations = listOf(
                        TargetBomAllocation("tgt-X", BigDecimal("70")),
                        TargetBomAllocation("tgt-Y", BigDecimal("30")),
                    ),
                ),
            ),
        )
        val candidates = (1..30).map {
            CandidateProduct("p$it", "src-bom", qty = (it % 4) + 1) // 2,3,4,1 repeating
        }
        val totalQty = candidates.sumOf { it.qty }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(30, result.decisions.size)
        val xQty = (result.finalBomAllocations[SiteBomKey("factory1", "tgt-X")]
            ?: BigDecimal.ZERO).toDouble()
        val yQty = (result.finalBomAllocations[SiteBomKey("factory1", "tgt-Y")]
            ?: BigDecimal.ZERO).toDouble()
        // tgt-X should be ~70%, tgt-Y ~30%
        val xRatio = xQty / (xQty + yQty)
        assertTrue(xRatio in 0.55..0.85, "tgt-X ratio $xRatio not near 70%")
    }

    @Test
    fun `RATIO lv1 and lv2 combined - same sourceBomId - all dispatched`() {
        // Site A=60%, Site B=40%. Both sites share the SAME sourceBomId
        // so all candidates are eligible for either site.
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "shared",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("60")),
                SiteTarget("B", BigDecimal("40")),
            ),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "shared-src",
                    targetAllocations = listOf(
                        TargetBomAllocation("tgt-A1", BigDecimal("60")),
                        TargetBomAllocation("tgt-A2", BigDecimal("40")),
                    ),
                ),
                "B" to BomMapping(
                    sourceBomId = "shared-src",
                    targetAllocations = listOf(
                        TargetBomAllocation("tgt-B1", BigDecimal("50")),
                        TargetBomAllocation("tgt-B2", BigDecimal("50")),
                    ),
                ),
            ),
        )
        val candidates = (1..50).map {
            CandidateProduct("p$it", "shared-src", (it % 5) + 1)
        }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(50, result.decisions.size)
        val aQty = result.finalSiteAllocations["A"]!!.toDouble()
        val bQty = result.finalSiteAllocations["B"]!!.toDouble()
        val total = aQty + bQty
        assertTrue(aQty / total in 0.45..0.75, "Site A ratio ${aQty / total} not near 60%")
        assertTrue(result.finalBomAllocations.isNotEmpty())
        assertTrue(result.decisions.all { it.targetBomId != null })
        assertTrue(result.decisions.all { it.bomGap != null })
    }

    @Test
    fun `RATIO lv1 and lv2 combined - different sourceBomId per site - fail-fast stops early`() {
        // Different sourceBomId per site: once the gap-selected site's candidates
        // are exhausted, FailFastTermination halts the entire simulation.
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "src",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("60")),
                SiteTarget("B", BigDecimal("40")),
            ),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "src-A",
                    targetAllocations = listOf(TargetBomAllocation("tgt-A1", BigDecimal("100"))),
                ),
                "B" to BomMapping(
                    sourceBomId = "src-B",
                    targetAllocations = listOf(TargetBomAllocation("tgt-B1", BigDecimal("100"))),
                ),
            ),
        )
        val candidatesA = (1..15).map { CandidateProduct("pA$it", "src-A", 2) }
        val candidatesB = (1..25).map { CandidateProduct("pB$it", "src-B", 2) }
        val candidates = (candidatesA + candidatesB).shuffled(java.util.Random(99))
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // Not all 40 dispatched — fail-fast terminates when one site's pool is empty
        assertTrue(result.decisions.size < 40, "Fail-fast should stop before all dispatched")
        assertTrue(result.decisions.size > 10, "Should dispatch a reasonable amount before stopping")
        // Constraint: right products → right sites
        for (d in result.decisions) {
            if (d.sourceBomId == "src-A") assertEquals("A", d.targetSiteId)
            else assertEquals("B", d.targetSiteId)
        }
        assertEquals(result.decisions.map { it.productId }.toSet().size, result.decisions.size)
    }

    // =========================================================================
    // Scale: hundreds of products
    // =========================================================================

    @Test
    fun `QTY mode 200 products with mixed qty across 4 sites`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("S1", BigDecimal("500")),
                SiteTarget("S2", BigDecimal("400")),
                SiteTarget("S3", BigDecimal("300")),
                SiteTarget("S4", BigDecimal("200")),
            ),
            bomMappings = null,
        )
        val candidates = (1..200).map {
            CandidateProduct("p$it", "bom1", qty = (it % 7) + 1) // 2-8 repeating
        }
        val totalQty = candidates.sumOf { it.qty }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // Total capacity = 1400, enough for all
        assertEquals(200, result.decisions.size)
        // No duplicate dispatches
        assertEquals(200, result.decisions.map { it.productId }.toSet().size)
        // Total dispatched
        val dispatched = result.finalSiteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add)
        assertEquals(BigDecimal(totalQty), dispatched)
        // All sites got something
        assertTrue(result.finalSiteAllocations.size == 4)
        result.finalSiteAllocations.forEach { (site, qty) ->
            assertTrue(qty > BigDecimal.ZERO, "Site $site should have dispatches")
        }
    }

    @Test
    fun `RATIO mode 300 products with qty 1-25 across 3 sites`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("F1", BigDecimal("50")),
                SiteTarget("F2", BigDecimal("30")),
                SiteTarget("F3", BigDecimal("20")),
            ),
            bomMappings = null,
        )
        val candidates = (1..300).map {
            CandidateProduct("p$it", "bom1", qty = (it % 25) + 1) // 2-25,1 cycle
        }
        val totalQty = candidates.sumOf { it.qty }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(300, result.decisions.size)
        assertEquals(300, result.decisions.map { it.productId }.toSet().size)
        val f1 = result.finalSiteAllocations["F1"]!!.toDouble()
        val f2 = result.finalSiteAllocations["F2"]!!.toDouble()
        val f3 = result.finalSiteAllocations["F3"]!!.toDouble()
        val total = totalQty.toDouble()
        // Verify ratios within reasonable tolerance
        assertTrue(f1 / total in 0.38..0.62, "F1 ratio ${f1 / total} not near 50%")
        assertTrue(f2 / total in 0.18..0.42, "F2 ratio ${f2 / total} not near 30%")
        assertTrue(f3 / total in 0.08..0.32, "F3 ratio ${f3 / total} not near 20%")
    }

    @Test
    fun `QTY lv2 200 products shared sourceBomId with BOM sub-allocation`() {
        // Both sites share the SAME sourceBomId, so all candidates are eligible for any site.
        // This tests LV2 BOM target sub-allocation at scale.
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "shared",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("500")),
                SiteTarget("B", BigDecimal("400")),
            ),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "shared-pfx",
                    targetAllocations = listOf(
                        TargetBomAllocation("tgt-A1", BigDecimal("300")),
                        TargetBomAllocation("tgt-A2", BigDecimal("200")),
                    ),
                ),
                "B" to BomMapping(
                    sourceBomId = "shared-pfx",
                    targetAllocations = listOf(
                        TargetBomAllocation("tgt-B1", BigDecimal("400")),
                    ),
                ),
            ),
        )
        val candidates = (1..200).map {
            CandidateProduct("p$it", "shared-pfx", qty = (it % 5) + 1) // 2-6
        }
        val totalQty = candidates.sumOf { it.qty }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // All products dispatched — shared sourceBomId means no fail-fast mismatch
        assertEquals(200, result.decisions.size)
        assertEquals(200, result.decisions.map { it.productId }.toSet().size)
        // All decisions have targetBomId
        assertTrue(result.decisions.all { it.targetBomId != null })
        for (d in result.decisions) {
            if (d.targetSiteId == "A") assertTrue(d.targetBomId in listOf("tgt-A1", "tgt-A2"))
            else assertEquals("tgt-B1", d.targetBomId)
        }
        // BOM sub-allocation: tgt-A1 should get more than tgt-A2 (300 vs 200 target)
        val a1 = result.finalBomAllocations[SiteBomKey("A", "tgt-A1")] ?: BigDecimal.ZERO
        val a2 = result.finalBomAllocations[SiteBomKey("A", "tgt-A2")] ?: BigDecimal.ZERO
        assertTrue(a1 > a2, "tgt-A1 ($a1) should exceed tgt-A2 ($a2)")
        // Total dispatched matches
        val dispatched = result.finalSiteAllocations.values.fold(BigDecimal.ZERO, BigDecimal::add)
        assertEquals(BigDecimal(totalQty), dispatched)
    }

    @Test
    fun `QTY lv2 different sourceBomId per site - fail-fast terminates early`() {
        // Different sourceBomId per site: fail-fast terminates when gap-selected
        // site has no matching candidates remaining.
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "PFX",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("400")),
                SiteTarget("B", BigDecimal("300")),
            ),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "PFX-A",
                    targetAllocations = listOf(TargetBomAllocation("tgt-A1", BigDecimal("400"))),
                ),
                "B" to BomMapping(
                    sourceBomId = "PFX-B",
                    targetAllocations = listOf(TargetBomAllocation("tgt-B1", BigDecimal("300"))),
                ),
            ),
        )
        val candidatesA = (1..60).map { CandidateProduct("pA$it", "PFX-A", qty = 3) }
        val candidatesB = (1..100).map { CandidateProduct("pB$it", "PFX-B", qty = 3) }
        val candidates = (candidatesA + candidatesB).shuffled(java.util.Random(123))
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // Fail-fast stops when one pool is exhausted — not all dispatched
        assertTrue(result.decisions.size < 160, "Should stop before all dispatched")
        assertTrue(result.decisions.size > 50, "Should dispatch most before stopping")
        for (d in result.decisions) {
            if (d.sourceBomId == "PFX-A") assertEquals("A", d.targetSiteId)
            else assertEquals("B", d.targetSiteId)
        }
        assertEquals(result.decisions.map { it.productId }.toSet().size, result.decisions.size)
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    @Test
    fun `no targetBomId when bomMappings is null`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = null,
        )
        val candidates = listOf(CandidateProduct("p1", "bom1", 5))
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(1, result.decisions.size)
        assertNull(result.decisions[0].targetBomId)
        assertNull(result.decisions[0].bomGap)
    }

    @Test
    fun `each candidate dispatched exactly once - uniqueness invariant`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.RATIO, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("50")),
                SiteTarget("B", BigDecimal("50")),
            ),
            bomMappings = null,
        )
        val candidates = (1..150).map {
            CandidateProduct("p$it", "bom1", qty = (it % 10) + 1)
        }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(150, result.decisions.size)
        val productIds = result.decisions.map { it.productId }
        assertEquals(productIds.toSet().size, productIds.size, "Duplicate dispatch detected")
    }

    @Test
    fun `QTY mode partial dispatch when capacity is insufficient for all`() {
        // 2 sites with limited capacity, more products than can fit
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("30")),
                SiteTarget("B", BigDecimal("20")),
            ),
            bomMappings = null,
        )
        // 50 products of qty=5 each = 250 total, but capacity only 50
        val candidates = (1..50).map { CandidateProduct("p$it", "bom1", 5) }
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        // Should dispatch exactly 10 products (30/5=6 for A, 20/5=4 for B)
        assertEquals(10, result.decisions.size)
        assertEquals(BigDecimal("30"), result.finalSiteAllocations["A"])
        assertEquals(BigDecimal("20"), result.finalSiteAllocations["B"])
    }

    // =========================================================================
    // LV1 prefix / LV2 full BOM ID validation
    // =========================================================================

    @Test
    fun `rejects LV2 sourceBomId that does not start with LV1 prefix`() {
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "PFX-A",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "OTHER-001", // does NOT start with "PFX-A"
                    targetAllocations = listOf(TargetBomAllocation("tgt-1", BigDecimal("100"))),
                ),
            ),
        )
        val candidates = listOf(CandidateProduct("p1", "OTHER-001", 5))
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
            engine.simulate(config, candidates, baseline)
        }
    }

    @Test
    fun `accepts LV2 sourceBomId that starts with LV1 prefix`() {
        // LV1 prefix "PFX", LV2 sourceBomId "PFX-A-001" starts with "PFX"
        val config = DispatchConfig(
            id = "cfg1", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "PFX",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "PFX-A-001",
                    targetAllocations = listOf(TargetBomAllocation("tgt-1", BigDecimal("100"))),
                ),
            ),
        )
        val candidates = listOf(
            CandidateProduct("p1", "PFX-A-001", 5),
            CandidateProduct("p2", "PFX-A-001", 5),
        )
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(2, result.decisions.size)
        assertTrue(result.decisions.all { it.targetBomId == "tgt-1" })
    }

    @Test
    fun `simulate throws when a site has empty targetAllocations`() {
        val config = DispatchConfig(
            id = "cfg", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "src",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = mapOf(
                "A" to BomMapping(sourceBomId = "src", targetAllocations = emptyList()),
            ),
        )
        val candidates = listOf(CandidateProduct("p1", "src", 1))
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val ex = assertFailsWith<IllegalArgumentException> {
            engine.simulate(config, candidates, baseline)
        }
        assertEquals("Site A has empty targetAllocations", ex.message)
    }

    @Test
    fun `simulate runs normally when all selected bom ids are in bomTargetMap`() {
        // This is a consistency smoke test: the algorithm must only select bomIds
        // that exist in the config's targetAllocations (and therefore in bomTargetMap).
        val config = DispatchConfig(
            id = "cfg", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "src",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = mapOf(
                "A" to BomMapping(
                    sourceBomId = "src-bom",
                    targetAllocations = listOf(TargetBomAllocation("tgt-1", BigDecimal("100"))),
                ),
            ),
        )
        val candidates = listOf(CandidateProduct("p1", "src-bom", 5))
        val baseline = Baseline(siteAllocations = emptyMap(), bomAllocations = emptyMap())

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(1, result.decisions.size)
        assertEquals("tgt-1", result.decisions[0].targetBomId)
    }

    @Test
    fun `each site starts at first bom when no prior dispatch recorded for that site`() {
        // Both sites share the same 3 BOM IDs: [bom1(50), bom2(50), bom3(50)].
        // After dispatching to site A (→ bom1, stored in lastBomIds["A"]), the next dispatch goes to site B.
        // Expected: B picks bom1 (no entry in lastBomIds for B → list order), not bom2.
        val sharedAllocations = listOf(
            TargetBomAllocation("bom1", BigDecimal("50")),
            TargetBomAllocation("bom2", BigDecimal("50")),
            TargetBomAllocation("bom3", BigDecimal("50")),
        )
        val config = DispatchConfig(
            id = "cfg", mode = DispatchMode.QTY, algorithmId = "default",
            sourceBomPrefix = "src",
            siteTargets = listOf(
                SiteTarget("A", BigDecimal("200")),
                SiteTarget("B", BigDecimal("200")),
            ),
            bomMappings = mapOf(
                "A" to BomMapping("src", sharedAllocations),
                "B" to BomMapping("src", sharedAllocations),
            ),
        )
        // Equal currents (100 each) → tied gap → A wins on first dispatch (round-robin with null lastSite).
        // After A gets +1, B has a lower gap → B is selected second.
        val baseline = Baseline(
            siteAllocations = mapOf("A" to BigDecimal("100"), "B" to BigDecimal("100")),
            bomAllocations = emptyMap(),
        )
        val candidates = listOf(
            CandidateProduct("p1", "src", 1),
            CandidateProduct("p2", "src", 1),
        )

        val result = engine.simulate(config, candidates, baseline)

        assertEquals(2, result.decisions.size)
        assertEquals("A", result.decisions[0].targetSiteId)
        assertEquals("bom1", result.decisions[0].targetBomId)   // first BOM for A (no prior state)
        assertEquals("B", result.decisions[1].targetSiteId)
        assertEquals("bom1", result.decisions[1].targetBomId)   // B's round-robin must reset: bom1, not bom2
    }
}
