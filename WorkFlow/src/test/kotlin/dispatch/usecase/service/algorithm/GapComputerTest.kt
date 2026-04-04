package com.workflow.dispatch.usecase.service.algorithm

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals

class GapComputerTest {

    @Test
    fun `QtyGapComputer returns current minus target`() {
        val gc = QtyGapComputer()
        // current=30, target=50 -> gap=-20
        assertEquals(
            BigDecimal("-20"),
            gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("100")),
        )
    }

    @Test
    fun `QtyGapComputer ignores total`() {
        val gc = QtyGapComputer()
        val gap1 = gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("100"))
        val gap2 = gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("999"))
        assertEquals(gap1, gap2)
    }

    @Test
    fun `RatioGapComputer returns ratio difference`() {
        val gc = RatioGapComputer()
        // current=30, total=100 -> ratio=0.30, target=50% -> gap=0.30-0.50=-0.20
        val gap = gc.computeGap(BigDecimal("30"), BigDecimal("50"), BigDecimal("100"))
        assertEquals(0, gap.compareTo(BigDecimal("-0.20")))
    }

    @Test
    fun `RatioGapComputer returns zero ratio when total is zero`() {
        val gc = RatioGapComputer()
        // total=0 -> currentRatio=0, target=50% -> gap=0-0.50=-0.50
        val gap = gc.computeGap(BigDecimal.ZERO, BigDecimal("50"), BigDecimal.ZERO)
        assertEquals(0, gap.compareTo(BigDecimal("-0.50")))
    }
}
