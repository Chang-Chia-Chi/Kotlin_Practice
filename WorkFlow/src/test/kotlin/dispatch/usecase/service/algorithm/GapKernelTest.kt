package com.workflow.dispatch.usecase.service.algorithm

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class GapKernelTest {

    @Test
    fun `selects entry with lowest gap`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("B", selectByGap(entries, null, false)?.id)
    }

    @Test
    fun `breaks tie by highest target`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("30"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("B", selectByGap(entries, null, false)?.id)
    }

    @Test
    fun `breaks remaining tie by last dispatched — no prior defaults to list order`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("A", selectByGap(entries, null, false)?.id)   // no prior → list order → A
        assertEquals("A", selectByGap(entries, "A", false)?.id)    // last was A → sticky → A
        assertEquals("B", selectByGap(entries, "B", false)?.id)    // last was B → sticky → B
    }

    @Test
    fun `returns null for empty entries`() {
        assertNull(selectByGap(emptyList(), null, false))
    }

    @Test
    fun `breaks tie by lowest current when useCumulative is true`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("60")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        // B has lower current → wins cumulative tiebreaker even though A was last
        assertEquals("B", selectByGap(entries, "A", true)?.id)
    }

    @Test
    fun `sticky applies after cumulative when current is also tied`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50"), BigDecimal("40")),
        )
        // cumulative tied (same current) → sticky → B
        assertEquals("B", selectByGap(entries, "B", true)?.id)
    }

    @Test
    fun `sticky does not override lower gap`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-5"), BigDecimal("50"), BigDecimal("40")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50"), BigDecimal("40")),
        )
        assertEquals("B", selectByGap(entries, "A", false)?.id)
    }
}
