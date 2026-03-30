package com.workflow.dispatch.algorithm

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SelectionKernelTest {

    @Test
    fun `selects entry with lowest gap`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-10"), BigDecimal("50")),
            SelectionEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, null))
    }

    @Test
    fun `breaks tie by highest target`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-10"), BigDecimal("30")),
            SelectionEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, null))
    }

    @Test
    fun `breaks double tie with sticky routing`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-10"), BigDecimal("50")),
            SelectionEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("A", selectByGap(entries, "A"))
        assertEquals("B", selectByGap(entries, "B"))
    }

    @Test
    fun `returns null for empty entries`() {
        assertNull(selectByGap(emptyList(), null))
    }

    @Test
    fun `sticky routing does not override lower gap`() {
        val entries = listOf(
            SelectionEntry("A", BigDecimal("-5"), BigDecimal("50")),
            SelectionEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        // B has lower gap, sticky on A should not override
        assertEquals("B", selectByGap(entries, "A"))
    }
}
