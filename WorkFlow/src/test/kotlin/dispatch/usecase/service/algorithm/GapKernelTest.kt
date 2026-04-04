package com.workflow.dispatch.usecase.service.algorithm

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class GapKernelTest {

    @Test
    fun `selects entry with lowest gap`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        val result = selectByGap(entries, null)
        assertEquals("B", result?.id)
        assertEquals(BigDecimal("-20"), result?.gap)
    }

    @Test
    fun `breaks tie by highest target`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("30")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, null)?.id)
    }

    @Test
    fun `breaks double tie with round-robin routing`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-10"), BigDecimal("50")),
            GapEntry("B", BigDecimal("-10"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, "A")?.id)
        assertEquals("A", selectByGap(entries, "B")?.id)
    }

    @Test
    fun `returns null for empty entries`() {
        assertNull(selectByGap(emptyList(), null))
    }

    @Test
    fun `sticky routing does not override lower gap`() {
        val entries = listOf(
            GapEntry("A", BigDecimal("-5"), BigDecimal("50")),
            GapEntry("B", BigDecimal("-20"), BigDecimal("50")),
        )
        assertEquals("B", selectByGap(entries, "A")?.id)
    }
}
