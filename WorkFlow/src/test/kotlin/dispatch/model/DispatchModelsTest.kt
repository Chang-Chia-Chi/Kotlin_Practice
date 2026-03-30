package com.workflow.dispatch.model

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import kotlin.test.assertEquals

class DispatchModelsTest {

    @Test
    fun `CandidateProduct rejects qty outside 1-25`() {
        assertThrows<IllegalArgumentException> {
            CandidateProduct("p1", "bom1", 0)
        }
        assertThrows<IllegalArgumentException> {
            CandidateProduct("p1", "bom1", 26)
        }
    }

    @Test
    fun `CandidateProduct accepts qty in valid range`() {
        val p = CandidateProduct("p1", "bom1", 5)
        assertEquals(5, p.qty)
    }

    @Test
    fun `SiteTarget rejects non-positive target`() {
        assertThrows<IllegalArgumentException> {
            SiteTarget("site1", BigDecimal.ZERO)
        }
    }

    @Test
    fun `SiteBomKey equality by siteId and targetBomId`() {
        val k1 = SiteBomKey("s1", "b1")
        val k2 = SiteBomKey("s1", "b1")
        assertEquals(k1, k2)
        assertEquals(k1.hashCode(), k2.hashCode())
    }

    @Test
    fun `DispatchMode has QTY and RATIO`() {
        assertEquals(2, DispatchMode.entries.size)
    }
}
