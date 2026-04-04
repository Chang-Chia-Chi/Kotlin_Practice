package com.workflow.dispatch.model

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
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

    @Test
    fun `BatchStatus has NORMAL and DRYRUN values`() {
        assertEquals(BatchStatus.NORMAL, BatchStatus.valueOf("NORMAL"))
        assertEquals(BatchStatus.DRYRUN, BatchStatus.valueOf("DRYRUN"))
        assertEquals(2, BatchStatus.entries.size)
    }

    @Test
    fun `DispatchBatch holds batch metadata`() {
        val now = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)
        val batch = DispatchBatch(
            batchToken = "20260403060000",
            status = BatchStatus.NORMAL,
            createdAt = now,
            configCount = 3,
        )
        assertEquals("20260403060000", batch.batchToken)
        assertEquals(BatchStatus.NORMAL, batch.status)
        assertEquals(now, batch.createdAt)
        assertEquals(3, batch.configCount)
    }
}
