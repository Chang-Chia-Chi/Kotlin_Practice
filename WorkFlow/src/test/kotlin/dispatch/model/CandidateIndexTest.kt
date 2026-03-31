package com.workflow.dispatch.model

import com.workflow.dispatch.model.CandidateIndex
import com.workflow.dispatch.model.CandidateProduct
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CandidateIndexTest {

    private val candidates = listOf(
        CandidateProduct("p1", "bom-A", 5),
        CandidateProduct("p2", "bom-B", 3),
        CandidateProduct("p3", "bom-A", 2),
    )

    @Test
    fun `findFirst returns first unconsumed candidate`() {
        val index = CandidateIndex(candidates)
        val idx = index.findFirst(null)
        assertEquals(0, idx)
        assertEquals("p1", index[idx!!].productId)
    }

    @Test
    fun `findFirst with sourceBom constraint filters correctly`() {
        val index = CandidateIndex(candidates)
        val idx = index.findFirst("bom-B")
        assertEquals(1, idx)
    }

    @Test
    fun `findFirst returns null for unknown sourceBom`() {
        val index = CandidateIndex(candidates)
        assertNull(index.findFirst("bom-Z"))
    }

    @Test
    fun `consume marks candidate as used`() {
        val index = CandidateIndex(candidates)
        index.consume(0)
        // Next findFirst(null) should skip index 0
        assertEquals(1, index.findFirst(null))
    }

    @Test
    fun `consume marks bom-specific candidate as used`() {
        val index = CandidateIndex(candidates)
        index.consume(0) // consume first bom-A
        val idx = index.findFirst("bom-A")
        assertEquals(2, idx) // second bom-A candidate
    }

    @Test
    fun `hasUnconsumed returns false when all consumed`() {
        val index = CandidateIndex(candidates)
        index.consume(0)
        index.consume(1)
        index.consume(2)
        assertFalse(index.hasUnconsumed())
    }

    @Test
    fun `hasUnconsumed returns true when some remain`() {
        val index = CandidateIndex(candidates)
        index.consume(0)
        assertTrue(index.hasUnconsumed())
    }

    @Test
    fun `findFirst with predicate filters candidates`() {
        val index = CandidateIndex(candidates)
        // Only accept candidates with qty >= 4
        val idx = index.findFirst(null) { it.qty >= 4 }
        assertEquals(0, idx) // p1 has qty=5
    }

    @Test
    fun `findFirst with predicate skips non-matching`() {
        val index = CandidateIndex(candidates)
        // Only accept qty >= 4; skip p1 by consuming it
        index.consume(0)
        val idx = index.findFirst(null) { it.qty >= 4 }
        assertNull(idx) // p2=3, p3=2 — none >= 4
    }

    @Test
    fun `empty candidates list`() {
        val index = CandidateIndex(emptyList())
        assertFalse(index.hasUnconsumed())
        assertNull(index.findFirst(null))
    }
}
