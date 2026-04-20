package com.workflow.dispatch.model

import java.util.BitSet
import java.util.LinkedList

class CandidateIndex(private val candidates: List<CandidateProduct>) {

    private val bySourceBom: Map<String, LinkedList<Int>> =
        candidates.indices.groupBy { candidates[it].sourceBomId }
            .mapValues { (_, indices) -> LinkedList(indices) }

    private val allIndices: LinkedList<Int> = LinkedList(candidates.indices.toList())

    private val consumed = BitSet(candidates.size)

    fun findFirst(
        sourceBomConstraint: String?,
        predicate: (CandidateProduct) -> Boolean = { true },
    ): Int? {
        val pool = if (sourceBomConstraint != null) {
            bySourceBom[sourceBomConstraint] ?: return null
        } else {
            allIndices
        }
        val iter = pool.iterator()
        while (iter.hasNext()) {
            val idx = iter.next()
            if (consumed[idx]) {
                iter.remove() // prune stale entry so the pool stays compact on future scans
                continue
            }
            if (predicate(candidates[idx])) return idx
        }
        return null
    }

    fun consume(index: Int) {
        consumed.set(index)
    }

    fun hasUnconsumed(): Boolean = consumed.cardinality() < candidates.size

    operator fun get(index: Int): CandidateProduct = candidates[index]
}
