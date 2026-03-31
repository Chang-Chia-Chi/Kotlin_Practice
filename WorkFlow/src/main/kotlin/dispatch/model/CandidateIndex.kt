package com.workflow.dispatch.model

import java.util.BitSet

class CandidateIndex(private val candidates: List<CandidateProduct>) {

    private val bySourceBom: Map<String, List<Int>> =
        candidates.indices.groupBy { candidates[it].sourceBomId }

    private val allIndices: List<Int> = candidates.indices.toList()

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
        return pool.firstOrNull { !consumed[it] && predicate(candidates[it]) }
    }

    fun consume(index: Int) {
        consumed.set(index)
    }

    fun hasUnconsumed(): Boolean = consumed.cardinality() < candidates.size

    operator fun get(index: Int): CandidateProduct = candidates[index]
}
