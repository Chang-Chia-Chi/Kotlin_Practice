// src/test/kotlin/benchmark/PhaseTimer.kt
package com.workflow.benchmark

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

data class PhaseSummary(
    val count: Int,
    val meanMs: Double,
    val p50Ms: Double,
    val p95Ms: Double,
    val p99Ms: Double,
)

class PhaseTimer {
    private val recordings = ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>()

    fun <T> time(phase: String, block: () -> T): T {
        val start = System.nanoTime()
        try {
            return block()
        } finally {
            recordings.getOrPut(phase) { CopyOnWriteArrayList() }
                .add(System.nanoTime() - start)
        }
    }

    suspend fun <T> suspendTime(phase: String, block: suspend () -> T): T {
        val start = System.nanoTime()
        try {
            return block()
        } finally {
            recordings.getOrPut(phase) { CopyOnWriteArrayList() }
                .add(System.nanoTime() - start)
        }
    }

    fun summary(): Map<String, PhaseSummary> =
        recordings.mapValues { (_, nanos) ->
            val ms = nanos.map { it / 1_000_000.0 }.sorted()
            PhaseSummary(
                count = ms.size,
                meanMs = ms.average(),
                p50Ms = ms.percentile(50),
                p95Ms = ms.percentile(95),
                p99Ms = ms.percentile(99),
            )
        }

    fun reset() = recordings.clear()
}

private fun List<Double>.percentile(p: Int): Double {
    if (isEmpty()) return 0.0
    val idx = (p / 100.0 * (size - 1)).toInt().coerceIn(0, size - 1)
    return this[idx]
}
