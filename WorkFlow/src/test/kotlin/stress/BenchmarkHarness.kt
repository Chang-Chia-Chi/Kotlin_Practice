package com.workflow.stress

import java.time.Duration
import java.time.Instant

/**
 * Throughput benchmark result with percentile latency calculations.
 *
 * Results are printed, not asserted — machine variance makes absolute
 * thresholds brittle. Use for relative comparison across runs.
 *
 * Inspired by MIT 6.824's repeated-apply-under-load pattern and
 * Jepsen's Kafka workload throughput measurement.
 */
data class BenchmarkResult(
    val label: String,
    val totalWorkflows: Int,
    val totalTasks: Int,
    val wallClockMs: Long,
    val latencies: List<Long>,
) {
    val workflowsPerSec: Double = if (wallClockMs > 0) totalWorkflows * 1000.0 / wallClockMs else 0.0
    val tasksPerSec: Double = if (wallClockMs > 0) totalTasks * 1000.0 / wallClockMs else 0.0
    val p50ms: Long = percentile(50)
    val p95ms: Long = percentile(95)
    val p99ms: Long = percentile(99)

    private fun percentile(p: Int): Long {
        if (latencies.isEmpty()) return 0
        val sorted = latencies.sorted()
        val index = ((p / 100.0) * sorted.size).toInt().coerceIn(0, sorted.size - 1)
        return sorted[index]
    }

    fun print() {
        println("=== $label ===")
        println("  Workflows: $totalWorkflows | Tasks: $totalTasks | Wall clock: ${wallClockMs}ms")
        println("  Throughput: ${"%.1f".format(workflowsPerSec)} wf/s | ${"%.1f".format(tasksPerSec)} tasks/s")
        println("  Latency: p50=${p50ms}ms  p95=${p95ms}ms  p99=${p99ms}ms")
        println()
    }
}

/**
 * Tracks workflow submission and completion times for benchmark measurement.
 */
class BenchmarkHarness {

    private val submissions = mutableMapOf<String, Instant>()
    private val completions = mutableMapOf<String, Instant>()

    fun recordSubmission(workflowId: String) {
        submissions[workflowId] = Instant.now()
    }

    fun recordCompletion(workflowId: String) {
        completions[workflowId] = Instant.now()
    }

    fun result(label: String, tasksPerWorkflow: Int): BenchmarkResult {
        val latencies = submissions.keys.mapNotNull { wfId ->
            val start = submissions[wfId] ?: return@mapNotNull null
            val end = completions[wfId] ?: return@mapNotNull null
            Duration.between(start, end).toMillis()
        }
        val wallClock = if (submissions.isNotEmpty() && completions.isNotEmpty()) {
            Duration.between(
                submissions.values.min(),
                completions.values.max(),
            ).toMillis()
        } else {
            0L
        }
        return BenchmarkResult(
            label = label,
            totalWorkflows = submissions.size,
            totalTasks = submissions.size * tasksPerWorkflow,
            wallClockMs = wallClock,
            latencies = latencies,
        )
    }

    fun reset() {
        submissions.clear()
        completions.clear()
    }
}
