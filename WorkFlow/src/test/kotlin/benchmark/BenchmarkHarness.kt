package com.workflow.benchmark

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

data class LatencyStats(val p50Ms: Long, val p95Ms: Long, val p99Ms: Long)

data class WindowSnapshot(
    val offsetSec: Int,
    val workflowsPerSec: Double,
    val inflightCount: Int,
    val latency: LatencyStats,
)

data class WindowSample(val timestamp: Instant, val inflightCount: Int)

data class ScenarioResult(
    val name: String,
    val parameters: Map<String, Any>,
    val totalWorkflows: Int,
    val totalTasks: Int,
    val wallClockMs: Long,
    val workflowsPerSec: Double,
    val tasksPerSec: Double,
    val latency: LatencyStats,
    val phaseBreakdown: Map<String, PhaseSummary>,
    val windows: List<WindowSnapshot>? = null,
)

class EnhancedBenchmarkHarness {
    private val submissions = ConcurrentHashMap<String, Instant>()
    private val completions = ConcurrentHashMap<String, Instant>()

    fun recordSubmission(workflowId: String) {
        submissions[workflowId] = Instant.now()
    }

    fun recordSubmissionAt(workflowId: String, at: Instant) {
        submissions[workflowId] = at
    }

    fun recordCompletion(workflowId: String) {
        completions[workflowId] = Instant.now()
    }

    fun recordCompletionAt(workflowId: String, at: Instant) {
        completions[workflowId] = at
    }

    fun inflightCount(): Int = submissions.size - completions.size

    fun submittedIds(): Set<String> = submissions.keys.toSet()

    fun completedIds(): Set<String> = completions.keys.toSet()

    fun batchResult(
        label: String,
        tasksPerWorkflow: Int,
        phaseBreakdown: Map<String, PhaseSummary>,
        parameters: Map<String, Any> = emptyMap(),
    ): ScenarioResult {
        val latencies = perWorkflowLatencies()
        val wallClock = wallClockMs()
        val total = submissions.size
        return ScenarioResult(
            name = label,
            parameters = parameters,
            totalWorkflows = total,
            totalTasks = total * tasksPerWorkflow,
            wallClockMs = wallClock,
            workflowsPerSec = if (wallClock > 0) total * 1000.0 / wallClock else 0.0,
            tasksPerSec = if (wallClock > 0) total * tasksPerWorkflow * 1000.0 / wallClock else 0.0,
            latency = latencyStats(latencies),
            phaseBreakdown = phaseBreakdown,
        )
    }

    fun sustainedResult(
        label: String,
        tasksPerWorkflow: Int,
        phaseBreakdown: Map<String, PhaseSummary>,
        parameters: Map<String, Any> = emptyMap(),
        windowDurationMs: Long = 10_000,
        inflightSamples: List<WindowSample>,
    ): ScenarioResult {
        val runStart = submissions.values.minOrNull() ?: Instant.now()
        val runEnd = completions.values.maxOrNull() ?: Instant.now()
        val totalDurationMs = Duration.between(runStart, runEnd).toMillis().coerceAtLeast(1)
        val total = submissions.size

        val windows = mutableListOf<WindowSnapshot>()
        var windowStart = runStart
        var windowIndex = 0
        while (windowStart.isBefore(runEnd)) {
            val windowEnd = windowStart.plusMillis(windowDurationMs)
            val completedInWindow = completions.entries.filter {
                !it.value.isBefore(windowStart) && it.value.isBefore(windowEnd)
            }
            val windowLatencies = completedInWindow.mapNotNull { (wfId, endTime) ->
                submissions[wfId]?.let { Duration.between(it, endTime).toMillis() }
            }
            val inflight = inflightSamples.getOrNull(windowIndex)?.inflightCount ?: 0
            val wfPerSec = if (windowDurationMs > 0) {
                completedInWindow.size * 1000.0 / windowDurationMs
            } else 0.0

            windows.add(WindowSnapshot(
                offsetSec = (windowIndex * windowDurationMs / 1000).toInt(),
                workflowsPerSec = wfPerSec,
                inflightCount = inflight,
                latency = latencyStats(windowLatencies),
            ))
            windowStart = windowEnd
            windowIndex++
        }

        val allLatencies = perWorkflowLatencies()
        return ScenarioResult(
            name = label,
            parameters = parameters,
            totalWorkflows = total,
            totalTasks = total * tasksPerWorkflow,
            wallClockMs = totalDurationMs,
            workflowsPerSec = if (totalDurationMs > 0) total * 1000.0 / totalDurationMs else 0.0,
            tasksPerSec = if (totalDurationMs > 0) total * tasksPerWorkflow * 1000.0 / totalDurationMs else 0.0,
            latency = latencyStats(allLatencies),
            phaseBreakdown = phaseBreakdown,
            windows = windows,
        )
    }

    fun reset() {
        submissions.clear()
        completions.clear()
    }

    private fun perWorkflowLatencies(): List<Long> =
        submissions.keys.mapNotNull { wfId ->
            val start = submissions[wfId] ?: return@mapNotNull null
            val end = completions[wfId] ?: return@mapNotNull null
            Duration.between(start, end).toMillis()
        }

    private fun wallClockMs(): Long {
        val start = submissions.values.minOrNull() ?: return 0
        val end = completions.values.maxOrNull() ?: return 0
        return Duration.between(start, end).toMillis()
    }
}

private fun latencyStats(latencies: List<Long>): LatencyStats {
    if (latencies.isEmpty()) return LatencyStats(0, 0, 0)
    val sorted = latencies.sorted()
    return LatencyStats(
        p50Ms = sorted.percentile(50),
        p95Ms = sorted.percentile(95),
        p99Ms = sorted.percentile(99),
    )
}

private fun List<Long>.percentile(p: Int): Long {
    if (isEmpty()) return 0
    val idx = (p / 100.0 * (size - 1)).toInt().coerceIn(0, size - 1)
    return this[idx]
}
