package com.workflow.benchmark

import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class BenchmarkHarnessTest {

    @Test
    fun `batch result computes throughput and latency`() {
        val harness = EnhancedBenchmarkHarness()
        val base = Instant.now()
        harness.recordSubmissionAt("wf1", base)
        harness.recordSubmissionAt("wf2", base.plusMillis(10))
        harness.recordSubmissionAt("wf3", base.plusMillis(20))
        harness.recordCompletionAt("wf1", base.plusMillis(100))
        harness.recordCompletionAt("wf2", base.plusMillis(210))
        harness.recordCompletionAt("wf3", base.plusMillis(320))

        val breakdown = mapOf("task.claim" to PhaseSummary(3, 1.0, 1.0, 1.0, 1.0))
        val result = harness.batchResult("test", tasksPerWorkflow = 2, phaseBreakdown = breakdown)

        assertEquals(3, result.totalWorkflows)
        assertEquals(6, result.totalTasks)
        assertTrue(result.wallClockMs in 300..321)
        assertTrue(result.workflowsPerSec > 0.0)
        assertTrue(result.latency.p50Ms in 100..300)
        assertEquals(1, result.phaseBreakdown.size)
        assertNull(result.windows)
    }

    @Test
    fun `sustained result buckets completions into windows`() {
        val harness = EnhancedBenchmarkHarness()
        val base = Instant.now()

        for (i in 0 until 5) {
            harness.recordSubmissionAt("w0-$i", base.plusMillis(i * 100L))
            harness.recordCompletionAt("w0-$i", base.plusMillis(500 + i * 100L))
        }
        for (i in 0 until 3) {
            harness.recordSubmissionAt("w1-$i", base.plusMillis(10_000 + i * 100L))
            harness.recordCompletionAt("w1-$i", base.plusMillis(10_500 + i * 100L))
        }

        val inflight = listOf(
            WindowSample(base.plusMillis(10_000), 2),
            WindowSample(base.plusMillis(20_000), 0),
        )
        val result = harness.sustainedResult(
            "test", tasksPerWorkflow = 1, phaseBreakdown = emptyMap(),
            windowDurationMs = 10_000, inflightSamples = inflight,
        )

        assertEquals(8, result.totalWorkflows)
        assertTrue(result.windows!!.size >= 2)
        assertTrue(result.windows!![0].workflowsPerSec > 0.0)
    }

    @Test
    fun `inflight count tracks unfinished workflows`() {
        val harness = EnhancedBenchmarkHarness()
        harness.recordSubmission("a")
        harness.recordSubmission("b")
        assertEquals(2, harness.inflightCount())
        harness.recordCompletion("a")
        assertEquals(1, harness.inflightCount())
    }

    @Test
    fun `reset clears all state`() {
        val harness = EnhancedBenchmarkHarness()
        harness.recordSubmission("a")
        harness.recordCompletion("a")
        harness.reset()
        assertEquals(0, harness.inflightCount())
    }
}
