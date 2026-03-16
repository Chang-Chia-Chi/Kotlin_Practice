package com.mapreduce.mr.model

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class JobModelTest {

    // ── FAIL_JOB ─────────────────────────────────────────────────

    @Test
    fun `FAIL_JOB returns null when zero dead-lettered`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 0, totalTasks = 10, failureThreshold = 0.0)
        assertNull(result)
    }

    @Test
    fun `FAIL_JOB returns reason when any task is dead-lettered`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 1, totalTasks = 10, failureThreshold = 0.0)
        assertNotNull(result)
        assertEquals("FAIL_JOB: 1 task(s) dead-lettered", result)
    }

    @Test
    fun `FAIL_JOB returns reason with multiple dead-lettered`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 5, totalTasks = 10, failureThreshold = 0.0)
        assertNotNull(result)
        assertEquals("FAIL_JOB: 5 task(s) dead-lettered", result)
    }

    // ── THRESHOLD ────────────────────────────────────────────────

    @Test
    fun `THRESHOLD returns null when rate is below threshold`() {
        // 1/10 = 10% <= 50%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 1, totalTasks = 10, failureThreshold = 0.5)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD returns reason when rate exceeds threshold`() {
        // 6/10 = 60% > 50%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 6, totalTasks = 10, failureThreshold = 0.5)
        assertNotNull(result)
        assert(result.startsWith("THRESHOLD:"))
    }

    @Test
    fun `THRESHOLD boundary -- exactly at threshold passes`() {
        // 5/10 = 50% is NOT > 50%, so it should pass
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 5, totalTasks = 10, failureThreshold = 0.5)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD with zero dead-lettered always passes`() {
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 0, totalTasks = 10, failureThreshold = 0.0)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD all tasks dead-lettered exceeds any sub-100 threshold`() {
        // 10/10 = 100% > 90%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 10, totalTasks = 10, failureThreshold = 0.9)
        assertNotNull(result)
    }

    // ── BEST_EFFORT ──────────────────────────────────────────────

    @Test
    fun `BEST_EFFORT always returns null regardless of failures`() {
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 0, totalTasks = 10, failureThreshold = 0.0))
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 5, totalTasks = 10, failureThreshold = 0.0))
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 10, totalTasks = 10, failureThreshold = 0.0))
    }

    // ── Edge case: totalTasks = 1 ────────────────────────────────

    @Test
    fun `single task with FAIL_JOB -- zero dead-lettered passes`() {
        assertNull(evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 0, totalTasks = 1, failureThreshold = 0.0))
    }

    @Test
    fun `single task with FAIL_JOB -- one dead-lettered fails`() {
        assertNotNull(evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 1, totalTasks = 1, failureThreshold = 0.0))
    }

    @Test
    fun `single task with THRESHOLD -- one dead-lettered at 100 pct exceeds any sub-100 threshold`() {
        // 1/1 = 100% > 50%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 1, totalTasks = 1, failureThreshold = 0.5)
        assertNotNull(result)
    }

    @Test
    fun `single task with BEST_EFFORT -- always passes`() {
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 1, totalTasks = 1, failureThreshold = 0.0))
    }
}
