package com.mapreduce.mr.model

import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.model.evaluateFailurePolicy
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class JobModelTest {

    // ── FAIL_GROUP ────────────────────────────────────────────────

    @Test
    fun `FAIL_GROUP returns null when zero failed`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_GROUP, failed = 0, total = 10, failureThreshold = 0.0)
        assertNull(result)
    }

    @Test
    fun `FAIL_GROUP returns reason when any task failed`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_GROUP, failed = 1, total = 10, failureThreshold = 0.0)
        assertNotNull(result)
        assertEquals("FAIL_GROUP: 1 task(s) failed", result)
    }

    @Test
    fun `FAIL_GROUP returns reason with multiple failed`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_GROUP, failed = 5, total = 10, failureThreshold = 0.0)
        assertNotNull(result)
        assertEquals("FAIL_GROUP: 5 task(s) failed", result)
    }

    // ── THRESHOLD ────────────────────────────────────────────────

    @Test
    fun `THRESHOLD returns null when rate is below threshold`() {
        // 1/10 = 10% <= 50%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, failed = 1, total = 10, failureThreshold = 0.5)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD returns reason when rate exceeds threshold`() {
        // 6/10 = 60% > 50%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, failed = 6, total = 10, failureThreshold = 0.5)
        assertNotNull(result)
        assert(result.startsWith("THRESHOLD:"))
    }

    @Test
    fun `THRESHOLD boundary -- exactly at threshold passes`() {
        // 5/10 = 50% is NOT > 50%, so it should pass
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, failed = 5, total = 10, failureThreshold = 0.5)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD with zero failed always passes`() {
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, failed = 0, total = 10, failureThreshold = 0.0)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD all tasks failed exceeds any sub-100 threshold`() {
        // 10/10 = 100% > 90%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, failed = 10, total = 10, failureThreshold = 0.9)
        assertNotNull(result)
    }

    // ── BEST_EFFORT ──────────────────────────────────────────────

    @Test
    fun `BEST_EFFORT always returns null regardless of failures`() {
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, failed = 0, total = 10, failureThreshold = 0.0))
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, failed = 5, total = 10, failureThreshold = 0.0))
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, failed = 10, total = 10, failureThreshold = 0.0))
    }

    // ── Edge case: total = 1 ─────────────────────────────────────

    @Test
    fun `single task with FAIL_GROUP -- zero failed passes`() {
        assertNull(evaluateFailurePolicy(FailurePolicy.FAIL_GROUP, failed = 0, total = 1, failureThreshold = 0.0))
    }

    @Test
    fun `single task with FAIL_GROUP -- one failed fails`() {
        assertNotNull(evaluateFailurePolicy(FailurePolicy.FAIL_GROUP, failed = 1, total = 1, failureThreshold = 0.0))
    }

    @Test
    fun `single task with THRESHOLD -- one failed at 100 pct exceeds any sub-100 threshold`() {
        // 1/1 = 100% > 50%
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, failed = 1, total = 1, failureThreshold = 0.5)
        assertNotNull(result)
    }

    @Test
    fun `single task with BEST_EFFORT -- always passes`() {
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, failed = 1, total = 1, failureThreshold = 0.0))
    }
}
