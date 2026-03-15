package com.mapreduce.mr.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class FailurePolicyTest {

    // ── FAIL_JOB ─────────────────────────────────────────────────

    @Test
    fun `FAIL_JOB with zero dead-lettered returns null`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 0, totalTasks = 10, failureThreshold = 0.0)
        assertNull(result)
    }

    @Test
    fun `FAIL_JOB with one dead-lettered returns reason`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 1, totalTasks = 10, failureThreshold = 0.0)
        assertNotNull(result)
        assert(result!!.contains("FAIL_JOB"))
        assert(result.contains("1"))
    }

    @Test
    fun `FAIL_JOB with totalTasks 1 and 1 dead returns reason`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 1, totalTasks = 1, failureThreshold = 0.0)
        assertNotNull(result)
    }

    // ── THRESHOLD ────────────────────────────────────────────────

    @Test
    fun `THRESHOLD below threshold returns null`() {
        // 1/10 = 10%, threshold 20% → passes
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 1, totalTasks = 10, failureThreshold = 0.2)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD above threshold returns reason`() {
        // 3/10 = 30%, threshold 20% → fails
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 3, totalTasks = 10, failureThreshold = 0.2)
        assertNotNull(result)
        assert(result!!.contains("THRESHOLD"))
    }

    @Test
    fun `THRESHOLD at exact boundary returns null`() {
        // 2/10 = 20%, threshold 20% → rate is NOT greater than threshold, so passes
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 2, totalTasks = 10, failureThreshold = 0.2)
        assertNull(result)
    }

    @Test
    fun `THRESHOLD just above boundary returns reason`() {
        // 3/10 = 30% > 20% → fails
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 3, totalTasks = 10, failureThreshold = 0.2)
        assertNotNull(result)
    }

    @Test
    fun `THRESHOLD with totalTasks 1 and 1 dead exceeds any sub-100 threshold`() {
        // 1/1 = 100%, threshold 50% → fails
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 1, totalTasks = 1, failureThreshold = 0.5)
        assertNotNull(result)
    }

    // ── BEST_EFFORT ──────────────────────────────────────────────

    @Test
    fun `BEST_EFFORT with many dead-lettered returns null`() {
        val result = evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 99, totalTasks = 100, failureThreshold = 0.0)
        assertNull(result)
    }

    @Test
    fun `BEST_EFFORT with zero dead-lettered returns null`() {
        val result = evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 0, totalTasks = 100, failureThreshold = 0.0)
        assertNull(result)
    }

    @Test
    fun `BEST_EFFORT with totalTasks 1 and 1 dead returns null`() {
        val result = evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 1, totalTasks = 1, failureThreshold = 0.0)
        assertNull(result)
    }

    // ── Cross-policy edge cases ──────────────────────────────────

    @Test
    fun `all policies with zero dead and zero total`() {
        // FAIL_JOB: 0 dead → null
        assertNull(evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 0, totalTasks = 0, failureThreshold = 0.0))

        // BEST_EFFORT: always null
        assertNull(evaluateFailurePolicy(FailurePolicy.BEST_EFFORT, deadLettered = 0, totalTasks = 0, failureThreshold = 0.0))
    }

    @Test
    fun `FAIL_JOB reason message includes dead-lettered count`() {
        val result = evaluateFailurePolicy(FailurePolicy.FAIL_JOB, deadLettered = 7, totalTasks = 20, failureThreshold = 0.0)
        assertNotNull(result)
        assert(result!!.contains("7"))
    }

    @Test
    fun `THRESHOLD reason message includes percentage`() {
        val result = evaluateFailurePolicy(FailurePolicy.THRESHOLD, deadLettered = 5, totalTasks = 10, failureThreshold = 0.2)
        assertNotNull(result)
        // Should contain "50.0%" or similar
        assert(result!!.contains("50.0%"))
        assert(result.contains("20.0%"))
    }
}
