package com.workflow.benchmark

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PhaseTimerTest {

    @Test
    fun `records timing and computes percentiles`() {
        val timer = PhaseTimer()
        repeat(100) { timer.time("test.phase") { Thread.sleep(1) } }
        val summary = timer.summary()
        val phase = summary["test.phase"]!!
        assertEquals(100, phase.count)
        assertTrue(phase.meanMs > 0.0)
        assertTrue(phase.p50Ms > 0.0)
        assertTrue(phase.p95Ms >= phase.p50Ms)
        assertTrue(phase.p99Ms >= phase.p95Ms)
    }

    @Test
    fun `reset clears all recordings`() {
        val timer = PhaseTimer()
        timer.time("a") { Thread.sleep(1) }
        timer.time("b") { Thread.sleep(1) }
        timer.reset()
        assertTrue(timer.summary().isEmpty())
    }

    @Test
    fun `multiple phases tracked independently`() {
        val timer = PhaseTimer()
        repeat(10) { timer.time("fast") { } }
        repeat(5) { timer.time("slow") { Thread.sleep(2) } }
        val summary = timer.summary()
        assertEquals(10, summary["fast"]!!.count)
        assertEquals(5, summary["slow"]!!.count)
        assertTrue(summary["slow"]!!.meanMs > summary["fast"]!!.meanMs)
    }

    @Test
    fun `time returns the block result`() {
        val timer = PhaseTimer()
        val result = timer.time("phase") { 42 }
        assertEquals(42, result)
    }

    @Test
    fun `suspendTime records timing and returns block result`() = runTest {
        val timer = PhaseTimer()
        val result = timer.suspendTime("suspend.phase") { 42 }
        assertEquals(42, result)
        val summary = timer.summary()
        assertEquals(1, summary["suspend.phase"]!!.count)
    }

    @Test
    fun `time records timing even when block throws`() {
        val timer = PhaseTimer()
        assertThrows<RuntimeException> {
            timer.time("err") { throw RuntimeException("boom") }
        }
        assertEquals(1, timer.summary()["err"]!!.count)
    }
}
