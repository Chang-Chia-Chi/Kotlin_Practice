package com.workflow.infrastructure.shutdown

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ShutdownSignalTest {

    @Test
    fun `ShutdownSignal returns supplier value when true`() {
        val signal = ShutdownSignal { true }

        assertTrue(signal.isShuttingDown)
    }

    @Test
    fun `ShutdownSignal returns supplier value when false`() {
        val signal = ShutdownSignal { false }

        assertFalse(signal.isShuttingDown)
    }

    @Test
    fun `ShutdownSignal tracks dynamic supplier changes`() {
        var shuttingDown = false
        val signal = ShutdownSignal { shuttingDown }

        assertFalse(signal.isShuttingDown)

        shuttingDown = true
        assertTrue(signal.isShuttingDown)
    }

    @Test
    fun `top-level isShuttingDown returns false when no signal in context`() = runTest {
        val result = isShuttingDown()

        assertFalse(result)
    }

    @Test
    fun `top-level isShuttingDown returns true when signal supplier returns true`() = runTest {
        val signal = ShutdownSignal { true }

        val result = withContext(signal) {
            isShuttingDown()
        }

        assertTrue(result)
    }

    @Test
    fun `top-level isShuttingDown returns false when signal supplier returns false`() = runTest {
        val signal = ShutdownSignal { false }

        val result = withContext(signal) {
            isShuttingDown()
        }

        assertFalse(result)
    }
}
