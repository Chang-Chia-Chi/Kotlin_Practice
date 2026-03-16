package com.mapreduce.shutdown

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ShutdownStateTest {

    @Test
    fun `enum contains exactly four values`() {
        assertEquals(4, ShutdownState.entries.size)
    }

    @Test
    fun `RUNNING exists`() {
        assertEquals("RUNNING", ShutdownState.RUNNING.name)
    }

    @Test
    fun `DRAINING exists`() {
        assertEquals("DRAINING", ShutdownState.DRAINING.name)
    }

    @Test
    fun `RELEASING exists`() {
        assertEquals("RELEASING", ShutdownState.RELEASING.name)
    }

    @Test
    fun `TERMINATED exists`() {
        assertEquals("TERMINATED", ShutdownState.TERMINATED.name)
    }

    @Test
    fun `ordinal ordering is RUNNING lt DRAINING lt RELEASING lt TERMINATED`() {
        assertTrue(ShutdownState.RUNNING.ordinal < ShutdownState.DRAINING.ordinal)
        assertTrue(ShutdownState.DRAINING.ordinal < ShutdownState.RELEASING.ordinal)
        assertTrue(ShutdownState.RELEASING.ordinal < ShutdownState.TERMINATED.ordinal)
    }

    @Test
    fun `ordinals are sequential starting from 0`() {
        assertEquals(0, ShutdownState.RUNNING.ordinal)
        assertEquals(1, ShutdownState.DRAINING.ordinal)
        assertEquals(2, ShutdownState.RELEASING.ordinal)
        assertEquals(3, ShutdownState.TERMINATED.ordinal)
    }

    @Test
    fun `valueOf round-trips for all entries`() {
        for (state in ShutdownState.entries) {
            assertEquals(state, ShutdownState.valueOf(state.name))
        }
    }
}
