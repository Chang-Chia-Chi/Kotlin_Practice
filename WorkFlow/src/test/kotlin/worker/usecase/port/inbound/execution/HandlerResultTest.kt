package com.workflow.worker.usecase.port.inbound.execution

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class HandlerResultTest {

    @Test
    fun `HandlerResult wraps result`() {
        val result = HandlerResult(result = "some-output")
        assertEquals("some-output", result.result)
    }

    @Test
    fun `HandlerResult with null result`() {
        val result = HandlerResult(result = null)
        assertNull(result.result)
    }

    @Test
    fun `HandlerResult preserves fanOutPayloads`() {
        val result = HandlerResult(result = "done", fanOutPayloads = listOf("a", "b"))
        assertEquals("done", result.result)
        assertEquals(listOf("a", "b"), result.fanOutPayloads)
    }
}
