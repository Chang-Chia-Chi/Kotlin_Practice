package com.workflow.worker.usecase.port.inbound.execution

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class HandlerResultTest {

    @Test
    fun `HandlerResult Completed wraps result`() {
        val result = HandlerResult.Completed(result = "some-output")
        assertEquals("some-output", result.result)
    }

    @Test
    fun `HandlerResult Completed with null result`() {
        val result = HandlerResult.Completed(result = null)
        assertNull(result.result)
    }

    @Test
    fun `HandlerResult Defer carries trigger metadata`() {
        val result = HandlerResult.Defer(
            triggerType = "k8s-job",
            triggerMeta = """{"jobName":"my-job","namespace":"default"}""",
        )
        assertEquals("k8s-job", result.triggerType)
        assertEquals("""{"jobName":"my-job","namespace":"default"}""", result.triggerMeta)
    }

    @Test
    fun `exhaustive when on HandlerResult`() {
        val results: List<HandlerResult> = listOf(
            HandlerResult.Completed(result = "done"),
            HandlerResult.Defer(triggerType = "sql-exec", triggerMeta = "{}"),
        )
        val labels = results.map { hr ->
            when (hr) {
                is HandlerResult.Completed -> "completed"
                is HandlerResult.Defer -> "defer"
            }
        }
        assertEquals(listOf("completed", "defer"), labels)
    }
}
