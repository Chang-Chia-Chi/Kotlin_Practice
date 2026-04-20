package com.workflow.worker.usecase.port.inbound.trigger

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TriggerResultTest {

    @Test
    fun `Succeeded carries taskId and result`() {
        val r: TriggerResult = TriggerResult.Succeeded("t-1", """{"key":"value"}""")
        assertEquals("t-1", r.taskId)
        assertTrue(r is TriggerResult.Succeeded)
        assertEquals("""{"key":"value"}""", (r as TriggerResult.Succeeded).result)
    }

    @Test
    fun `Succeeded with null result`() {
        val r = TriggerResult.Succeeded("t-2", null)
        assertEquals("t-2", r.taskId)
        assertNull(r.result)
    }

    @Test
    fun `Failed carries taskId and reason`() {
        val r: TriggerResult = TriggerResult.Failed("t-3", "Job exited with code 1")
        assertEquals("t-3", r.taskId)
        assertEquals("Job exited with code 1", (r as TriggerResult.Failed).reason)
    }

    @Test
    fun `exhaustive when on TriggerResult covers all subtypes`() {
        val results: List<TriggerResult> = listOf(
            TriggerResult.Succeeded("t-1", "ok"),
            TriggerResult.Failed("t-2", "err"),
        )
        for (r in results) {
            val label = when (r) {
                is TriggerResult.Succeeded -> "succeeded"
                is TriggerResult.Failed -> "failed"
            }
            assertTrue(label.isNotEmpty())
        }
    }

    @Test
    fun `Succeeded equality and copy`() {
        val a = TriggerResult.Succeeded("t-1", "data")
        val b = TriggerResult.Succeeded("t-1", "data")
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())

        val c = a.copy(result = "updated")
        assertEquals("updated", c.result)
        assertEquals("t-1", c.taskId)
    }

    @Test
    fun `Failed equality and copy`() {
        val a = TriggerResult.Failed("t-1", "reason")
        val b = TriggerResult.Failed("t-1", "reason")
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())

        val c = a.copy(reason = "new reason")
        assertEquals("new reason", c.reason)
        assertEquals("t-1", c.taskId)
    }

    @Test
    fun `Succeeded and Failed are not equal even with same taskId`() {
        val s: TriggerResult = TriggerResult.Succeeded("t-1", null)
        val f: TriggerResult = TriggerResult.Failed("t-1", "err")
        assertTrue(s != f)
    }

    @Test
    fun `taskId is accessible via sealed interface`() {
        val results: List<TriggerResult> = listOf(
            TriggerResult.Succeeded("s-1", "ok"),
            TriggerResult.Failed("f-1", "err"),
        )
        assertEquals("s-1", results[0].taskId)
        assertEquals("f-1", results[1].taskId)
    }
}
