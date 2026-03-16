package com.mapreduce.queue.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration

class TaskResultTest {

    // ── Success ───────────────────────────────────────────────────

    @Test
    fun `Success with output`() {
        val result = TaskResult.Success(output = "done")

        assertEquals("done", result.output)
    }

    @Test
    fun `Success without output defaults to null`() {
        val result = TaskResult.Success()

        assertNull(result.output)
    }

    @Test
    fun `Success is a TaskResult`() {
        val result: TaskResult = TaskResult.Success("ok")

        assertTrue(result is TaskResult.Success)
    }

    // ── Retry ─────────────────────────────────────────────────────

    @Test
    fun `Retry defaults - delay null, reason set, consumeRetry true`() {
        val result = TaskResult.Retry()

        assertNull(result.delay)
        assertEquals("Retry requested", result.reason)
        assertTrue(result.consumeRetry)
    }

    @Test
    fun `Retry with custom delay and reason`() {
        val result = TaskResult.Retry(
            delay = Duration.ofSeconds(30),
            reason = "Rate limited",
        )

        assertEquals(Duration.ofSeconds(30), result.delay)
        assertEquals("Rate limited", result.reason)
        assertTrue(result.consumeRetry)
    }

    @Test
    fun `Retry non-consuming does not count against budget`() {
        val result = TaskResult.Retry(consumeRetry = false)

        assertEquals(false, result.consumeRetry)
    }

    // ── Failure ───────────────────────────────────────────────────

    @Test
    fun `Failure has message`() {
        val result = TaskResult.Failure(message = "NullPointerException")

        assertEquals("NullPointerException", result.message)
    }

    @Test
    fun `Failure is a TaskResult`() {
        val result: TaskResult = TaskResult.Failure("err")

        assertTrue(result is TaskResult.Failure)
    }

    // ── DeadLetter ────────────────────────────────────────────────

    @Test
    fun `DeadLetter has reason`() {
        val result = TaskResult.DeadLetter(reason = "Invalid payload schema")

        assertEquals("Invalid payload schema", result.reason)
    }

    @Test
    fun `DeadLetter is a TaskResult`() {
        val result: TaskResult = TaskResult.DeadLetter("permanent")

        assertTrue(result is TaskResult.DeadLetter)
    }

    // ── Exhaustive pattern matching ───────────────────────────────

    @Test
    fun `when expression is exhaustive over all subtypes`() {
        val results: List<TaskResult> = listOf(
            TaskResult.Success("ok"),
            TaskResult.Retry(),
            TaskResult.Failure("err"),
            TaskResult.DeadLetter("bad"),
        )

        val labels = results.map { result ->
            when (result) {
                is TaskResult.Success -> "success"
                is TaskResult.Retry -> "retry"
                is TaskResult.Failure -> "failure"
                is TaskResult.DeadLetter -> "dead-letter"
            }
        }

        assertEquals(listOf("success", "retry", "failure", "dead-letter"), labels)
    }

    // ── Data class equality ───────────────────────────────────────

    @Test
    fun `Success data class equality`() {
        assertEquals(TaskResult.Success("a"), TaskResult.Success("a"))
    }

    @Test
    fun `Retry data class equality`() {
        assertEquals(
            TaskResult.Retry(Duration.ofSeconds(5), "r", true),
            TaskResult.Retry(Duration.ofSeconds(5), "r", true),
        )
    }

    @Test
    fun `Failure data class equality`() {
        assertEquals(TaskResult.Failure("x"), TaskResult.Failure("x"))
    }

    @Test
    fun `DeadLetter data class equality`() {
        assertEquals(TaskResult.DeadLetter("z"), TaskResult.DeadLetter("z"))
    }

    @Test
    fun `Success copy with different output`() {
        val original = TaskResult.Success("a")
        val copied = original.copy(output = "b")

        assertEquals("b", copied.output)
    }
}
