package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class MetricsMiddlewareTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var middleware: MetricsMiddleware

    @BeforeEach
    fun setup() {
        registry = SimpleMeterRegistry()
        middleware = MetricsMiddleware(registry)
    }

    // ── helpers ──────────────────────────────────────────────────────

    private fun ctx(handler: String = "test", queue: String = "default") = TaskExecutionContext(
        taskId = "t1", handler = handler, queue = queue, groupId = null,
        payload = "{}", metadata = null, retryCount = 0, maxRetries = 3,
        claimedAt = null, executionGeneration = null,
        taskContext = TaskContext("t1", "{}", null, null, null, 0, 3),
    )

    private fun returning(r: TaskResult): suspend (TaskExecutionContext) -> TaskResult = { r }

    private fun throwing(e: Exception): suspend (TaskExecutionContext) -> TaskResult = { throw e }

    private fun timerCount(handler: String, result: String): Long =
        registry.find("taskqueue.handler.duration")
            .tag("handler", handler)
            .tag("result", result)
            .timer()
            ?.count() ?: 0

    private fun counterValue(name: String, vararg tags: String): Double =
        registry.find(name)
            .tags(*tags)
            .counter()
            ?.count() ?: 0.0

    private fun gaugeValue(handler: String): Double =
        registry.find("taskqueue.handler.inflight")
            .tag("handler", handler)
            .gauge()
            ?.value() ?: 0.0

    // ── success ─────────────────────────────────────────────────────

    @Test
    fun `successful execution records duration timer and success counter`() = runTest {
        middleware.invoke(ctx(), returning(TaskResult.Success("ok")))

        assertEquals(1, timerCount("test", "success"))
        assertEquals(1.0, counterValue("taskqueue.handler.executions", "handler", "test", "result", "success"))
    }

    // ── failure result ──────────────────────────────────────────────

    @Test
    fun `failure result records duration timer and failure counter`() = runTest {
        middleware.invoke(ctx(), returning(TaskResult.Failure("bad")))

        assertEquals(1, timerCount("test", "failure"))
        assertEquals(1.0, counterValue("taskqueue.handler.executions", "handler", "test", "result", "failure"))
    }

    // ── retry result ────────────────────────────────────────────────

    @Test
    fun `retry result records retry counter`() = runTest {
        middleware.invoke(ctx(), returning(TaskResult.Retry(delay = Duration.ofSeconds(1))))

        assertEquals(1, timerCount("test", "retry"))
        assertEquals(1.0, counterValue("taskqueue.handler.executions", "handler", "test", "result", "retry"))
    }

    // ── exception ───────────────────────────────────────────────────

    @Test
    fun `exception records error timer, error counter, and exception class counter`() = runTest {
        assertThrows<IllegalStateException> {
            middleware.invoke(ctx(), throwing(IllegalStateException("boom")))
        }

        assertEquals(1, timerCount("test", "error"))
        assertEquals(1.0, counterValue("taskqueue.handler.executions", "handler", "test", "result", "error"))
        assertEquals(
            1.0,
            counterValue("taskqueue.handler.exceptions", "handler", "test", "exception_class", "IllegalStateException"),
        )
    }

    // ── in-flight gauge ─────────────────────────────────────────────

    @Test
    fun `inflight gauge increments during execution and decrements after`() = runTest {
        var inflightDuringExecution = -1.0

        middleware.invoke(ctx()) { _ ->
            inflightDuringExecution = gaugeValue("test")
            TaskResult.Success()
        }

        assertEquals(1.0, inflightDuringExecution, "in-flight should be 1 during execution")
        assertEquals(0.0, gaugeValue("test"), "in-flight should be 0 after execution")
    }

    @Test
    fun `inflight gauge decrements after exception`() = runTest {
        assertThrows<RuntimeException> {
            middleware.invoke(ctx(), throwing(RuntimeException("fail")))
        }

        assertEquals(0.0, gaugeValue("test"), "in-flight should be 0 after exception")
    }

    // ── multiple handlers ───────────────────────────────────────────

    @Test
    fun `multiple handlers get separate metrics`() = runTest {
        middleware.invoke(ctx(handler = "handler-a"), returning(TaskResult.Success()))
        middleware.invoke(ctx(handler = "handler-b"), returning(TaskResult.Failure("x")))

        assertEquals(1, timerCount("handler-a", "success"))
        assertEquals(0, timerCount("handler-a", "failure"))
        assertEquals(0, timerCount("handler-b", "success"))
        assertEquals(1, timerCount("handler-b", "failure"))

        assertEquals(1.0, counterValue("taskqueue.handler.executions", "handler", "handler-a", "result", "success"))
        assertEquals(1.0, counterValue("taskqueue.handler.executions", "handler", "handler-b", "result", "failure"))
    }
}
