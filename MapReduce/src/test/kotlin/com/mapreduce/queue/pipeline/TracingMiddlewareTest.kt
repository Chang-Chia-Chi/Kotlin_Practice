package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock

class TracingMiddlewareTest {

    private lateinit var tracer: Tracer
    private lateinit var spanBuilder: SpanBuilder
    private lateinit var span: Span
    private lateinit var middleware: TracingMiddleware

    @BeforeEach
    fun setUp() {
        tracer = mock()
        spanBuilder = mock()
        span = mock()

        // Chain: tracer.spanBuilder(...) -> spanBuilder.setAttribute(...) -> ... -> spanBuilder.startSpan() -> span
        `when`(tracer.spanBuilder(any())).thenReturn(spanBuilder)
        `when`(spanBuilder.setAttribute(any<String>(), any<String>())).thenReturn(spanBuilder)
        `when`(spanBuilder.setAttribute(any<String>(), any<Long>())).thenReturn(spanBuilder)
        `when`(spanBuilder.startSpan()).thenReturn(span)

        middleware = TracingMiddleware(tracer)
    }

    private fun buildContext(
        handler: String = "test-handler",
        taskId: String = "t-1",
        queue: String = "default",
        retryCount: Int = 0,
        groupId: String? = null,
    ) = TaskExecutionContext(
        taskId = taskId,
        handler = handler,
        queue = queue,
        groupId = groupId,
        payload = "{}",
        metadata = null,
        retryCount = retryCount,
        maxRetries = 3,
        claimedAt = java.time.Instant.now(),
        executionGeneration = "gen-1",
        taskContext = TaskContext(
            taskId = taskId,
            payload = "{}",
            groupId = groupId,
            metadata = null,
            executionGeneration = "gen-1",
            retryCount = retryCount,
        ),
    )

    // -- Span creation --

    @Test
    fun `span created with correct name and attributes`() = runTest {
        val ctx = buildContext(
            handler = "email.send",
            taskId = "task-42",
            queue = "priority",
            retryCount = 2,
            groupId = "group-7",
        )

        middleware.invoke(ctx) { TaskResult.Success("ok") }

        verify(tracer).spanBuilder("task.execute email.send")
        verify(spanBuilder).setAttribute("task.id", "task-42")
        verify(spanBuilder).setAttribute("task.handler", "email.send")
        verify(spanBuilder).setAttribute("task.queue", "priority")
        verify(spanBuilder).setAttribute("task.retryCount", 2L)
        verify(spanBuilder).setAttribute("task.groupId", "group-7")
        verify(spanBuilder).startSpan()
    }

    @Test
    fun `groupId attribute is skipped when null`() = runTest {
        middleware.invoke(buildContext(groupId = null)) { TaskResult.Success() }

        verify(spanBuilder, never()).setAttribute(eq("task.groupId"), any<String>())
    }

    // -- Span status on success --

    @Test
    fun `span status OK on Success result`() = runTest {
        middleware.invoke(buildContext()) { TaskResult.Success("ok") }

        verify(span).setStatus(StatusCode.OK)
    }

    @Test
    fun `span status OK on Retry result`() = runTest {
        middleware.invoke(buildContext()) { TaskResult.Retry(reason = "try again") }

        verify(span).setStatus(StatusCode.OK, "retry: try again")
    }

    // -- Span status on failure --

    @Test
    fun `span status ERROR on Failure result`() = runTest {
        middleware.invoke(buildContext()) { TaskResult.Failure("broken") }

        verify(span).setStatus(StatusCode.ERROR, "broken")
    }

    @Test
    fun `span status ERROR on DeadLetter result`() = runTest {
        middleware.invoke(buildContext()) { TaskResult.DeadLetter("permanent") }

        verify(span).setStatus(StatusCode.ERROR, "dead-letter: permanent")
    }

    // -- Exception handling --

    @Test
    fun `exception recorded on span when thrown`() = runTest {
        val exception = RuntimeException("boom")

        assertThrows<RuntimeException> {
            middleware.invoke(buildContext()) { throw exception }
        }

        verify(span).setStatus(StatusCode.ERROR, "boom")
        verify(span).recordException(exception)
    }

    @Test
    fun `exception is rethrown after recording`() = runTest {
        val exception = IllegalStateException("bad state")

        val thrown = assertThrows<IllegalStateException> {
            middleware.invoke(buildContext()) { throw exception }
        }

        assertEquals("bad state", thrown.message)
    }

    // -- Span always ended --

    @Test
    fun `span ended after successful execution`() = runTest {
        middleware.invoke(buildContext()) { TaskResult.Success() }

        verify(span).end()
    }

    @Test
    fun `span ended after failure result`() = runTest {
        middleware.invoke(buildContext()) { TaskResult.Failure("fail") }

        verify(span).end()
    }

    @Test
    fun `span ended even when exception thrown`() = runTest {
        runCatching {
            middleware.invoke(buildContext()) { throw RuntimeException("boom") }
        }

        verify(span).end()
    }

    @Test
    fun `span end is called after status is set`() = runTest {
        middleware.invoke(buildContext()) { TaskResult.Success("ok") }

        val ordered = inOrder(span)
        ordered.verify(span).setStatus(StatusCode.OK)
        ordered.verify(span).end()
    }
}
