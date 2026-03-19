package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class TracingMiddlewareTest {

    private lateinit var span: Span
    private lateinit var spanBuilder: SpanBuilder
    private lateinit var tracer: Tracer
    private lateinit var middleware: TracingMiddleware

    @BeforeEach
    fun setUp() {
        span = mock()
        spanBuilder = mock()
        tracer = mock()

        whenever(tracer.spanBuilder(any())).thenReturn(spanBuilder)
        whenever(spanBuilder.setAttribute(any<String>(), any<String>())).thenReturn(spanBuilder)
        whenever(spanBuilder.setAttribute(any<String>(), any<Long>())).thenReturn(spanBuilder)
        whenever(spanBuilder.startSpan()).thenReturn(span)

        middleware = TracingMiddleware(tracer)
    }

    private fun buildContext(
        taskId: String = "t-1",
        handler: String = "test.handler",
        queue: String = "default",
        retryCount: Int = 2,
        groupId: String? = null,
    ) = TaskContext(
        taskId = taskId,
        handler = handler,
        queue = queue,
        payload = "{}",
        retryCount = retryCount,
        groupId = groupId,
    )

    @Test
    fun `order is 10`() {
        assertEquals(10, middleware.order)
    }

    @Nested
    inner class SpanAttributes {

        @Test
        fun `span builder receives correct task attributes`() = runTest {
            val ctx = buildContext(taskId = "task-42", handler = "my.handler", queue = "high", retryCount = 5)

            middleware.invoke(ctx) { TaskResult.Success() }

            verify(tracer).spanBuilder("task.execute my.handler")
            verify(spanBuilder).setAttribute("task.id", "task-42")
            verify(spanBuilder).setAttribute("task.handler", "my.handler")
            verify(spanBuilder).setAttribute("task.queue", "high")
            verify(spanBuilder).setAttribute("task.retryCount", 5L)
        }

        @Test
        fun `groupId attribute set when present`() = runTest {
            val ctx = buildContext(groupId = "grp-7")

            middleware.invoke(ctx) { TaskResult.Success() }

            verify(spanBuilder).setAttribute("task.groupId", "grp-7")
        }

        @Test
        fun `groupId attribute not set when null`() = runTest {
            val ctx = buildContext(groupId = null)

            middleware.invoke(ctx) { TaskResult.Success() }

            verify(spanBuilder, never()).setAttribute(eq("task.groupId"), any<String>())
        }
    }

    @Nested
    inner class SuccessResult {

        @Test
        fun `sets span status OK and ends span`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.Success("done") }

            verify(span).setStatus(StatusCode.OK)
            verify(span).end()
        }

        @Test
        fun `returns the result unchanged`() = runTest {
            val expected = TaskResult.Success("payload-out")

            val result = middleware.invoke(buildContext()) { expected }

            assertSame(expected, result)
        }
    }

    @Nested
    inner class RetryResult {

        @Test
        fun `sets span status OK with retry reason and ends span`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.Retry(reason = "transient db") }

            verify(span).setStatus(StatusCode.OK, "retry: transient db")
            verify(span).end()
        }

        @Test
        fun `returns the result unchanged`() = runTest {
            val expected = TaskResult.Retry(reason = "backoff")

            val result = middleware.invoke(buildContext()) { expected }

            assertSame(expected, result)
        }
    }

    @Nested
    inner class FailureResult {

        @Test
        fun `sets span status ERROR with message and ends span`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.Failure("something broke") }

            verify(span).setStatus(StatusCode.ERROR, "something broke")
            verify(span).end()
        }

        @Test
        fun `returns the result unchanged`() = runTest {
            val expected = TaskResult.Failure("oops")

            val result = middleware.invoke(buildContext()) { expected }

            assertSame(expected, result)
        }
    }

    @Nested
    inner class DeadLetterResult {

        @Test
        fun `sets span status ERROR with dead-letter reason and ends span`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.DeadLetter("bad payload") }

            verify(span).setStatus(StatusCode.ERROR, "dead-letter: bad payload")
            verify(span).end()
        }

        @Test
        fun `returns the result unchanged`() = runTest {
            val expected = TaskResult.DeadLetter("permanent")

            val result = middleware.invoke(buildContext()) { expected }

            assertSame(expected, result)
        }
    }

    @Nested
    inner class ExceptionHandling {

        @Test
        fun `records exception and sets ERROR status on span`() = runTest {
            val ex = RuntimeException("kaboom")

            assertThrows<RuntimeException> {
                middleware.invoke(buildContext()) { throw ex }
            }

            verify(span).recordException(ex)
            verify(span).setStatus(StatusCode.ERROR, "kaboom")
        }

        @Test
        fun `rethrows the original exception`() = runTest {
            val ex = IllegalStateException("broken")

            val thrown = assertThrows<IllegalStateException> {
                middleware.invoke(buildContext()) { throw ex }
            }

            assertSame(ex, thrown)
        }

        @Test
        fun `span end is called even when exception is thrown`() = runTest {
            assertThrows<RuntimeException> {
                middleware.invoke(buildContext()) { throw RuntimeException("fail") }
            }

            verify(span).end()
        }

        @Test
        fun `exception with null message uses unknown`() = runTest {
            assertThrows<RuntimeException> {
                middleware.invoke(buildContext()) { throw RuntimeException() }
            }

            verify(span).setStatus(StatusCode.ERROR, "unknown")
        }
    }

    @Nested
    inner class SpanLifecycle {

        @Test
        fun `span end is always called on success`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.Success() }

            verify(span).end()
        }

        @Test
        fun `span end is always called on failure result`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.Failure("err") }

            verify(span).end()
        }

        @Test
        fun `span end is always called on retry result`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.Retry(reason = "later") }

            verify(span).end()
        }

        @Test
        fun `span end is always called on dead letter result`() = runTest {
            middleware.invoke(buildContext()) { TaskResult.DeadLetter("poison") }

            verify(span).end()
        }

        @Test
        fun `span end is always called on exception`() = runTest {
            assertThrows<RuntimeException> {
                middleware.invoke(buildContext()) { throw RuntimeException("boom") }
            }

            verify(span).end()
        }
    }
}
