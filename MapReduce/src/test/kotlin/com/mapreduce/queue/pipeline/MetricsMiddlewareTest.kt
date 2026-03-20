package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class MetricsMiddlewareTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var middleware: MetricsMiddleware

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        middleware = MetricsMiddleware(registry)
    }

    private fun buildContext(
        handler: String = "test.handler",
        queue: String = "my-queue",
    ) = TaskContext(
        taskId = "t-1",
        handler = handler,
        queue = queue,
        payload = "{}",
    )

    @Test
    fun `order is 20`() {
        assertEquals(20, middleware.order)
    }

    @Nested
    inner class ResultLabeling {

        @Test
        fun `success result records timer and counter with result=success`() = runTest {
            val result = TaskResult.Success("done")

            middleware.invoke(buildContext()) { result }

            val timer = registry.find("taskqueue.handler.duration")
                .tag("result", "success")
                .timer()
            val counter = registry.find("taskqueue.handler.executions")
                .tag("result", "success")
                .counter()

            assertEquals(1, timer!!.count())
            assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) > 0)
            assertEquals(1.0, counter!!.count())
        }

        @Test
        fun `retry result records timer and counter with result=retry`() = runTest {
            val result = TaskResult.Retry(reason = "transient")

            middleware.invoke(buildContext()) { result }

            val timer = registry.find("taskqueue.handler.duration")
                .tag("result", "retry")
                .timer()
            val counter = registry.find("taskqueue.handler.executions")
                .tag("result", "retry")
                .counter()

            assertEquals(1, timer!!.count())
            assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) > 0)
            assertEquals(1.0, counter!!.count())
        }

        @Test
        fun `failure result records timer and counter with result=failure`() = runTest {
            val result = TaskResult.Failure("oops")

            middleware.invoke(buildContext()) { result }

            val timer = registry.find("taskqueue.handler.duration")
                .tag("result", "failure")
                .timer()
            val counter = registry.find("taskqueue.handler.executions")
                .tag("result", "failure")
                .counter()

            assertEquals(1, timer!!.count())
            assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) > 0)
            assertEquals(1.0, counter!!.count())
        }

        @Test
        fun `dead letter result records timer and counter with result=dead_letter`() = runTest {
            val result = TaskResult.DeadLetter("poison pill")

            middleware.invoke(buildContext()) { result }

            val timer = registry.find("taskqueue.handler.duration")
                .tag("result", "dead_letter")
                .timer()
            val counter = registry.find("taskqueue.handler.executions")
                .tag("result", "dead_letter")
                .counter()

            assertEquals(1, timer!!.count())
            assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) > 0)
            assertEquals(1.0, counter!!.count())
        }
    }

    @Nested
    inner class TagVerification {

        @Test
        fun `timer tags include handler name and queue`() = runTest {
            middleware.invoke(buildContext(handler = "billing.charge", queue = "payments")) {
                TaskResult.Success()
            }

            val timer = registry.find("taskqueue.handler.duration")
                .tag("handler", "billing.charge")
                .tag("queue", "payments")
                .tag("result", "success")
                .timer()

            assertEquals(1, timer!!.count(), "Timer should be recorded with handler and queue tags")
        }

        @Test
        fun `counter tags include handler name and result`() = runTest {
            middleware.invoke(buildContext(handler = "billing.charge", queue = "payments")) {
                TaskResult.Failure("bad input")
            }

            val counter = registry.find("taskqueue.handler.executions")
                .tag("handler", "billing.charge")
                .tag("result", "failure")
                .counter()

            assertEquals(1.0, counter!!.count(), "Counter should be recorded with handler and result tags")
        }
    }

    @Nested
    inner class PassThrough {

        @Test
        fun `result is passed through unchanged`() = runTest {
            val success = TaskResult.Success(output = "payload-out", outputUri = "/blob/1")
            val returned = middleware.invoke(buildContext()) { success }
            assertSame(success, returned)

            val retry = TaskResult.Retry(reason = "later")
            val retryReturned = middleware.invoke(buildContext()) { retry }
            assertSame(retry, retryReturned)

            val failure = TaskResult.Failure("kaboom")
            val failureReturned = middleware.invoke(buildContext()) { failure }
            assertSame(failure, failureReturned)

            val deadLetter = TaskResult.DeadLetter("poison")
            val dlReturned = middleware.invoke(buildContext()) { deadLetter }
            assertSame(deadLetter, dlReturned)
        }
    }

    @Nested
    inner class ExceptionPropagation {

        @Test
        fun `exception from next propagates and records exception metrics`() = runTest {
            val exception = RuntimeException("handler exploded")

            assertThrows<RuntimeException> {
                middleware.invoke(buildContext()) { throw exception }
            }

            val timer = registry.find("taskqueue.handler.duration")
                .tag("result", "exception")
                .timer()
            val counter = registry.find("taskqueue.handler.executions")
                .tag("result", "exception")
                .counter()

            assertNotNull(timer, "Timer should be recorded with result=exception when next throws")
            assertEquals(1, timer!!.count())
            assertNotNull(counter, "Counter should be recorded with result=exception when next throws")
            assertEquals(1.0, counter!!.count())
        }
    }
}
