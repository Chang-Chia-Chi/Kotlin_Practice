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
import java.time.Instant

class MetricsMiddlewareTest {

    private lateinit var registry: SimpleMeterRegistry
    private lateinit var middleware: MetricsMiddleware

    private fun buildContext(handler: String = "test-handler", queue: String = "default") =
        TaskExecutionContext(
            taskId = "t-1",
            handler = handler,
            queue = queue,
            groupId = null,
            payload = "{}",
            metadata = null,
            retryCount = 0,
            maxRetries = 3,
            claimedAt = Instant.now(),
            executionGeneration = "gen-1",
            taskContext = TaskContext(
                taskId = "t-1",
                payload = "{}",
                groupId = null,
                metadata = null,
                executionGeneration = "gen-1",
            ),
        )

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        middleware = MetricsMiddleware(registry)
    }

    private suspend fun invokeWith(
        result: TaskResult,
        handler: String = "test-handler",
    ): TaskResult =
        middleware.invoke(buildContext(handler)) { result }

    private suspend fun invokeWithException(
        exception: Exception,
        handler: String = "test-handler",
    ) {
        middleware.invoke(buildContext(handler)) { throw exception }
    }

    // -- Duration timer --

    @Test
    fun `duration timer recorded on success`() = runTest {
        invokeWith(TaskResult.Success("ok"))

        val timer = registry.find("taskqueue.handler.duration")
            .tag("handler", "test-handler")
            .tag("result", "success")
            .timer()

        assertTrue(timer != null && timer.count() == 1L)
    }

    @Test
    fun `duration timer recorded on exception with error label`() = runTest {
        runCatching { invokeWithException(RuntimeException("boom")) }

        val timer = registry.find("taskqueue.handler.duration")
            .tag("handler", "test-handler")
            .tag("result", "error")
            .timer()

        assertTrue(timer != null && timer.count() == 1L)
    }

    @Test
    fun `duration timer records correct result labels for each result type`() = runTest {
        invokeWith(TaskResult.Success())
        invokeWith(TaskResult.Retry())
        invokeWith(TaskResult.Failure("fail"))
        invokeWith(TaskResult.DeadLetter("dead"))

        assertEquals(1L, registry.find("taskqueue.handler.duration").tag("result", "success").timer()?.count())
        assertEquals(1L, registry.find("taskqueue.handler.duration").tag("result", "retry").timer()?.count())
        assertEquals(1L, registry.find("taskqueue.handler.duration").tag("result", "failure").timer()?.count())
        assertEquals(1L, registry.find("taskqueue.handler.duration").tag("result", "dead_letter").timer()?.count())
    }

    // -- Execution counter --

    @Test
    fun `execution counter incremented with success label`() = runTest {
        invokeWith(TaskResult.Success("ok"))

        val counter = registry.find("taskqueue.handler.executions")
            .tag("handler", "test-handler")
            .tag("result", "success")
            .counter()

        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `execution counter incremented with error label on exception`() = runTest {
        runCatching { invokeWithException(RuntimeException("boom")) }

        val counter = registry.find("taskqueue.handler.executions")
            .tag("handler", "test-handler")
            .tag("result", "error")
            .counter()

        assertEquals(1.0, counter?.count())
    }

    // -- Inflight gauge --

    @Test
    fun `inflight gauge returns to zero after success`() = runTest {
        invokeWith(TaskResult.Success("ok"))

        val gauge = registry.find("taskqueue.handler.inflight")
            .tag("handler", "test-handler")
            .gauge()

        assertEquals(0.0, gauge?.value())
    }

    @Test
    fun `inflight gauge returns to zero after exception`() = runTest {
        runCatching { invokeWithException(RuntimeException("boom")) }

        val gauge = registry.find("taskqueue.handler.inflight")
            .tag("handler", "test-handler")
            .gauge()

        assertEquals(0.0, gauge?.value())
    }

    // -- Exception counter --

    @Test
    fun `exception counter incremented on exception`() = runTest {
        runCatching { invokeWithException(IllegalStateException("bad state")) }

        val counter = registry.find("taskqueue.handler.exceptions")
            .tag("handler", "test-handler")
            .tag("exception_class", "IllegalStateException")
            .counter()

        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `exception counter not created on normal result`() = runTest {
        invokeWith(TaskResult.Failure("fail"))

        val counter = registry.find("taskqueue.handler.exceptions")
            .tag("handler", "test-handler")
            .counter()

        // No exception counter should exist since no exception was thrown
        assertTrue(counter == null || counter.count() == 0.0)
    }

    // -- Exception propagation --

    @Test
    fun `exception is rethrown after metrics are recorded`() = runTest {
        val thrown = assertThrows<RuntimeException> {
            invokeWithException(RuntimeException("boom"))
        }
        assertEquals("boom", thrown.message)
    }
}
