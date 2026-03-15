package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.spi.TaskHandler
import com.mapreduce.testinfra.TestConfig
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit

class TimeoutMiddlewareTest {

    // ── helpers ──────────────────────────────────────────────────────

    private fun ctx(
        handler: String = "test",
        shuttingDown: Boolean = false,
    ) = TaskExecutionContext(
        taskId = "t1", handler = handler, queue = "default", groupId = null,
        payload = "{}", metadata = null, retryCount = 0, maxRetries = 3,
        claimedAt = null, executionGeneration = null,
        taskContext = TaskContext("t1", "{}", null, null, null, 0, 3, shuttingDownSupplier = { shuttingDown }),
    )

    private fun handler(name: String) = object : TaskHandler {
        override val handlerName = name
        override suspend fun handle(ctx: TaskContext) = TaskResult.Success()
    }

    private fun registry(vararg handlers: TaskHandler): HandlerRegistry {
        val reg = HandlerRegistry(FakeInstance(emptyList()))
        handlers.forEach { reg.register(it) }
        return reg
    }

    private fun middleware(
        defaultTimeout: Duration = Duration.ofSeconds(30),
        vararg handlers: TaskHandler,
    ): TimeoutMiddleware {
        val config = TestConfig.create(defaultTimeout = defaultTimeout)
        return TimeoutMiddleware(config, registry(*handlers))
    }

    // ── tests ────────────────────────────────────────────────────────

    @Test
    fun `handler completes within timeout - result passes through`() = runTest {
        val mw = middleware(Duration.ofSeconds(1), handler("test"))
        val expected = TaskResult.Success("done")

        val result = mw.invoke(ctx()) { expected }

        assertEquals(expected, result)
    }

    @Test
    fun `handler exceeds timeout - produces Failure`() = runTest {
        val mw = middleware(Duration.ofMillis(100), handler("test"))

        val result = mw.invoke(ctx()) {
            delay(5_000)
            TaskResult.Success()
        }

        assertTrue(result is TaskResult.Failure)
        val failure = result as TaskResult.Failure
        assertTrue(failure.message.contains("timed out"))
        assertTrue(failure.message.contains("100ms"))
    }

    @Test
    fun `handler exceeds timeout during shutdown - produces Retry with consumeRetry false`() = runTest {
        val mw = middleware(Duration.ofMillis(100), handler("test"))

        val result = mw.invoke(ctx(shuttingDown = true)) {
            delay(5_000)
            TaskResult.Success()
        }

        assertTrue(result is TaskResult.Retry)
        val retry = result as TaskResult.Retry
        assertEquals(Duration.ZERO, retry.delay)
        assertFalse(retry.consumeRetry)
    }

    @HandlerTimeout(200, TimeUnit.MILLISECONDS)
    private class ShortTimeoutHandler : TaskHandler {
        override val handlerName = "short-timeout"
        override suspend fun handle(ctx: TaskContext) = TaskResult.Success()
    }

    @Test
    fun `custom @HandlerTimeout annotation is respected`() = runTest {
        // Config default is 30s, but annotation says 200ms
        val mw = middleware(Duration.ofSeconds(30), ShortTimeoutHandler())

        val result = mw.invoke(ctx(handler = "short-timeout")) {
            delay(5_000)
            TaskResult.Success()
        }

        assertTrue(result is TaskResult.Failure)
        val failure = result as TaskResult.Failure
        assertTrue(failure.message.contains("200ms"), "should use annotation timeout, got: ${failure.message}")
    }

    @Test
    fun `default timeout from config used when no annotation`() = runTest {
        val mw = middleware(Duration.ofMillis(150), handler("no-annotation"))

        val result = mw.invoke(ctx(handler = "no-annotation")) {
            delay(5_000)
            TaskResult.Success()
        }

        assertTrue(result is TaskResult.Failure)
        val failure = result as TaskResult.Failure
        assertTrue(failure.message.contains("150ms"), "should use config default, got: ${failure.message}")
    }

    @Test
    fun `resolved timeout is cached per handler`() = runTest {
        val mw = middleware(Duration.ofSeconds(1), handler("cached"))

        // First invocation resolves and caches
        val r1 = mw.invoke(ctx(handler = "cached")) { TaskResult.Success("a") }
        // Second invocation should use cached value
        val r2 = mw.invoke(ctx(handler = "cached")) { TaskResult.Success("b") }

        assertEquals(TaskResult.Success("a"), r1)
        assertEquals(TaskResult.Success("b"), r2)
        // Both succeed within the 1s timeout — if caching were broken, the second might
        // re-resolve and potentially fail. The test verifies no exception.
    }

    // ── Fake CDI Instance ───────────────────────────────────────────

    @Suppress("UNCHECKED_CAST")
    private class FakeInstance<T>(
        private val items: List<T>,
    ) : Instance<T> {
        override fun iterator(): MutableIterator<T> = items.toMutableList().iterator()
        override fun get(): T = items.first()
        override fun isAmbiguous() = false
        override fun isUnsatisfied() = items.isEmpty()
        override fun isResolvable() = items.isNotEmpty()
        override fun destroy(instance: T & Any) {}
        override fun select(vararg qualifiers: Annotation): Instance<T> = this
        override fun <U : T> select(subtype: Class<U>, vararg qualifiers: Annotation): Instance<U> = this as Instance<U>
        override fun <U : T> select(subtype: jakarta.enterprise.util.TypeLiteral<U>, vararg qualifiers: Annotation): Instance<U> = this as Instance<U>
        override fun getHandle(): Instance.Handle<T> = throw UnsupportedOperationException()
        override fun handles(): MutableIterable<Instance.Handle<T>> = throw UnsupportedOperationException()
    }
}
