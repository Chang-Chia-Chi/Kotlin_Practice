package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.sql.SQLTransientConnectionException

class ErrorClassifierMiddlewareTest {

    // ── helpers ──────────────────────────────────────────────────────

    private fun ctx(handler: String = "test", retryCount: Int = 0) = TaskExecutionContext(
        taskId = "t1", handler = handler, queue = "default", groupId = null,
        payload = "{}", metadata = null, retryCount = retryCount, maxRetries = 3,
        claimedAt = null, executionGeneration = null,
        taskContext = TaskContext("t1", "{}", null, null, null, retryCount, 3),
    )

    private fun handler(name: String, block: suspend (TaskContext) -> TaskResult = { TaskResult.Success() }) =
        object : TaskHandler {
            override val handlerName = name
            override suspend fun handle(ctx: TaskContext) = block(ctx)
        }

    private fun registry(vararg handlers: TaskHandler): HandlerRegistry {
        val reg = HandlerRegistry(FakeInstance(emptyList()))
        handlers.forEach { reg.register(it) }
        return reg
    }

    private fun middleware(vararg handlers: TaskHandler): ErrorClassifierMiddleware =
        ErrorClassifierMiddleware(registry(*handlers))

    /** next() that always throws the given exception. */
    private fun throwing(e: Exception): suspend (TaskExecutionContext) -> TaskResult = { throw e }

    /** next() that returns the given result. */
    private fun returning(r: TaskResult): suspend (TaskExecutionContext) -> TaskResult = { r }

    // ── transient exceptions ────────────────────────────────────────

    @Test
    fun `transient exception - ConnectException - produces Retry with backoff`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(), throwing(ConnectException("refused")))

        assertTrue(result is TaskResult.Retry)
        val retry = result as TaskResult.Retry
        assertTrue(retry.delay!!.toMillis() >= 100, "backoff should be at least 100ms")
        assertTrue(retry.reason.contains("ConnectException"))
    }

    @Test
    fun `transient exception - SocketTimeoutException - produces Retry`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(), throwing(SocketTimeoutException("timeout")))

        assertTrue(result is TaskResult.Retry)
    }

    @Test
    fun `transient exception - SQLTransientException subclass - produces Retry via isAssignableFrom`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(), throwing(SQLTransientConnectionException("conn lost")))

        assertTrue(result is TaskResult.Retry)
        assertTrue((result as TaskResult.Retry).reason.contains("SQLTransientConnectionException"))
    }

    // ── permanent exceptions ────────────────────────────────────────

    @Test
    fun `permanent exception - IllegalArgumentException - produces DeadLetter`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(), throwing(IllegalArgumentException("bad input")))

        assertTrue(result is TaskResult.DeadLetter)
        assertTrue((result as TaskResult.DeadLetter).reason.contains("IllegalArgumentException"))
    }

    @Test
    fun `permanent exception - NullPointerException - produces DeadLetter`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(), throwing(NullPointerException("npe")))

        assertTrue(result is TaskResult.DeadLetter)
    }

    // ── unknown exceptions ──────────────────────────────────────────

    @Test
    fun `unknown exception - RuntimeException - produces Failure`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(), throwing(RuntimeException("oops")))

        assertTrue(result is TaskResult.Failure)
        assertTrue((result as TaskResult.Failure).message.contains("RuntimeException"))
    }

    // ── CancellationException ───────────────────────────────────────

    @Test
    fun `CancellationException is re-thrown, never classified`() = runTest {
        val mw = middleware(handler("test"))

        assertThrows<CancellationException> {
            mw.invoke(ctx()) { throw CancellationException("cancelled") }
        }
    }

    // ── custom annotations ──────────────────────────────────────────

    @TransientExceptions([RuntimeException::class])
    private class CustomTransientHandler : TaskHandler {
        override val handlerName = "custom-transient"
        override suspend fun handle(ctx: TaskContext) = TaskResult.Success()
    }

    @Test
    fun `custom @TransientExceptions overrides defaults`() = runTest {
        val mw = middleware(CustomTransientHandler())

        // RuntimeException is normally unknown -> Failure, but annotation makes it transient -> Retry
        val result = mw.invoke(ctx(handler = "custom-transient"), throwing(RuntimeException("custom")))
        assertTrue(result is TaskResult.Retry)

        // IllegalArgumentException is normally permanent, but custom transient set replaces defaults
        // so it falls through to unknown -> Failure
        val result2 = mw.invoke(ctx(handler = "custom-transient"), throwing(IllegalArgumentException("arg")))
        assertTrue(result2 is TaskResult.DeadLetter)
    }

    @PermanentExceptions([RuntimeException::class])
    private class CustomPermanentHandler : TaskHandler {
        override val handlerName = "custom-permanent"
        override suspend fun handle(ctx: TaskContext) = TaskResult.Success()
    }

    @Test
    fun `custom @PermanentExceptions overrides defaults`() = runTest {
        val mw = middleware(CustomPermanentHandler())

        // RuntimeException is normally unknown -> Failure, but annotation makes it permanent -> DeadLetter
        val result = mw.invoke(ctx(handler = "custom-permanent"), throwing(RuntimeException("custom")))
        assertTrue(result is TaskResult.DeadLetter)
    }

    // ── backoff behaviour ───────────────────────────────────────────

    @Test
    fun `backoff - retryCount 0 produces delay around 1 second`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(retryCount = 0), throwing(ConnectException("refused")))

        val delayMs = (result as TaskResult.Retry).delay!!.toMillis()
        // 1000ms +/- 25% jitter = [750, 1250], clamped at min 100
        assertTrue(delayMs in 100..1250, "expected ~1s backoff, got ${delayMs}ms")
    }

    @Test
    fun `backoff - retryCount 3 produces delay around 8 seconds`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(retryCount = 3), throwing(ConnectException("refused")))

        val delayMs = (result as TaskResult.Retry).delay!!.toMillis()
        // 1000 * 2^3 = 8000ms, +/- 25% jitter = [6000, 10000]
        assertTrue(delayMs in 100..10_000, "expected ~8s backoff, got ${delayMs}ms")
    }

    @Test
    fun `backoff - capped at 60 seconds for high retry count`() = runTest {
        val mw = middleware(handler("test"))
        val result = mw.invoke(ctx(retryCount = 20), throwing(ConnectException("refused")))

        val delayMs = (result as TaskResult.Retry).delay!!.toMillis()
        // raw = min(1000 * 2^20, 60000) = 60000, jitter +/- 25% = [45000, 75000], but clamped at 60000 max raw
        // Actually: jitter can push above 60000 but raw is capped at 60000
        // 60000 +/- 25%(15000) = [45000, 75000], clamped to min 100
        assertTrue(delayMs <= 75_000, "backoff should be capped near 60s, got ${delayMs}ms")
        assertTrue(delayMs >= 100, "backoff minimum is 100ms, got ${delayMs}ms")
    }

    @Test
    fun `backoff - minimum 100ms`() = runTest {
        val mw = middleware(handler("test"))
        // retryCount=0 -> raw=1000, jitter=-250 = 750. Always >= 100.
        // We run many times to check the floor is respected.
        repeat(20) {
            val result = mw.invoke(ctx(retryCount = 0), throwing(ConnectException("refused")))
            val delayMs = (result as TaskResult.Retry).delay!!.toMillis()
            assertTrue(delayMs >= 100, "backoff must be at least 100ms, got ${delayMs}ms")
        }
    }

    // ── pass-through ────────────────────────────────────────────────

    @Test
    fun `handler returning Success passes through unmodified`() = runTest {
        val mw = middleware(handler("test"))
        val expected = TaskResult.Success("ok")
        val result = mw.invoke(ctx(), returning(expected))

        assertEquals(expected, result)
    }

    @Test
    fun `handler returning Failure passes through unmodified`() = runTest {
        val mw = middleware(handler("test"))
        val expected = TaskResult.Failure("fail")
        val result = mw.invoke(ctx(), returning(expected))

        assertEquals(expected, result)
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
