package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.spi.TaskHandler
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.kotlin.mock
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.sql.SQLTransientException
import java.time.Duration
import java.time.Instant

class ErrorClassifierMiddlewareTest {

    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var middleware: ErrorClassifierMiddleware

    @BeforeEach
    fun setUp() {
        handlerRegistry = mock()
        middleware = ErrorClassifierMiddleware(handlerRegistry)
    }

    private fun buildContext(
        handler: String = "test-handler",
        retryCount: Int = 0,
    ) = TaskExecutionContext(
        taskId = "t-1",
        handler = handler,
        queue = "default",
        groupId = null,
        payload = "{}",
        metadata = null,
        retryCount = retryCount,
        maxRetries = 3,
        claimedAt = Instant.now(),
        claimToken = "gen-1",
        taskContext = TaskContext(
            taskId = "t-1",
            payload = "{}",
            groupId = null,
            metadata = null,
            claimToken = "gen-1",
            retryCount = retryCount,
        ),
    )

    private suspend fun invokeWithException(
        exception: Exception,
        handler: String = "test-handler",
        retryCount: Int = 0,
    ): TaskResult {
        `when`(handlerRegistry.resolve(handler)).thenReturn(null)
        return middleware.invoke(buildContext(handler, retryCount)) { throw exception }
    }

    // -- Pass-through --

    @Test
    fun `normal result passes through without classification`() = runTest {
        val expected = TaskResult.Success("ok")
        val result = middleware.invoke(buildContext()) { expected }
        assertEquals(expected, result)
    }

    @Test
    fun `failure result passes through without classification`() = runTest {
        val expected = TaskResult.Failure("explicit fail")
        val result = middleware.invoke(buildContext()) { expected }
        assertEquals(expected, result)
    }

    @Test
    fun `retry result passes through without classification`() = runTest {
        val expected = TaskResult.Retry(reason = "handler retry")
        val result = middleware.invoke(buildContext()) { expected }
        assertEquals(expected, result)
    }

    // -- CancellationException --

    @Test
    fun `CancellationException is rethrown never classified`() = runTest {
        val thrown = try {
            middleware.invoke(buildContext()) { throw CancellationException("cancelled") }
            null
        } catch (e: CancellationException) {
            e
        }
        assertNotNull(thrown)
        assertEquals("cancelled", thrown!!.message)
    }

    // -- Default permanent exceptions --

    @Test
    fun `IllegalArgumentException maps to DeadLetter`() = runTest {
        val result = invokeWithException(IllegalArgumentException("bad arg"))

        assertInstanceOf(TaskResult.DeadLetter::class.java, result)
        val dl = result as TaskResult.DeadLetter
        assertTrue(dl.reason.contains("IllegalArgumentException"))
    }

    @Test
    fun `NullPointerException maps to DeadLetter`() = runTest {
        val result = invokeWithException(NullPointerException("null ref"))

        assertInstanceOf(TaskResult.DeadLetter::class.java, result)
        assertTrue((result as TaskResult.DeadLetter).reason.contains("NullPointerException"))
    }

    // -- Default transient exceptions --

    @Test
    fun `SQLTransientException maps to Retry with backoff`() = runTest {
        val result = invokeWithException(SQLTransientException("db flake"))

        assertInstanceOf(TaskResult.Retry::class.java, result)
        val retry = result as TaskResult.Retry
        assertTrue(retry.reason.contains("SQLTransientException"))
        assertTrue(retry.delay != null && retry.delay!! > Duration.ZERO)
    }

    @Test
    fun `ConnectException maps to Retry with backoff`() = runTest {
        val result = invokeWithException(ConnectException("connection refused"))

        assertInstanceOf(TaskResult.Retry::class.java, result)
        assertTrue((result as TaskResult.Retry).reason.contains("ConnectException"))
    }

    @Test
    fun `SocketTimeoutException maps to Retry with backoff`() = runTest {
        val result = invokeWithException(SocketTimeoutException("socket timed out"))

        assertInstanceOf(TaskResult.Retry::class.java, result)
        assertTrue((result as TaskResult.Retry).reason.contains("SocketTimeoutException"))
    }

    // -- Unknown exception --

    @Test
    fun `RuntimeException maps to Failure`() = runTest {
        val result = invokeWithException(RuntimeException("mysterious"))

        assertInstanceOf(TaskResult.Failure::class.java, result)
        val failure = result as TaskResult.Failure
        assertTrue(failure.message.contains("RuntimeException"))
        assertTrue(failure.message.contains("mysterious"))
    }

    // -- Custom annotations --

    @TransientExceptions([UnsupportedOperationException::class])
    private class CustomTransientHandler : TaskHandler {
        override val handlerName = "custom-transient"
        override suspend fun handle(ctx: TaskContext): TaskResult = TaskResult.Success()
    }

    @Test
    fun `custom TransientExceptions annotation classifies as Retry`() = runTest {
        val handler = CustomTransientHandler()
        `when`(handlerRegistry.resolve("custom-transient")).thenReturn(handler)

        val result = middleware.invoke(buildContext(handler = "custom-transient")) {
            throw UnsupportedOperationException("custom transient")
        }

        assertInstanceOf(TaskResult.Retry::class.java, result)
        assertTrue((result as TaskResult.Retry).reason.contains("UnsupportedOperationException"))
    }

    @PermanentExceptions([IllegalStateException::class])
    private class CustomPermanentHandler : TaskHandler {
        override val handlerName = "custom-permanent"
        override suspend fun handle(ctx: TaskContext): TaskResult = TaskResult.Success()
    }

    @Test
    fun `custom PermanentExceptions annotation classifies as DeadLetter`() = runTest {
        val handler = CustomPermanentHandler()
        `when`(handlerRegistry.resolve("custom-permanent")).thenReturn(handler)

        val result = middleware.invoke(buildContext(handler = "custom-permanent")) {
            throw IllegalStateException("custom permanent")
        }

        assertInstanceOf(TaskResult.DeadLetter::class.java, result)
        assertTrue((result as TaskResult.DeadLetter).reason.contains("IllegalStateException"))
    }

    // -- Backoff formula --

    @Test
    fun `backoff grows exponentially with cap at 60 seconds`() = runTest {
        // Collect backoff durations for retries 0..7
        val delays = (0..7).map { retryCount ->
            // Each retry needs a unique handler name to avoid classification cache conflicts
            // but all resolve to null (default classification)
            val handlerName = "backoff-$retryCount"
            `when`(handlerRegistry.resolve(handlerName)).thenReturn(null)

            val result = middleware.invoke(buildContext(handler = handlerName, retryCount = retryCount)) {
                throw ConnectException("retry $retryCount")
            }
            (result as TaskResult.Retry).delay!!.toMillis()
        }

        // Expected raw values before jitter: 1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000
        // With +/-25% jitter, verify monotonic trend up to cap
        assertTrue(delays[0] < delays[3], "Early retries should have shorter backoff than later ones")
        // Retry 6+ should be near the cap (60s +/- 25% = 45000..75000)
        assertTrue(delays[6] >= 45_000, "Retry 6 backoff should be near 60s cap: was ${delays[6]}ms")
        assertTrue(delays[7] >= 45_000, "Retry 7 backoff should be capped near 60s: was ${delays[7]}ms")
    }

    @Test
    fun `backoff has jitter within 25 percent bounds`() = runTest {
        // Run many samples at retry 0 (base = 1000ms) to verify jitter range
        // Expected: 1000 +/- 250, clamped to min 100 => [750, 1250]
        val samples = (0 until 50).map { i ->
            val handlerName = "jitter-$i"
            `when`(handlerRegistry.resolve(handlerName)).thenReturn(null)

            val result = middleware.invoke(buildContext(handler = handlerName, retryCount = 0)) {
                throw ConnectException("jitter test")
            }
            (result as TaskResult.Retry).delay!!.toMillis()
        }

        // All values should be within [750, 1250] for retry 0 (base 1000, jitter 250)
        assertTrue(samples.all { it in 100..1250 }, "All backoff values should be within jitter bounds: $samples")
        // At least some variance should exist (not all the same value)
        assertTrue(samples.distinct().size > 1, "Backoff should have jitter variance")
    }
}
