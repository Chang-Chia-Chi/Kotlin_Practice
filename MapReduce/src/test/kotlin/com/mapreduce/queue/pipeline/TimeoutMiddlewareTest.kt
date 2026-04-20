package com.mapreduce.queue.pipeline

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.spi.TaskHandler
import com.mapreduce.shutdown.ShutdownSignal
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.kotlin.mock
import java.time.Duration
import java.util.concurrent.TimeUnit

class TimeoutMiddlewareTest {

    private lateinit var config: FrameworkConfig
    private lateinit var pipelineConfig: FrameworkConfig.PipelineConfig
    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var middleware: TimeoutMiddleware

    @BeforeEach
    fun setUp() {
        config = mock()
        pipelineConfig = mock()
        handlerRegistry = mock()

        `when`(config.pipeline()).thenReturn(pipelineConfig)
        `when`(pipelineConfig.defaultTimeout()).thenReturn(Duration.ofMinutes(2))

        middleware = TimeoutMiddleware(config, handlerRegistry)
    }

    private fun buildContext(
        handler: String = "test-handler",
    ) = TaskContext(
        taskId = "t-1",
        handler = handler,
        queue = "default",
        payload = "{}",
        claimToken = "gen-1",
        maxRetries = 3,
    )

    // -- Normal execution --

    @Test
    fun `normal execution within timeout passes through`() = runTest {
        `when`(handlerRegistry.resolve("test-handler")).thenReturn(null)

        val expected = TaskResult.Success("done")
        val result = middleware.invoke(buildContext()) { expected }

        assertEquals(expected, result)
    }

    @Test
    fun `handler result types are preserved`() = runTest {
        `when`(handlerRegistry.resolve("test-handler")).thenReturn(null)

        val retry = TaskResult.Retry(reason = "try again")
        assertEquals(retry, middleware.invoke(buildContext()) { retry })

        val failure = TaskResult.Failure("oops")
        assertEquals(failure, middleware.invoke(buildContext()) { failure })
    }

    // -- Timeout --

    @Test
    fun `timeout returns failure with message`() = runTest {
        `when`(handlerRegistry.resolve("timeout-handler")).thenReturn(null)
        `when`(pipelineConfig.defaultTimeout()).thenReturn(Duration.ofMillis(100))

        // Need a fresh middleware to pick up the new timeout (cache is per handler name)
        val mw = TimeoutMiddleware(config, handlerRegistry)

        val result = mw.invoke(buildContext(handler = "timeout-handler")) {
            delay(10_000) // virtual time: will be advanced past timeout
            TaskResult.Success("never")
        }

        assertInstanceOf(TaskResult.Failure::class.java, result)
        val failure = result as TaskResult.Failure
        assert(failure.message.contains("timed out"))
        assert(failure.message.contains("100ms"))
    }

    // -- Timeout during shutdown --

    @Test
    fun `timeout during shutdown returns retry with consumeRetry false`() = runTest {
        `when`(handlerRegistry.resolve("shutdown-handler")).thenReturn(null)
        `when`(pipelineConfig.defaultTimeout()).thenReturn(Duration.ofMillis(100))

        val mw = TimeoutMiddleware(config, handlerRegistry)

        val result = withContext(ShutdownSignal { true }) {
            mw.invoke(buildContext(handler = "shutdown-handler")) {
                delay(10_000)
                TaskResult.Success("never")
            }
        }

        assertInstanceOf(TaskResult.Retry::class.java, result)
        val retry = result as TaskResult.Retry
        assertEquals(Duration.ZERO, retry.delay)
        assertFalse(retry.consumeRetry)
    }

    // -- Custom @HandlerTimeout annotation --

    @HandlerTimeout(500, unit = TimeUnit.MILLISECONDS)
    private class AnnotatedHandler : TaskHandler {
        override val handlerName = "annotated-handler"
        override suspend fun handle(ctx: TaskContext): TaskResult = TaskResult.Success()
    }

    @Test
    fun `custom HandlerTimeout annotation overrides default`() = runTest {
        val annotatedHandler = AnnotatedHandler()
        `when`(handlerRegistry.resolve("annotated-handler")).thenReturn(annotatedHandler)

        val result = middleware.invoke(buildContext(handler = "annotated-handler")) {
            delay(10_000) // virtual time exceeds 500ms
            TaskResult.Success("never")
        }

        assertInstanceOf(TaskResult.Failure::class.java, result)
        val failure = result as TaskResult.Failure
        assert(failure.message.contains("500ms"))
    }

    // -- Default timeout from config --

    @Test
    fun `default timeout from config used when no annotation`() = runTest {
        `when`(handlerRegistry.resolve("plain-handler")).thenReturn(null)
        `when`(pipelineConfig.defaultTimeout()).thenReturn(Duration.ofMillis(200))

        val mw = TimeoutMiddleware(config, handlerRegistry)

        val result = mw.invoke(buildContext(handler = "plain-handler")) {
            delay(10_000) // exceeds 200ms
            TaskResult.Success("never")
        }

        assertInstanceOf(TaskResult.Failure::class.java, result)
        val failure = result as TaskResult.Failure
        assert(failure.message.contains("200ms"))
    }

    @Test
    fun `handler without annotation uses default 2-minute timeout and completes within it`() = runTest {
        `when`(handlerRegistry.resolve("fast-handler")).thenReturn(null)
        // Default is 2 minutes

        val expected = TaskResult.Success("fast")
        val result = middleware.invoke(buildContext(handler = "fast-handler")) {
            delay(100) // well within 2 minutes
            expected
        }

        assertEquals(expected, result)
    }
}
