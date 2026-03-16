package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.kotlin.mock
import java.time.Instant
import java.util.stream.Stream

class HandlerPipelineBuilderTest {

    private fun buildContext(handler: String = "test-handler") = TaskExecutionContext(
        taskId = "t-1",
        handler = handler,
        queue = "default",
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

    private fun fakeHandler(name: String = "test-handler", result: TaskResult = TaskResult.Success("ok")): TaskHandler =
        object : TaskHandler {
            override val handlerName: String = name
            override suspend fun handle(ctx: TaskContext): TaskResult = result
        }

    @Suppress("UNCHECKED_CAST")
    private fun instanceOf(vararg middlewares: HandlerMiddleware): Instance<HandlerMiddleware> {
        val instance = mock<Instance<HandlerMiddleware>>()
        `when`(instance.iterator()).thenReturn(middlewares.toMutableList().iterator())
        `when`(instance.stream()).thenReturn(Stream.of(*middlewares))
        // toList() uses iterator()
        return instance
    }

    // -- Tests --

    @Test
    fun `chain calls middlewares in order then handler`() = runTest {
        val callOrder = mutableListOf<String>()

        val outer = object : HandlerMiddleware {
            override val order = 10
            override suspend fun invoke(
                context: TaskExecutionContext,
                next: suspend (TaskExecutionContext) -> TaskResult,
            ): TaskResult {
                callOrder.add("outer-before")
                val result = next(context)
                callOrder.add("outer-after")
                return result
            }
        }

        val inner = object : HandlerMiddleware {
            override val order = 50
            override suspend fun invoke(
                context: TaskExecutionContext,
                next: suspend (TaskExecutionContext) -> TaskResult,
            ): TaskResult {
                callOrder.add("inner-before")
                val result = next(context)
                callOrder.add("inner-after")
                return result
            }
        }

        val handler = object : TaskHandler {
            override val handlerName = "test-handler"
            override suspend fun handle(ctx: TaskContext): TaskResult {
                callOrder.add("handler")
                return TaskResult.Success("done")
            }
        }

        val builder = HandlerPipelineBuilder(instanceOf(inner, outer))
        val chain = builder.chainFor(handler)
        val result = chain(buildContext())

        assertEquals(
            listOf("outer-before", "inner-before", "handler", "inner-after", "outer-after"),
            callOrder,
        )
        assertEquals(TaskResult.Success("done"), result)
    }

    @Test
    fun `chain is cached per handler name`() = runTest {
        val builder = HandlerPipelineBuilder(instanceOf())
        val handler = fakeHandler("cached-handler")

        val chain1 = builder.chainFor(handler)
        val chain2 = builder.chainFor(handler)

        assertSame(chain1, chain2)
    }

    @Test
    fun `different handler names produce different chains`() = runTest {
        val builder = HandlerPipelineBuilder(instanceOf())

        val handlerA = fakeHandler("handler-a", TaskResult.Success("a"))
        val handlerB = fakeHandler("handler-b", TaskResult.Success("b"))

        val chainA = builder.chainFor(handlerA)
        val chainB = builder.chainFor(handlerB)

        assertEquals(TaskResult.Success("a"), chainA(buildContext("handler-a")))
        assertEquals(TaskResult.Success("b"), chainB(buildContext("handler-b")))
    }

    @Test
    fun `handler is the innermost call`() = runTest {
        val expected = TaskResult.Success("from-handler")
        val handler = fakeHandler(result = expected)

        val middleware = object : HandlerMiddleware {
            override val order = 1
            override suspend fun invoke(
                context: TaskExecutionContext,
                next: suspend (TaskExecutionContext) -> TaskResult,
            ): TaskResult = next(context) // pass through
        }

        val builder = HandlerPipelineBuilder(instanceOf(middleware))
        val result = builder.chainFor(handler)(buildContext())

        assertEquals(expected, result)
    }

    @Test
    fun `empty middleware list calls handler directly`() = runTest {
        val expected = TaskResult.Success("direct")
        val handler = fakeHandler(result = expected)

        val builder = HandlerPipelineBuilder(instanceOf())
        val result = builder.chainFor(handler)(buildContext())

        assertEquals(expected, result)
    }

    @Test
    fun `middleware ordering is by order field not insertion order`() = runTest {
        val callOrder = mutableListOf<Int>()

        fun trackingMiddleware(ord: Int) = object : HandlerMiddleware {
            override val order = ord
            override suspend fun invoke(
                context: TaskExecutionContext,
                next: suspend (TaskExecutionContext) -> TaskResult,
            ): TaskResult {
                callOrder.add(ord)
                return next(context)
            }
        }

        // Inserted in reverse order
        val m50 = trackingMiddleware(50)
        val m10 = trackingMiddleware(10)
        val m30 = trackingMiddleware(30)

        val builder = HandlerPipelineBuilder(instanceOf(m50, m10, m30))
        builder.chainFor(fakeHandler())(buildContext())

        assertEquals(listOf(10, 30, 50), callOrder)
    }
}
