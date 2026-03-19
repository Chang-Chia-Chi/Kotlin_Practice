package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class TaskPipelineTest {

    private fun buildContext() = TaskContext(
        taskId = "t-1",
        handler = "test.handler",
        queue = "default",
        payload = "{}",
        claimToken = "gen-1",
    )

    private fun handler(result: TaskResult = TaskResult.Success("done")): TaskHandler =
        object : TaskHandler {
            override val handlerName = "test.handler"
            override suspend fun handle(ctx: TaskContext) = result
        }

    private fun middlewareInstance(vararg middlewares: Middleware): Instance<Middleware> {
        val list = middlewares.toList()
        val instance = mock<Instance<Middleware>>(defaultAnswer = org.mockito.Mockito.RETURNS_DEEP_STUBS)
        whenever(instance.iterator()).thenAnswer { list.iterator() }
        return instance
    }

    // ── No middleware ─────────────────────────────────────────────

    @Nested
    inner class NoMiddleware {

        @Test
        fun `executes handler directly when no middleware`() = runTest {
            val pipeline = TaskPipeline(middlewareInstance())
            val result = pipeline.execute(buildContext(), handler())

            assertEquals(TaskResult.Success("done"), result)
        }

        @Test
        fun `propagates handler result type`() = runTest {
            val pipeline = TaskPipeline(middlewareInstance())
            val failure = TaskResult.Failure("boom")
            val result = pipeline.execute(buildContext(), handler(failure))

            assertInstanceOf(TaskResult.Failure::class.java, result)
            assertEquals("boom", (result as TaskResult.Failure).message)
        }
    }

    // ── Middleware ordering ───────────────────────────────────────

    @Nested
    inner class MiddlewareOrdering {

        @Test
        fun `middleware wraps handler in order`() = runTest {
            val callOrder = mutableListOf<String>()

            val m1 = object : Middleware {
                override val order = 10
                override suspend fun invoke(
                    context: TaskContext,
                    next: suspend (TaskContext) -> TaskResult,
                ): TaskResult {
                    callOrder.add("m1-before")
                    val result = next(context)
                    callOrder.add("m1-after")
                    return result
                }
            }
            val m2 = object : Middleware {
                override val order = 20
                override suspend fun invoke(
                    context: TaskContext,
                    next: suspend (TaskContext) -> TaskResult,
                ): TaskResult {
                    callOrder.add("m2-before")
                    val result = next(context)
                    callOrder.add("m2-after")
                    return result
                }
            }

            val pipeline = TaskPipeline(middlewareInstance(m2, m1)) // out of order input
            pipeline.execute(buildContext(), handler())

            assertEquals(listOf("m1-before", "m2-before", "m2-after", "m1-after"), callOrder)
        }

        @Test
        fun `lower order middleware is outermost`() = runTest {
            val outerSawInner = mutableListOf<Boolean>()

            val outer = object : Middleware {
                override val order = 5
                override suspend fun invoke(
                    context: TaskContext,
                    next: suspend (TaskContext) -> TaskResult,
                ): TaskResult {
                    outerSawInner.add(true)
                    return next(context)
                }
            }
            val inner = object : Middleware {
                override val order = 50
                override suspend fun invoke(
                    context: TaskContext,
                    next: suspend (TaskContext) -> TaskResult,
                ): TaskResult = next(context)
            }

            val pipeline = TaskPipeline(middlewareInstance(inner, outer))
            pipeline.execute(buildContext(), handler())

            assertEquals(1, outerSawInner.size)
        }
    }

    // ── Middleware can short-circuit ──────────────────────────────

    @Nested
    inner class ShortCircuit {

        @Test
        fun `middleware can return without calling next`() = runTest {
            val shortCircuit = object : Middleware {
                override val order = 10
                override suspend fun invoke(
                    context: TaskContext,
                    next: suspend (TaskContext) -> TaskResult,
                ): TaskResult = TaskResult.Failure("blocked")
            }

            val pipeline = TaskPipeline(middlewareInstance(shortCircuit))
            val result = pipeline.execute(buildContext(), handler())

            assertInstanceOf(TaskResult.Failure::class.java, result)
            assertEquals("blocked", (result as TaskResult.Failure).message)
        }
    }

    // ── Exception propagation ────────────────────────────────────

    @Nested
    inner class ExceptionPropagation {

        @Test
        fun `handler exception propagates through middleware`() = runTest {
            val throwingHandler = object : TaskHandler {
                override val handlerName = "test.handler"
                override suspend fun handle(ctx: TaskContext): TaskResult {
                    throw RuntimeException("handler error")
                }
            }

            val pipeline = TaskPipeline(middlewareInstance())
            val thrown = try {
                pipeline.execute(buildContext(), throwingHandler)
                null
            } catch (e: RuntimeException) {
                e
            }

            assertEquals("handler error", thrown?.message)
        }
    }
}
