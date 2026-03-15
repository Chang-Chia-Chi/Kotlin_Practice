package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class HandlerPipelineBuilderTest {

    // ── helpers ──────────────────────────────────────────────────────

    private fun ctx(handler: String = "test") = TaskExecutionContext(
        taskId = "t1", handler = handler, queue = "default", groupId = null,
        payload = "{}", metadata = null, retryCount = 0, maxRetries = 3,
        claimedAt = null, executionGeneration = null,
        taskContext = TaskContext("t1", "{}", null, null, null, 0, 3),
    )

    private fun handler(name: String, result: TaskResult = TaskResult.Success("handled")) =
        object : TaskHandler {
            override val handlerName = name
            override suspend fun handle(ctx: TaskContext) = result
        }

    private fun builder(vararg middlewares: HandlerMiddleware): HandlerPipelineBuilder =
        HandlerPipelineBuilder(FakeInstance(middlewares.toList()))

    /** Records before/after invocations into a shared log. */
    private class RecordingMiddleware(
        override val order: Int,
        private val log: MutableList<String>,
    ) : HandlerMiddleware {
        override suspend fun invoke(
            context: TaskExecutionContext,
            next: suspend (TaskExecutionContext) -> TaskResult,
        ): TaskResult {
            log.add("before-$order")
            val result = next(context)
            log.add("after-$order")
            return result
        }
    }

    /** Middleware that short-circuits without calling next(). */
    private class ShortCircuitMiddleware(
        override val order: Int,
        private val result: TaskResult,
    ) : HandlerMiddleware {
        override suspend fun invoke(
            context: TaskExecutionContext,
            next: suspend (TaskExecutionContext) -> TaskResult,
        ): TaskResult = result
    }

    /** Middleware that captures the context it received. */
    private class ContextCapture(
        override val order: Int,
    ) : HandlerMiddleware {
        var captured: TaskExecutionContext? = null

        override suspend fun invoke(
            context: TaskExecutionContext,
            next: suspend (TaskExecutionContext) -> TaskResult,
        ): TaskResult {
            captured = context
            return next(context)
        }
    }

    // ── tests ────────────────────────────────────────────────────────

    @Test
    fun `no middlewares - handler called directly`() = runTest {
        val b = builder()
        val h = handler("direct", TaskResult.Success("direct-result"))

        val chain = b.chainFor(h)
        val result = chain(ctx())

        assertEquals(TaskResult.Success("direct-result"), result)
    }

    @Test
    fun `single middleware wraps handler`() = runTest {
        val log = mutableListOf<String>()
        val b = builder(RecordingMiddleware(10, log))
        val h = handler("wrapped")

        val chain = b.chainFor(h)
        chain(ctx())

        assertEquals(listOf("before-10", "after-10"), log)
    }

    @Test
    fun `multiple middlewares execute in order - lowest order is outermost`() = runTest {
        val log = mutableListOf<String>()
        val b = builder(
            RecordingMiddleware(30, log),
            RecordingMiddleware(10, log),
            RecordingMiddleware(20, log),
        )
        val h = handler("ordered")

        val chain = b.chainFor(h)
        chain(ctx())

        // Outermost (10) enters first, innermost (30) enters last
        assertEquals(
            listOf("before-10", "before-20", "before-30", "after-30", "after-20", "after-10"),
            log,
        )
    }

    @Test
    fun `chain is cached for same handler - computeIfAbsent`() = runTest {
        val log = mutableListOf<String>()
        val b = builder(RecordingMiddleware(10, log))
        val h = handler("cached")

        val chain1 = b.chainFor(h)
        val chain2 = b.chainFor(h)

        // Same function reference — cached
        assert(chain1 === chain2) { "Expected same chain instance from cache" }
    }

    @Test
    fun `middleware can short-circuit without calling next`() = runTest {
        val log = mutableListOf<String>()
        val shortCircuit = TaskResult.Failure("blocked")
        val b = builder(
            RecordingMiddleware(10, log),
            ShortCircuitMiddleware(20, shortCircuit),
            RecordingMiddleware(30, log),
        )
        val h = handler("short-circuit")

        val chain = b.chainFor(h)
        val result = chain(ctx())

        assertEquals(shortCircuit, result)
        // Order 10 wraps 20, 20 short-circuits, 30 and handler are never reached
        assertEquals(listOf("before-10", "after-10"), log)
    }

    @Test
    fun `middleware receives correct context`() = runTest {
        val capture = ContextCapture(10)
        val b = builder(capture)
        val h = handler("ctx-check")

        val context = ctx(handler = "ctx-check")
        val chain = b.chainFor(h)
        chain(context)

        assertEquals(context, capture.captured)
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
