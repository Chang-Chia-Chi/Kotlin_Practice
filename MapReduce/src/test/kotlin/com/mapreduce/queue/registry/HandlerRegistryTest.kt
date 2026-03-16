package com.mapreduce.queue.registry

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.inject.Instance
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

class HandlerRegistryTest {

    private lateinit var registry: HandlerRegistry

    @BeforeEach
    fun setUp() {
        @Suppress("UNCHECKED_CAST")
        val emptyInstance = mock(Instance::class.java) as Instance<TaskHandler>
        registry = HandlerRegistry(emptyInstance)
    }

    @Test
    fun `register and resolve handler by name`() {
        val handler = stubHandler("email.send")

        registry.register(handler)

        assertSame(handler, registry.resolve("email.send"))
    }

    @Test
    fun `duplicate handler name keeps first registration`() {
        val first = stubHandler("email.send")
        val second = stubHandler("email.send")

        registry.register(first)
        registry.register(second)

        assertSame(first, registry.resolve("email.send"))
    }

    @Test
    fun `resolve unknown handler returns null`() {
        assertNull(registry.resolve("nonexistent.handler"))
    }

    @Test
    fun `registeredHandlers returns all registered names`() {
        registry.register(stubHandler("handler.a"))
        registry.register(stubHandler("handler.b"))
        registry.register(stubHandler("handler.c"))

        assertEquals(setOf("handler.a", "handler.b", "handler.c"), registry.registeredHandlers())
    }

    @Test
    fun `register multiple handlers with distinct names`() {
        val handlers = listOf(
            stubHandler("map.task"),
            stubHandler("reduce.task"),
            stubHandler("shuffle.task"),
        )

        handlers.forEach { registry.register(it) }

        handlers.forEach { h ->
            assertSame(h, registry.resolve(h.handlerName))
        }
        assertEquals(3, registry.registeredHandlers().size)
    }

    @Test
    fun `registeredHandlers is empty initially`() {
        assertTrue(registry.registeredHandlers().isEmpty())
    }

    private fun stubHandler(name: String): TaskHandler = object : TaskHandler {
        override val handlerName: String = name
        override suspend fun handle(ctx: TaskContext): TaskResult = TaskResult.Success()
    }
}
