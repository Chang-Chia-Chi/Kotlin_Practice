package com.mapreduce.queue.registry

import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.inject.Instance
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class HandlerRegistryTest {

    private lateinit var registry: HandlerRegistry

    @BeforeEach
    fun setup() {
        registry = HandlerRegistry(FakeInstance(emptyList()))
    }

    // ── helpers ─────────────────────────────────────────────────

    private fun handler(name: String) = object : TaskHandler {
        override val handlerName = name
        override suspend fun handle(ctx: TaskContext): TaskResult = TaskResult.Success()
    }

    // ── tests ───────────────────────────────────────────────────

    @Test
    fun `register and resolve returns handler`() {
        val h = handler("email.send")
        registry.register(h)
        assertEquals(h, registry.resolve("email.send"))
    }

    @Test
    fun `resolve unknown handler returns null`() {
        assertNull(registry.resolve("nonexistent"))
    }

    @Test
    fun `duplicate registration keeps first`() {
        val first = handler("dup")
        val second = handler("dup")
        registry.register(first)
        registry.register(second)
        assertEquals(first, registry.resolve("dup"))
    }

    @Test
    fun `multiple handlers registered independently`() {
        val a = handler("handler.a")
        val b = handler("handler.b")
        val c = handler("handler.c")
        registry.register(a)
        registry.register(b)
        registry.register(c)

        assertEquals(a, registry.resolve("handler.a"))
        assertEquals(b, registry.resolve("handler.b"))
        assertEquals(c, registry.resolve("handler.c"))
    }

    @Test
    fun `registeredHandlers returns all registered names`() {
        registry.register(handler("x"))
        registry.register(handler("y"))
        registry.register(handler("z"))
        assertEquals(setOf("x", "y", "z"), registry.registeredHandlers())
    }

    @Test
    fun `registeredHandlers is empty initially`() {
        assertTrue(registry.registeredHandlers().isEmpty())
    }

    @Test
    fun `CDI discovery via onStart populates registry`() {
        val cdiHandler = handler("cdi.discovered")
        val registryWithCdi = HandlerRegistry(FakeInstance(listOf(cdiHandler)))

        registryWithCdi.onStart(StartupEvent())

        assertEquals(cdiHandler, registryWithCdi.resolve("cdi.discovered"))
        assertEquals(setOf("cdi.discovered"), registryWithCdi.registeredHandlers())
    }

    @Test
    fun `programmatic register after CDI discovery works`() {
        val cdiHandler = handler("cdi.handler")
        val registryWithCdi = HandlerRegistry(FakeInstance(listOf(cdiHandler)))

        registryWithCdi.onStart(StartupEvent())

        val manual = handler("manual.handler")
        registryWithCdi.register(manual)

        assertEquals(cdiHandler, registryWithCdi.resolve("cdi.handler"))
        assertEquals(manual, registryWithCdi.resolve("manual.handler"))
        assertEquals(setOf("cdi.handler", "manual.handler"), registryWithCdi.registeredHandlers())
    }

    @Test
    fun `CDI discovery with multiple handlers`() {
        val handlers = listOf(handler("a"), handler("b"), handler("c"))
        val registryWithCdi = HandlerRegistry(FakeInstance(handlers))

        registryWithCdi.onStart(StartupEvent())

        assertEquals(setOf("a", "b", "c"), registryWithCdi.registeredHandlers())
    }

    // ── Fake CDI Instance ───────────────────────────────────────

    @Suppress("UNCHECKED_CAST")
    private class FakeInstance<T>(
        private val items: List<T>,
    ) : Instance<T> {
        override fun iterator(): MutableIterator<T> = items.toMutableList().iterator()

        override fun get(): T = items.first()

        override fun isAmbiguous(): Boolean = false

        override fun isUnsatisfied(): Boolean = items.isEmpty()

        override fun isResolvable(): Boolean = items.isNotEmpty()

        override fun destroy(instance: T & Any) {}

        override fun select(vararg qualifiers: Annotation): Instance<T> = this

        override fun <U : T> select(
            subtype: Class<U>,
            vararg qualifiers: Annotation,
        ): Instance<U> = this as Instance<U>

        override fun <U : T> select(
            subtype: jakarta.enterprise.util.TypeLiteral<U>,
            vararg qualifiers: Annotation,
        ): Instance<U> = this as Instance<U>

        override fun getHandle(): jakarta.enterprise.inject.Instance.Handle<T> = throw UnsupportedOperationException()

        override fun handles(): MutableIterable<jakarta.enterprise.inject.Instance.Handle<T>> = throw UnsupportedOperationException()
    }
}
