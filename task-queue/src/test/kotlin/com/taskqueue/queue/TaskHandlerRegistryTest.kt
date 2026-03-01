package com.taskqueue.queue

import io.mockk.every
import io.mockk.mockk
import jakarta.enterprise.inject.Instance
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TaskHandlerRegistryTest {

    private fun handler(type: String): TaskHandler {
        val h = mockk<TaskHandler>()
        every { h.taskType } returns type
        return h
    }

    private fun instanceOf(vararg handlers: TaskHandler): Instance<TaskHandler> {
        val instance = mockk<Instance<TaskHandler>>()
        every { instance.iterator() } returns handlers.toMutableList().iterator()
        return instance
    }

    @Test
    fun `getHandler returns correct handler for known type`() {
        val h1 = handler("TYPE_A")
        val h2 = handler("TYPE_B")
        val registry = TaskHandlerRegistry(instanceOf(h1, h2))

        assertThat(registry.getHandler("TYPE_A")).isSameAs(h1)
        assertThat(registry.getHandler("TYPE_B")).isSameAs(h2)
    }

    @Test
    fun `getHandler returns null for unknown type`() {
        val registry = TaskHandlerRegistry(instanceOf(handler("KNOWN")))

        assertThat(registry.getHandler("UNKNOWN")).isNull()
    }

    @Test
    fun `registeredTypes returns all registered types`() {
        val registry = TaskHandlerRegistry(instanceOf(handler("A"), handler("B"), handler("C")))

        assertThat(registry.registeredTypes()).containsExactlyInAnyOrder("A", "B", "C")
    }

    @Test
    fun `duplicate taskType throws IllegalStateException`() {
        assertThatThrownBy {
            TaskHandlerRegistry(instanceOf(handler("DUP"), handler("DUP")))
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("Duplicate TaskHandler for taskType='DUP'")
    }

    @Test
    fun `empty registry has no handlers`() {
        val registry = TaskHandlerRegistry(instanceOf())

        assertThat(registry.registeredTypes()).isEmpty()
        assertThat(registry.getHandler("ANY")).isNull()
    }
}
