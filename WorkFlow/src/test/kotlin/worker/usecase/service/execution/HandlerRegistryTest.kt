package com.workflow.worker.usecase.service.execution

import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import com.workflow.worker.usecase.service.execution.HandlerRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class HandlerRegistryTest {

    private lateinit var registry: HandlerRegistry

    @BeforeEach
    fun setup() {
        val emptyBeans = mock<Instance<TransitionHandler>>()
        whenever(emptyBeans.iterator()).thenReturn(mutableListOf<TransitionHandler>().iterator())
        registry = HandlerRegistry(emptyBeans)
    }

    @Test
    fun `register handler and resolve by key returns same handler`() = runTest {
        val handler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = null)
        }

        registry.register("order.validate", handler)

        val resolved = registry.resolve("order.validate")
        assertSame(handler, resolved)
    }

    @Test
    fun `resolve unknown key throws IllegalStateException with key in message`() = runTest {
        val ex = assertFailsWith<IllegalStateException> {
            registry.resolve("nonexistent.key")
        }
        assertTrue(
            ex.message!!.contains("nonexistent.key"),
            "Exception message should contain the missing key, was: ${ex.message}",
        )
    }

    @Test
    fun `register second handler with same key overwrites first`() = runTest {
        val firstHandler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = "first")
        }
        val secondHandler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = "second")
        }

        registry.register("step.one", firstHandler)
        registry.register("step.one", secondHandler)

        val resolved = registry.resolve("step.one")
        assertSame(secondHandler, resolved)

        val output = resolved.execute(
            HandlerInput(taskId = "t1", workflowId = "wf1", sequenceNumber = 1, inputs = null, item = null),
        )
        assertEquals("second", output.result)
    }
}
