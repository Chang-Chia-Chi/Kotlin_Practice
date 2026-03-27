package com.workflow.worker

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class HandlerRegistryTest {

    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var registry: HandlerRegistry

    @BeforeEach
    fun setup() {
        meterRegistry = SimpleMeterRegistry()
        registry = HandlerRegistry(meterRegistry)
    }

    @Test
    fun `register handler and resolve by key returns metered wrapper`() = runTest {
        val handler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = null)
        }

        registry.register("order.validate", handler)

        val resolved = registry.resolve("order.validate")
        assertIs<MeteredTransitionHandler>(resolved)
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
        val output = resolved.execute(
            HandlerInput(taskId = "t1", workflowId = "wf1", sequenceNumber = 1, payload = null),
        )
        assertEquals("second", output.result)
    }

    @Test
    fun `resolved handler records timer metric on execute`() = runTest {
        val handler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = "done")
        }

        registry.register("order.ship", handler)
        registry.resolve("order.ship").execute(
            HandlerInput(taskId = "t1", workflowId = "wf1", sequenceNumber = 1, payload = null),
        )

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "order.ship")
            .tag("status", "success")
            .timer()
        assertEquals(1, timer?.count())
    }
}
