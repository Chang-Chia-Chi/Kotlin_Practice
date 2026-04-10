package com.workflow.worker.usecase.service.execution

import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import com.workflow.worker.usecase.service.execution.MeteredTransitionHandler
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class MeteredTransitionHandlerTest {

    private lateinit var meterRegistry: SimpleMeterRegistry

    private val input = HandlerInput(
        taskId = "t1",
        workflowId = "wf1",
        sequenceNumber = 1,
        inputs = null,
        taskPayload = null,
    )

    @BeforeEach
    fun setup() {
        meterRegistry = SimpleMeterRegistry()
    }

    @Test
    fun `records success timer and returns delegate output`() = runTest {
        val delegate = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput) = HandlerResult(result = "ok")
        }
        val metered = MeteredTransitionHandler(delegate, "order.validate", meterRegistry)

        val output = metered.execute(input)

        assertEquals("ok", output.result)

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "order.validate")
            .tag("status", "success")
            .timer()
        assertNotNull(timer, "success timer should be registered")
        assertEquals(1, timer.count())
    }

    @Test
    fun `records failure timer and rethrows exception`() = runTest {
        val delegate = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                throw IllegalStateException("boom")
            }
        }
        val metered = MeteredTransitionHandler(delegate, "order.validate", meterRegistry)

        assertFailsWith<IllegalStateException>("boom") {
            metered.execute(input)
        }

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "order.validate")
            .tag("status", "failure")
            .timer()
        assertNotNull(timer, "failure timer should be registered")
        assertEquals(1, timer.count())
    }

    @Test
    fun `multiple executions accumulate in timer`() = runTest {
        val delegate = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput) = HandlerResult(result = null)
        }
        val metered = MeteredTransitionHandler(delegate, "step.process", meterRegistry)

        repeat(3) { metered.execute(input) }

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "step.process")
            .tag("status", "success")
            .timer()
        assertNotNull(timer)
        assertEquals(3, timer.count())
    }
}
