package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.model.DispatchCategory
import com.workflow.workflow.model.StartResult
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DispatchSchedulerTest {
    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    private fun newScheduler(engine: WorkflowEngine): DispatchScheduler =
        DispatchScheduler(engine, objectMapper)

    @Test
    fun `triggerUrgent emits URGENT-scoped idempotency key and payload`() = runTest {
        val engine = mock<WorkflowEngine>()
        whenever(engine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        newScheduler(engine).triggerUrgent()

        val defCaptor = argumentCaptor<WorkflowDefinition>()
        val keyCaptor = argumentCaptor<String>()
        val itemCaptor = argumentCaptor<String>()
        verify(engine).startWorkflow(defCaptor.capture(), keyCaptor.capture(), itemCaptor.capture())

        assertEquals(dispatchWorkflow, defCaptor.firstValue)
        assertTrue(
            keyCaptor.firstValue.startsWith("dispatch-URGENT-"),
            "expected idempotency key to start with 'dispatch-URGENT-', got ${keyCaptor.firstValue}",
        )
        val parsed = objectMapper.readTree(itemCaptor.firstValue)
        val categories = parsed["categories"].map { it.asText() }
        assertEquals(listOf("URGENT"), categories)
    }

    @Test
    fun `multi-category key is lexicographically sorted`() = runTest {
        val engine = mock<WorkflowEngine>()
        whenever(engine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))
        val scheduler = newScheduler(engine)

        scheduler.triggerForTest(setOf(DispatchCategory.URGENT, DispatchCategory.NORMAL))

        val keyCaptor = argumentCaptor<String>()
        verify(engine).startWorkflow(any(), keyCaptor.capture(), any())
        assertTrue(
            keyCaptor.firstValue.startsWith("dispatch-NORMAL-URGENT-"),
            "expected 'dispatch-NORMAL-URGENT-' (sorted), got ${keyCaptor.firstValue}",
        )
    }

    @Test
    fun `empty set produces ALL-scoped idempotency key`() = runTest {
        val engine = mock<WorkflowEngine>()
        whenever(engine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        newScheduler(engine).triggerForTest(emptySet())

        val keyCaptor = argumentCaptor<String>()
        verify(engine).startWorkflow(any(), keyCaptor.capture(), any())
        assertTrue(
            keyCaptor.firstValue.startsWith("dispatch-ALL-"),
            "expected 'dispatch-ALL-', got ${keyCaptor.firstValue}",
        )
    }

    @Test
    fun `key is identical regardless of set insertion order`() = runTest {
        val engine = mock<WorkflowEngine>()
        whenever(engine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))
        val s1 = newScheduler(engine)
        s1.triggerForTest(linkedSetOf(DispatchCategory.URGENT, DispatchCategory.NORMAL))
        s1.triggerForTest(linkedSetOf(DispatchCategory.NORMAL, DispatchCategory.URGENT))
        val keyCaptor = argumentCaptor<String>()
        verify(engine, times(2)).startWorkflow(any(), keyCaptor.capture(), any())
        // Both keys differ only in the token's seconds at the tail; strip the trailing token and compare the sorted-category prefix portion.
        val prefix1 = keyCaptor.firstValue.substringBeforeLast("-")
        val prefix2 = keyCaptor.secondValue.substringBeforeLast("-")
        assertEquals(prefix1, prefix2, "insertion order must not affect the key prefix")
        assertTrue(prefix1.endsWith("NORMAL-URGENT"))
    }
}
