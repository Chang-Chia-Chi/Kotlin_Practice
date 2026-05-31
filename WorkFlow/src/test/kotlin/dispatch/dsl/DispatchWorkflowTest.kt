package com.workflow.dispatch.dsl

import com.workflow.workflow.model.buildSequenceMap
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DispatchWorkflowTest {

    @Test
    fun `dispatchWorkflow start is scatter`() {
        assertEquals(listOf("scatter"), dispatchWorkflow.starts)
    }

    @Test
    fun `dispatchWorkflow scatter has FanOutDefinition with DispatchSimulationHandler`() {
        val scatter = dispatchWorkflow.activities["scatter"]!!
        assertNotNull(scatter.fanOut)
        assertEquals("DispatchSimulationHandler", scatter.fanOut!!.transition)
        assertEquals(2, scatter.fanOut!!.retries)
    }

    @Test
    fun `dispatchWorkflow scatter successor is join`() {
        val scatter = dispatchWorkflow.activities["scatter"]!!
        assertEquals(1, scatter.successors.size)
        assertEquals("join", scatter.successors[0].target)
    }

    @Test
    fun `dispatchWorkflow no simulate activity exists as named node`() {
        assertFalse(
            "simulate" in dispatchWorkflow.activities,
            "simulate should be embedded in fanOut, not a named activity",
        )
    }

    @Test
    fun `dispatchWorkflow join has no fanOut`() {
        val join = dispatchWorkflow.activities["join"]!!
        assertNull(join.fanOut)
    }

    @Test
    fun `dispatchWorkflow join batchToken resolves from scatter`() {
        val join = dispatchWorkflow.activities["join"]!!
        assertEquals("scatter.batchToken", join.inputs["batchToken"])
    }

    @Test
    fun `dispatchWorkflow builds valid sequence map with three entries`() {
        val seqMap = buildSequenceMap(dispatchWorkflow)
        assertEquals(3, seqMap.size)
    }
}
