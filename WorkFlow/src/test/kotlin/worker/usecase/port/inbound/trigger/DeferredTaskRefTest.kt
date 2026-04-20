package com.workflow.worker.usecase.port.inbound.trigger

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class DeferredTaskRefTest {

    @Test
    fun `DeferredTaskRef construction preserves all fields`() {
        val deadline = Instant.parse("2026-06-01T12:00:00Z")
        val ref = DeferredTaskRef(
            taskId = "task-42",
            workflowId = "wf-7",
            sequenceNumber = 3,
            triggerType = "k8s-job",
            triggerMeta = """{"jobName":"batch","namespace":"default"}""",
            deadlineAt = deadline,
            retryCount = 1,
            maxRetries = 5,
        )
        assertEquals("task-42", ref.taskId)
        assertEquals("wf-7", ref.workflowId)
        assertEquals(3, ref.sequenceNumber)
        assertEquals("k8s-job", ref.triggerType)
        assertEquals("""{"jobName":"batch","namespace":"default"}""", ref.triggerMeta)
        assertEquals(deadline, ref.deadlineAt)
        assertEquals(1, ref.retryCount)
        assertEquals(5, ref.maxRetries)
    }

    @Test
    fun `DeferredTaskRef with null deadlineAt`() {
        val ref = DeferredTaskRef(
            taskId = "task-99",
            workflowId = "wf-1",
            sequenceNumber = 0,
            triggerType = "k8s-job",
            triggerMeta = "{}",
            deadlineAt = null,
            retryCount = 0,
            maxRetries = 3,
        )
        assertNull(ref.deadlineAt)
    }
}
