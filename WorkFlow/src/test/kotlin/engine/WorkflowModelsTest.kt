package com.workflow.engine

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class WorkflowModelsTest {

    private val now: Instant = Instant.parse("2026-01-15T10:30:00Z")
    private val later: Instant = Instant.parse("2026-01-15T11:00:00Z")

    // ── WorkflowStatus enum ─────────────────────────────────────────────

    @Test
    fun `WorkflowStatus contains exactly three values`() {
        assertEquals(
            setOf("RUNNING", "COMPLETED", "FAILED"),
            WorkflowStatus.entries.map { it.name }.toSet(),
        )
    }

    @Test
    fun `WorkflowStatus valueOf round-trips each entry`() {
        WorkflowStatus.entries.forEach { status ->
            assertEquals(status, WorkflowStatus.valueOf(status.name))
        }
    }

    // ── TaskStatus enum ─────────────────────────────────────────────────

    @Test
    fun `TaskStatus contains exactly five values`() {
        assertEquals(
            setOf("PENDING", "PROCESSING", "COMPLETED", "FAILED", "DEAD_LETTER"),
            TaskStatus.entries.map { it.name }.toSet(),
        )
    }

    @Test
    fun `TaskStatus valueOf round-trips each entry`() {
        TaskStatus.entries.forEach { status ->
            assertEquals(status, TaskStatus.valueOf(status.name))
        }
    }

    @Test
    fun `isTerminal returns true only for COMPLETED, FAILED, and DEAD_LETTER`() {
        val expectedTerminal = setOf(TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.DEAD_LETTER)
        TaskStatus.entries.forEach { status ->
            assertEquals(
                status in expectedTerminal,
                status.isTerminal,
                "Expected isTerminal=${status in expectedTerminal} for $status",
            )
        }
    }

    @Test
    fun `PENDING is not terminal`() {
        assertEquals(false, TaskStatus.PENDING.isTerminal)
    }

    @Test
    fun `PROCESSING is not terminal`() {
        assertEquals(false, TaskStatus.PROCESSING.isTerminal)
    }

    @Test
    fun `COMPLETED is terminal`() {
        assertEquals(true, TaskStatus.COMPLETED.isTerminal)
    }

    @Test
    fun `FAILED is terminal`() {
        assertEquals(true, TaskStatus.FAILED.isTerminal)
    }

    @Test
    fun `DEAD_LETTER is terminal`() {
        assertEquals(true, TaskStatus.DEAD_LETTER.isTerminal)
    }

    // ── WorkflowRun data class ──────────────────────────────────────────

    private fun workflowRun(
        id: String = "wf-1",
        definitionJson: String = """{"activities":[]}""",
        currentSequence: Int = 1,
        version: Int = 0,
        status: WorkflowStatus = WorkflowStatus.RUNNING,
        createdAt: Instant = now,
        updatedAt: Instant = now,
    ) = WorkflowRun(id, definitionJson, currentSequence, version, status, createdAt, updatedAt)

    @Test
    fun `WorkflowRun construction preserves all fields`() {
        val run = workflowRun()
        assertEquals("wf-1", run.id)
        assertEquals("""{"activities":[]}""", run.definitionJson)
        assertEquals(1, run.currentSequence)
        assertEquals(0, run.version)
        assertEquals(WorkflowStatus.RUNNING, run.status)
        assertEquals(now, run.createdAt)
        assertEquals(now, run.updatedAt)
    }

    @Test
    fun `WorkflowRun with each status variant`() {
        WorkflowStatus.entries.forEach { status ->
            val run = workflowRun(status = status)
            assertEquals(status, run.status)
        }
    }

    // ── Task data class ─────────────────────────────────────────────────

    private fun task(
        id: String = "task-1",
        workflowId: String = "wf-1",
        sequenceNumber: Int = 1,
        status: TaskStatus = TaskStatus.PENDING,
        handlerKey: String = "process.step1",
        payloadJson: String? = """{"key":"value"}""",
        resultJson: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        completedAt: Instant? = null,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = later,
    ) = Task(
        id, workflowId, sequenceNumber, status, handlerKey,
        payloadJson, resultJson, claimedBy, claimedAt, completedAt,
        retryCount, maxRetries, deadlineAt,
    )

    @Test
    fun `Task construction preserves all fields`() {
        val t = task()
        assertEquals("task-1", t.id)
        assertEquals("wf-1", t.workflowId)
        assertEquals(1, t.sequenceNumber)
        assertEquals(TaskStatus.PENDING, t.status)
        assertEquals("process.step1", t.handlerKey)
        assertEquals("""{"key":"value"}""", t.payloadJson)
        assertNull(t.resultJson)
        assertNull(t.claimedBy)
        assertNull(t.claimedAt)
        assertNull(t.completedAt)
        assertEquals(0, t.retryCount)
        assertEquals(3, t.maxRetries)
        assertEquals(later, t.deadlineAt)
    }

    @Test
    fun `Task with all nullable fields null`() {
        val t = task(
            payloadJson = null,
            resultJson = null,
            claimedBy = null,
            claimedAt = null,
            completedAt = null,
            deadlineAt = null,
        )
        assertNull(t.payloadJson)
        assertNull(t.resultJson)
        assertNull(t.claimedBy)
        assertNull(t.claimedAt)
        assertNull(t.completedAt)
        assertNull(t.deadlineAt)
    }

    @Test
    fun `Task with all nullable fields populated`() {
        val t = task(
            payloadJson = """{"data":1}""",
            resultJson = """{"result":"ok"}""",
            claimedBy = "worker-1",
            claimedAt = now,
            completedAt = later,
            deadlineAt = later,
        )
        assertEquals("""{"data":1}""", t.payloadJson)
        assertEquals("""{"result":"ok"}""", t.resultJson)
        assertEquals("worker-1", t.claimedBy)
        assertEquals(now, t.claimedAt)
        assertEquals(later, t.completedAt)
        assertEquals(later, t.deadlineAt)
    }

    @Test
    fun `Task with each TaskStatus variant`() {
        TaskStatus.entries.forEach { status ->
            val t = task(status = status)
            assertEquals(status, t.status)
        }
    }
}
