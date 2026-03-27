package com.workflow.engine

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WorkflowModelsTest {

    private val now: Instant = Instant.parse("2026-01-15T10:30:00Z")
    private val later: Instant = Instant.parse("2026-01-15T11:00:00Z")

    // ── WorkflowStatus enum ─────────────────────────────────────────────

    @Test
    fun `WorkflowStatus contains exactly five values`() {
        assertEquals(
            setOf("RUNNING", "COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"),
            WorkflowStatus.entries.map { it.name }.toSet(),
        )
    }

    @Test
    fun `WorkflowStatus valueOf round-trips each entry`() {
        WorkflowStatus.entries.forEach { status ->
            assertEquals(status, WorkflowStatus.valueOf(status.name))
        }
    }

    @Test
    fun `WorkflowStatus isTerminal returns true for all except RUNNING`() {
        assertEquals(false, WorkflowStatus.RUNNING.isTerminal)
        WorkflowStatus.entries.filter { it != WorkflowStatus.RUNNING }.forEach {
            assertEquals(true, it.isTerminal, "Expected isTerminal=true for $it")
        }
    }

    @Test
    fun `WorkflowStatus allows all legal transitions from RUNNING`() {
        listOf(
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.TIMED_OUT,
            WorkflowStatus.CANCELLED,
        ).forEach { target ->
            WorkflowStatus.requireTransition(WorkflowStatus.RUNNING, target)
        }
    }

    @Test
    fun `WorkflowStatus allows future reclaim transitions`() {
        listOf(
            WorkflowStatus.FAILED,
            WorkflowStatus.TIMED_OUT,
            WorkflowStatus.CANCELLED,
        ).forEach { source ->
            WorkflowStatus.requireTransition(source, WorkflowStatus.RUNNING)
        }
    }

    @Test
    fun `WorkflowStatus rejects illegal transitions`() {
        val illegal = listOf(
            WorkflowStatus.COMPLETED to WorkflowStatus.RUNNING,
            WorkflowStatus.COMPLETED to WorkflowStatus.FAILED,
            WorkflowStatus.FAILED to WorkflowStatus.COMPLETED,
            WorkflowStatus.RUNNING to WorkflowStatus.RUNNING,
        )
        illegal.forEach { (from, to) ->
            val ex = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
                WorkflowStatus.requireTransition(from, to)
            }
            assertTrue(ex.message!!.contains("Illegal workflow transition"))
        }
    }

    // ── TaskStatus enum ─────────────────────────────────────────────────

    @Test
    fun `TaskStatus contains exactly seven values`() {
        assertEquals(
            setOf("PENDING", "PROCESSING", "COMPLETED", "FAILED", "TIMED_OUT", "DEAD_LETTER", "CANCELLED"),
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
    fun `isTerminal returns true only for terminal statuses`() {
        val expectedTerminal = setOf(
            TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMED_OUT,
            TaskStatus.DEAD_LETTER, TaskStatus.CANCELLED,
        )
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
    fun `TaskStatus allows all legal transitions`() {
        val legal = listOf(
            TaskStatus.PENDING to TaskStatus.PROCESSING,
            TaskStatus.PENDING to TaskStatus.CANCELLED,
            TaskStatus.PROCESSING to TaskStatus.COMPLETED,
            TaskStatus.PROCESSING to TaskStatus.FAILED,
            TaskStatus.PROCESSING to TaskStatus.TIMED_OUT,
            TaskStatus.PROCESSING to TaskStatus.PENDING,
            TaskStatus.PROCESSING to TaskStatus.DEAD_LETTER,
            TaskStatus.FAILED to TaskStatus.PENDING,
            TaskStatus.FAILED to TaskStatus.DEAD_LETTER,
        )
        legal.forEach { (from, to) ->
            TaskStatus.requireTransition(from, to)
        }
    }

    @Test
    fun `TaskStatus rejects illegal transitions`() {
        val illegal = listOf(
            TaskStatus.PENDING to TaskStatus.COMPLETED,
            TaskStatus.PENDING to TaskStatus.FAILED,
            TaskStatus.COMPLETED to TaskStatus.PENDING,
            TaskStatus.CANCELLED to TaskStatus.PENDING,
            TaskStatus.DEAD_LETTER to TaskStatus.PROCESSING,
        )
        illegal.forEach { (from, to) ->
            val ex = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
                TaskStatus.requireTransition(from, to)
            }
            assertTrue(ex.message!!.contains("Illegal task transition"))
        }
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
        deadlineAt: Instant = later,
    ) = WorkflowRun(id, definitionJson, currentSequence, version, status, createdAt, updatedAt, deadlineAt)

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
        assertEquals(later, run.deadlineAt)
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
