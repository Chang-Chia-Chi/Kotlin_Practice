package com.workflow.engine

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

class WorkflowModelsTest {

    private val now: Instant = Instant.parse("2026-01-15T10:30:00Z")
    private val later: Instant = Instant.parse("2026-01-15T11:00:00Z")

    // ── WorkflowStatus enum ─────────────────────────────────────────────

    @Test
    fun `WorkflowStatus contains exactly four values`() {
        assertEquals(
            setOf("PENDING", "RUNNING", "COMPLETED", "FAILED"),
            WorkflowStatus.entries.map { it.name }.toSet(),
        )
    }

    @Test
    fun `WorkflowStatus valueOf round-trips each entry`() {
        WorkflowStatus.entries.forEach { status ->
            assertEquals(status, WorkflowStatus.valueOf(status.name))
        }
    }

    // ── ActivityStatus enum ─────────────────────────────────────────────

    @Test
    fun `ActivityStatus contains exactly four values`() {
        assertEquals(
            setOf("PENDING", "DISPATCHED", "SUCCEEDED", "FAILED"),
            ActivityStatus.entries.map { it.name }.toSet(),
        )
    }

    @Test
    fun `ActivityStatus valueOf round-trips each entry`() {
        ActivityStatus.entries.forEach { status ->
            assertEquals(status, ActivityStatus.valueOf(status.name))
        }
    }

    // ── TaskStatus enum ─────────────────────────────────────────────────

    @Test
    fun `TaskStatus contains exactly four values`() {
        assertEquals(
            setOf("PENDING", "PROCESSING", "COMPLETED", "FAILED"),
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
    fun `isTerminal returns true only for COMPLETED and FAILED`() {
        val expectedTerminal = setOf(TaskStatus.COMPLETED, TaskStatus.FAILED)
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

    // ── TaskType enum ───────────────────────────────────────────────────

    @Test
    fun `TaskType contains exactly four values`() {
        assertEquals(
            setOf("LINEAR", "SCATTER", "FAN_OUT_SUB", "JOIN_AGGREGATION"),
            TaskType.entries.map { it.name }.toSet(),
        )
    }

    @Test
    fun `TaskType valueOf round-trips each entry`() {
        TaskType.entries.forEach { type ->
            assertEquals(type, TaskType.valueOf(type.name))
        }
    }

    // ── WorkflowRun data class ──────────────────────────────────────────

    private fun workflowRun(
        id: String = "wf-1",
        definitionJson: String = """{"activities":[]}""",
        currentActivityIndex: Int = 0,
        status: WorkflowStatus = WorkflowStatus.PENDING,
        version: Int = 1,
        createdAt: Instant = now,
        updatedAt: Instant = now,
    ) = WorkflowRun(id, definitionJson, currentActivityIndex, status, version, createdAt, updatedAt)

    @Test
    fun `WorkflowRun construction preserves all fields`() {
        val run = workflowRun()
        assertEquals("wf-1", run.id)
        assertEquals("""{"activities":[]}""", run.definitionJson)
        assertEquals(0, run.currentActivityIndex)
        assertEquals(WorkflowStatus.PENDING, run.status)
        assertEquals(1, run.version)
        assertEquals(now, run.createdAt)
        assertEquals(now, run.updatedAt)
    }

    @Test
    fun `WorkflowRun equality for identical instances`() {
        assertEquals(workflowRun(), workflowRun())
    }

    @Test
    fun `WorkflowRun inequality when id differs`() {
        assertNotEquals(workflowRun(id = "wf-1"), workflowRun(id = "wf-2"))
    }

    @Test
    fun `WorkflowRun inequality when status differs`() {
        assertNotEquals(
            workflowRun(status = WorkflowStatus.PENDING),
            workflowRun(status = WorkflowStatus.RUNNING),
        )
    }

    @Test
    fun `WorkflowRun copy changes only specified field`() {
        val original = workflowRun()
        val copied = original.copy(status = WorkflowStatus.COMPLETED, updatedAt = later)
        assertEquals(WorkflowStatus.COMPLETED, copied.status)
        assertEquals(later, copied.updatedAt)
        assertEquals(original.id, copied.id)
        assertEquals(original.version, copied.version)
    }

    @Test
    fun `WorkflowRun toString contains field values`() {
        val run = workflowRun()
        val str = run.toString()
        assertEquals(true, str.contains("wf-1"), "toString should contain id")
        assertEquals(true, str.contains("PENDING"), "toString should contain status")
    }

    @Test
    fun `WorkflowRun hashCode is consistent for equal instances`() {
        assertEquals(workflowRun().hashCode(), workflowRun().hashCode())
    }

    // ── ActivityInstance data class ──────────────────────────────────────

    private fun activityInstance(
        id: String = "act-1",
        workflowRunId: String = "wf-1",
        sequenceNumber: Int = 0,
        definitionJson: String = """{"name":"step-1"}""",
        nextActivityIndex: Int? = 1,
        status: ActivityStatus = ActivityStatus.PENDING,
        version: Int = 1,
        createdAt: Instant = now,
        updatedAt: Instant = now,
    ) = ActivityInstance(id, workflowRunId, sequenceNumber, definitionJson, nextActivityIndex, status, version, createdAt, updatedAt)

    @Test
    fun `ActivityInstance construction preserves all fields`() {
        val act = activityInstance()
        assertEquals("act-1", act.id)
        assertEquals("wf-1", act.workflowRunId)
        assertEquals(0, act.sequenceNumber)
        assertEquals("""{"name":"step-1"}""", act.definitionJson)
        assertEquals(1, act.nextActivityIndex)
        assertEquals(ActivityStatus.PENDING, act.status)
        assertEquals(1, act.version)
        assertEquals(now, act.createdAt)
        assertEquals(now, act.updatedAt)
    }

    @Test
    fun `ActivityInstance with null nextActivityIndex`() {
        val act = activityInstance(nextActivityIndex = null)
        assertNull(act.nextActivityIndex)
    }

    @Test
    fun `ActivityInstance equality for identical instances`() {
        assertEquals(activityInstance(), activityInstance())
    }

    @Test
    fun `ActivityInstance inequality when id differs`() {
        assertNotEquals(activityInstance(id = "act-1"), activityInstance(id = "act-2"))
    }

    @Test
    fun `ActivityInstance copy changes only specified field`() {
        val original = activityInstance()
        val copied = original.copy(status = ActivityStatus.SUCCEEDED, updatedAt = later)
        assertEquals(ActivityStatus.SUCCEEDED, copied.status)
        assertEquals(later, copied.updatedAt)
        assertEquals(original.id, copied.id)
        assertEquals(original.sequenceNumber, copied.sequenceNumber)
    }

    @Test
    fun `ActivityInstance toString contains field values`() {
        val act = activityInstance()
        val str = act.toString()
        assertEquals(true, str.contains("act-1"), "toString should contain id")
        assertEquals(true, str.contains("PENDING"), "toString should contain status")
    }

    @Test
    fun `ActivityInstance hashCode is consistent for equal instances`() {
        assertEquals(activityInstance().hashCode(), activityInstance().hashCode())
    }

    // ── Task data class ─────────────────────────────────────────────────

    private fun task(
        id: String = "task-1",
        activityId: String = "act-1",
        type: TaskType = TaskType.LINEAR,
        transition: String = "process.step1",
        payloadJson: String? = """{"key":"value"}""",
        status: TaskStatus = TaskStatus.PENDING,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = later,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        completedAt: Instant? = null,
        resultJson: String? = null,
        createdAt: Instant = now,
        updatedAt: Instant = now,
    ) = Task(id, activityId, type, transition, payloadJson, status, retryCount, maxRetries, deadlineAt, claimedBy, claimedAt, completedAt, resultJson, createdAt, updatedAt)

    @Test
    fun `Task construction preserves all fields`() {
        val t = task()
        assertEquals("task-1", t.id)
        assertEquals("act-1", t.activityId)
        assertEquals(TaskType.LINEAR, t.type)
        assertEquals("process.step1", t.transition)
        assertEquals("""{"key":"value"}""", t.payloadJson)
        assertEquals(TaskStatus.PENDING, t.status)
        assertEquals(0, t.retryCount)
        assertEquals(3, t.maxRetries)
        assertEquals(later, t.deadlineAt)
        assertNull(t.claimedBy)
        assertNull(t.claimedAt)
        assertNull(t.completedAt)
        assertNull(t.resultJson)
        assertEquals(now, t.createdAt)
        assertEquals(now, t.updatedAt)
    }

    @Test
    fun `Task with all nullable fields null`() {
        val t = task(payloadJson = null, deadlineAt = null, claimedBy = null, claimedAt = null, completedAt = null, resultJson = null)
        assertNull(t.payloadJson)
        assertNull(t.deadlineAt)
        assertNull(t.claimedBy)
        assertNull(t.claimedAt)
        assertNull(t.completedAt)
        assertNull(t.resultJson)
    }

    @Test
    fun `Task with all nullable fields populated`() {
        val t = task(
            payloadJson = """{"data":1}""",
            deadlineAt = later,
            claimedBy = "worker-1",
            claimedAt = now,
            completedAt = later,
            resultJson = """{"result":"ok"}""",
        )
        assertEquals("""{"data":1}""", t.payloadJson)
        assertEquals(later, t.deadlineAt)
        assertEquals("worker-1", t.claimedBy)
        assertEquals(now, t.claimedAt)
        assertEquals(later, t.completedAt)
        assertEquals("""{"result":"ok"}""", t.resultJson)
    }

    @Test
    fun `Task equality for identical instances`() {
        assertEquals(task(), task())
    }

    @Test
    fun `Task inequality when id differs`() {
        assertNotEquals(task(id = "task-1"), task(id = "task-2"))
    }

    @Test
    fun `Task inequality when status differs`() {
        assertNotEquals(
            task(status = TaskStatus.PENDING),
            task(status = TaskStatus.PROCESSING),
        )
    }

    @Test
    fun `Task inequality when type differs`() {
        assertNotEquals(
            task(type = TaskType.LINEAR),
            task(type = TaskType.SCATTER),
        )
    }

    @Test
    fun `Task copy changes only specified field`() {
        val original = task()
        val copied = original.copy(
            status = TaskStatus.COMPLETED,
            completedAt = later,
            resultJson = """{"done":true}""",
        )
        assertEquals(TaskStatus.COMPLETED, copied.status)
        assertEquals(later, copied.completedAt)
        assertEquals("""{"done":true}""", copied.resultJson)
        assertEquals(original.id, copied.id)
        assertEquals(original.type, copied.type)
        assertEquals(original.transition, copied.transition)
    }

    @Test
    fun `Task copy simulates claim`() {
        val original = task()
        val claimed = original.copy(claimedBy = "worker-7", claimedAt = now)
        assertEquals("worker-7", claimed.claimedBy)
        assertEquals(now, claimed.claimedAt)
        assertEquals(original.status, claimed.status)
    }

    @Test
    fun `Task copy simulates retry increment`() {
        val original = task(retryCount = 1, maxRetries = 3)
        val retried = original.copy(retryCount = original.retryCount + 1)
        assertEquals(2, retried.retryCount)
        assertEquals(3, retried.maxRetries)
    }

    @Test
    fun `Task toString contains field values`() {
        val t = task()
        val str = t.toString()
        assertEquals(true, str.contains("task-1"), "toString should contain id")
        assertEquals(true, str.contains("LINEAR"), "toString should contain type")
        assertEquals(true, str.contains("PENDING"), "toString should contain status")
    }

    @Test
    fun `Task hashCode is consistent for equal instances`() {
        assertEquals(task().hashCode(), task().hashCode())
    }

    @Test
    fun `Task with each TaskType variant`() {
        TaskType.entries.forEach { type ->
            val t = task(type = type)
            assertEquals(type, t.type)
        }
    }

    @Test
    fun `Task with each TaskStatus variant`() {
        TaskStatus.entries.forEach { status ->
            val t = task(status = status)
            assertEquals(status, t.status)
        }
    }
}
