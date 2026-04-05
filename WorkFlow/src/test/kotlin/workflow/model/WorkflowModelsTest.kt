package com.workflow.workflow.model

import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.JoinPolicy
import java.time.Duration
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
    fun `TaskStatus contains exactly ten values`() {
        assertEquals(
            setOf("PENDING", "PROCESSING", "WAITING_FOR_SIGNAL", "DEFERRED", "COMPLETED", "FAILED",
                  "TIMED_OUT", "DEAD_LETTER", "CANCELLED", "SKIPPED"),
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
            TaskStatus.DEAD_LETTER, TaskStatus.CANCELLED, TaskStatus.SKIPPED,
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
    fun `WAITING_FOR_SIGNAL is not terminal`() {
        assertEquals(false, TaskStatus.WAITING_FOR_SIGNAL.isTerminal)
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
    fun `SKIPPED is terminal`() {
        assertEquals(true, TaskStatus.SKIPPED.isTerminal)
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
            TaskStatus.PROCESSING to TaskStatus.WAITING_FOR_SIGNAL,
            TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.COMPLETED,
            TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.FAILED,
            TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.TIMED_OUT,
            TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.CANCELLED,
            TaskStatus.FAILED to TaskStatus.PENDING,
            TaskStatus.FAILED to TaskStatus.DEAD_LETTER,
            TaskStatus.PROCESSING to TaskStatus.DEFERRED,
            TaskStatus.DEFERRED to TaskStatus.COMPLETED,
            TaskStatus.DEFERRED to TaskStatus.FAILED,
            TaskStatus.DEFERRED to TaskStatus.TIMED_OUT,
            TaskStatus.DEFERRED to TaskStatus.CANCELLED,
            TaskStatus.DEFERRED to TaskStatus.PENDING,
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

    @Test
    fun `DEFERRED status is non-terminal`() {
        assertEquals(false, TaskStatus.DEFERRED.isTerminal)
    }

    @Test
    fun `DEFERRED allows transitions from PROCESSING and to terminal states`() {
        val deferredTransitions = listOf(
            TaskStatus.PROCESSING to TaskStatus.DEFERRED,
            TaskStatus.DEFERRED to TaskStatus.COMPLETED,
            TaskStatus.DEFERRED to TaskStatus.FAILED,
            TaskStatus.DEFERRED to TaskStatus.TIMED_OUT,
            TaskStatus.DEFERRED to TaskStatus.CANCELLED,
            TaskStatus.DEFERRED to TaskStatus.PENDING,
        )
        deferredTransitions.forEach { (from, to) ->
            org.junit.jupiter.api.assertDoesNotThrow {
                TaskStatus.requireTransition(from, to)
            }
        }
    }

    @Test
    fun `DEFERRED rejects illegal transitions`() {
        val illegal = listOf(
            TaskStatus.DEFERRED to TaskStatus.PROCESSING,
            TaskStatus.DEFERRED to TaskStatus.DEAD_LETTER,
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
        definitionJson: String = """{"activities":{}}""",
        version: Int = 0,
        status: WorkflowStatus = WorkflowStatus.RUNNING,
        createdAt: Instant = now,
        updatedAt: Instant = now,
        deadlineAt: Instant = later,
    ) = WorkflowRun(id, definitionJson, version, status, createdAt, updatedAt, deadlineAt)

    @Test
    fun `WorkflowRun construction preserves all fields`() {
        val run = workflowRun()
        assertEquals("wf-1", run.id)
        assertEquals("""{"activities":{}}""", run.definitionJson)
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
        activityName: String = "step1",
        sequenceNumber: Int = 1,
        status: TaskStatus = TaskStatus.PENDING,
        handlerKey: String = "process.step1",
        item: String? = null,
        resultJson: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        completedAt: Instant? = null,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = later,
    ) = Task(
        id = id, workflowId = workflowId, activityName = activityName,
        sequenceNumber = sequenceNumber, status = status, handlerKey = handlerKey,
        item = item, resultJson = resultJson, claimedBy = claimedBy,
        claimedAt = claimedAt, completedAt = completedAt,
        retryCount = retryCount, maxRetries = maxRetries, deadlineAt = deadlineAt,
    )

    @Test
    fun `Task construction preserves all fields`() {
        val t = task(item = """{"key":"value"}""")
        assertEquals("task-1", t.id)
        assertEquals("wf-1", t.workflowId)
        assertEquals("step1", t.activityName)
        assertEquals(1, t.sequenceNumber)
        assertEquals(TaskStatus.PENDING, t.status)
        assertEquals("process.step1", t.handlerKey)
        assertEquals("""{"key":"value"}""", t.item)
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
            item = null,
            resultJson = null,
            claimedBy = null,
            claimedAt = null,
            completedAt = null,
            deadlineAt = null,
        )
        assertNull(t.item)
        assertNull(t.resultJson)
        assertNull(t.claimedBy)
        assertNull(t.claimedAt)
        assertNull(t.completedAt)
        assertNull(t.deadlineAt)
    }

    @Test
    fun `Task with all nullable fields populated`() {
        val t = task(
            item = """{"data":1}""",
            resultJson = """{"result":"ok"}""",
            claimedBy = "worker-1",
            claimedAt = now,
            completedAt = later,
            deadlineAt = later,
        )
        assertEquals("""{"data":1}""", t.item)
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

    @Test
    fun `Task with trigger fields preserves values`() {
        val t = Task(
            id = "task-defer-1",
            workflowId = "wf-1",
            activityName = "step1",
            sequenceNumber = 1,
            status = TaskStatus.DEFERRED,
            handlerKey = "process.step1",
            item = null,
            resultJson = null,
            claimedBy = null,
            claimedAt = null,
            completedAt = null,
            retryCount = 0,
            maxRetries = 3,
            deadlineAt = later,
            triggerType = "k8s-job",
            triggerMeta = "{}",
        )
        assertEquals("k8s-job", t.triggerType)
        assertEquals("{}", t.triggerMeta)
    }

    // ── Edge ────────────────────────────────────────────────────────────

    @Test
    fun `Edge defaults to DEFAULT_BRANCH label`() {
        val edge = Edge("fulfill")
        assertEquals("fulfill", edge.target)
        assertEquals(DEFAULT_BRANCH, edge.label)
    }

    @Test
    fun `Edge with explicit label preserves label`() {
        val edge = Edge("reject", "FAILED")
        assertEquals("reject", edge.target)
        assertEquals("FAILED", edge.label)
    }

    @Test
    fun `DEFAULT_BRANCH constant value is double-underscore default double-underscore`() {
        assertEquals("__default__", DEFAULT_BRANCH)
    }

    @Test
    fun `Edge with blank target throws IllegalArgumentException`() {
        val ex = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
            Edge("")
        }
        assertTrue(ex.message!!.contains("Edge target must not be blank"))
    }

    @Test
    fun `Edge with whitespace-only target throws IllegalArgumentException`() {
        val ex = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
            Edge("   ")
        }
        assertTrue(ex.message!!.contains("Edge target must not be blank"))
    }

    // ── FanOutDefinition ─────────────────────────────────────────────────

    @Test
    fun `FanOutDefinition defaults match spec`() {
        val fanOut = FanOutDefinition(transition = "MyHandler")
        assertEquals("MyHandler", fanOut.transition)
        assertEquals(0, fanOut.retries)
        assertEquals(FailurePolicy.ABORT, fanOut.failurePolicy)
        assertEquals(Duration.ofMinutes(30), fanOut.deadline)
        assertEquals(JoinPolicy.All, fanOut.joinPolicy)
        assertEquals(Duration.ofSeconds(1), fanOut.backoffBase)
        assertEquals(Duration.ofSeconds(300), fanOut.backoffCap)
        assertEquals("default", fanOut.queue)
    }

    @Test
    fun `FanOutDefinition preserves overridden fields`() {
        val fanOut = FanOutDefinition(
            transition = "Handler",
            retries = 3,
            failurePolicy = FailurePolicy.BEST_EFFORT,
            deadline = Duration.ofMinutes(5),
            joinPolicy = JoinPolicy.Percentage(80),
            backoffBase = Duration.ofSeconds(2),
            backoffCap = Duration.ofSeconds(60),
            queue = "priority",
        )
        assertEquals(3, fanOut.retries)
        assertEquals(FailurePolicy.BEST_EFFORT, fanOut.failurePolicy)
        assertEquals(Duration.ofMinutes(5), fanOut.deadline)
        assertEquals(JoinPolicy.Percentage(80), fanOut.joinPolicy)
        assertEquals(Duration.ofSeconds(2), fanOut.backoffBase)
        assertEquals(Duration.ofSeconds(60), fanOut.backoffCap)
        assertEquals("priority", fanOut.queue)
    }
}
