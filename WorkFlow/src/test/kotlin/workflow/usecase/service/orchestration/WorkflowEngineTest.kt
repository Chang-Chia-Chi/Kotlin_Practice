package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.StartResult
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.usecase.service.orchestration.BarrierService
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import com.workflow.workflow.usecase.service.phase.PhaseStrategyRegistry
import com.workflow.worker.usecase.port.outbound.notification.DispatchNotifier
import com.workflow.worker.adapter.http.FakeDispatchNotifier
import com.workflow.infrastructure.persistence.OracleTestContainer
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkflowEngineTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var notifier: FakeDispatchNotifier
    private lateinit var engine: WorkflowEngine
    private lateinit var barrierService: BarrierService

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        notifier = FakeDispatchNotifier()
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
        barrierService = BarrierService(jdbi, workflowRepo, taskRepo, objectMapper, PhaseStrategyRegistry(), notifier)
    }

    @AfterEach
    fun cleanTables() {
        jdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    @Test
    fun `start linear workflow creates RUNNING run with one PENDING task`() = runTest {
        val definition = workflow {
            activity("step1") {
                transition("order.validate")
                retries(3)
                deadline(Duration.ofMinutes(10))
            }
            activity("step2") {
                transition("order.process")
            }
        }
        val runId = engine.startWorkflow(definition).workflowId

        // Verify workflow run
        val run = workflowRepo.findById(runId)
        assertNotNull(run)
        assertEquals(WorkflowStatus.RUNNING, run.status)
        assertEquals(1, run.currentSequence)
        assertEquals(0, run.version)

        // Verify serialized definition round-trips
        val storedDef = objectMapper.readValue<WorkflowDefinition>(run.definitionJson)
        assertEquals(definition, storedDef)

        // Verify exactly one task at sequence 1
        val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
        assertEquals(1, tasks.size)

        val task = tasks.single()
        assertEquals(runId, task.workflowId)
        assertEquals(TaskStatus.PENDING, task.status)
        assertEquals("order.validate", task.handlerKey)
        assertNull(task.item)
        assertEquals(3, task.maxRetries)
        assertEquals(0, task.retryCount)
        assertNotNull(task.deadlineAt)
        assertTrue(task.deadlineAt!! > Instant.now().plusSeconds(500), "deadline should be ~10 min from now")
        assertTrue(task.deadlineAt!! < Instant.now().plusSeconds(660), "deadline should not be too far in the future")
        assertNull(task.claimedBy)
        assertNull(task.claimedAt)
        assertNull(task.completedAt)
        assertNull(task.resultJson)

        // No tasks at sequence 2 yet
        val seq2Tasks = taskRepo.findByWorkflowAndSequence(runId, 2)
        assertTrue(seq2Tasks.isEmpty())
    }

    @Test
    fun `start fan-out workflow creates scatter task at sequence 1 with activity properties`() = runTest {
        val definition = workflow {
            activity("scatter-gather") {
                transition("batch.parallel-worker")
                retries(1)
                deadline(Duration.ofMinutes(15))
                fanOut("parallel")
            }
            activity("parallel") {
                transition("batch.scatter")
                retries(5)
                failurePolicy(FailurePolicy.BEST_EFFORT)
                deadline(Duration.ofMinutes(60))
                joinPolicy(JoinPolicy.Percentage(95))
            }
        }
        val runId = engine.startWorkflow(definition).workflowId

        // Verify workflow run
        val run = workflowRepo.findById(runId)
        assertNotNull(run)
        assertEquals(WorkflowStatus.RUNNING, run.status)
        assertEquals(1, run.currentSequence)
        assertEquals(0, run.version)

        // Verify exactly one scatter task at sequence 1
        val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
        assertEquals(1, tasks.size)

        val scatterTask = tasks.single()
        assertEquals(runId, scatterTask.workflowId)
        assertEquals(TaskStatus.PENDING, scatterTask.status)
        // Scatter task uses activity.transition, activity.retries, activity.deadline
        assertEquals("batch.parallel-worker", scatterTask.handlerKey)
        assertNull(scatterTask.item)
        assertEquals(1, scatterTask.maxRetries)
        assertEquals(0, scatterTask.retryCount)
        assertNotNull(scatterTask.deadlineAt)
        assertTrue(scatterTask.deadlineAt!! > Instant.now().plusSeconds(800), "deadline should be ~15 min from now")
        assertTrue(scatterTask.deadlineAt!! < Instant.now().plusSeconds(960), "deadline should not be too far in the future")
        assertNull(scatterTask.claimedBy)
        assertNull(scatterTask.claimedAt)
        assertNull(scatterTask.completedAt)
        assertNull(scatterTask.resultJson)

        // No tasks at sequence 2 (parallel phase not yet created)
        val seq2Tasks = taskRepo.findByWorkflowAndSequence(runId, 2)
        assertTrue(seq2Tasks.isEmpty())
    }

    @Test
    fun `start linear workflow with null payload creates task with null item`() = runTest {
        val definition = workflow {
            activity("init") {
                transition("system.init")
            }
        }

        val runId = engine.startWorkflow(definition).workflowId

        val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
        assertEquals(1, tasks.size)
        assertNull(tasks.single().item)
    }

    @Test
    fun `start workflow returns unique IDs for each invocation`() = runTest {
        val definition = workflow {
            activity("step") {
                transition("do.work")
            }
        }

        val id1 = engine.startWorkflow(definition).workflowId
        val id2 = engine.startWorkflow(definition).workflowId

        assertTrue(id1.isNotBlank())
        assertTrue(id2.isNotBlank())
        assertTrue(id1 != id2)
    }

    @Test
    fun `startWorkflow sets deadline_at from definition deadline`() = runTest {
        val definition = workflow {
            deadline(Duration.ofMinutes(45))
            activity("step1") {
                transition("order.validate")
            }
        }
        val before = Instant.now()
        val runId = engine.startWorkflow(definition).workflowId
        val after = Instant.now()

        val run = workflowRepo.findById(runId)
        assertNotNull(run)
        assertTrue(run.deadlineAt.isAfter(before.plus(Duration.ofMinutes(44))))
        assertTrue(run.deadlineAt.isBefore(after.plus(Duration.ofMinutes(46))))
    }

    @Test
    fun `cancelWorkflow transitions RUNNING to CANCELLED and cancels pending tasks`() = runTest {
        val definition = workflow {
            activity("step1") { transition("handler1") }
            activity("step2") { transition("handler2") }
        }
        val runId = engine.startWorkflow(definition).workflowId

        val result = engine.cancelWorkflow(runId)
        assertTrue(result)

        val run = workflowRepo.findById(runId)
        assertNotNull(run)
        assertEquals(WorkflowStatus.CANCELLED, run.status)

        val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
        assertTrue(tasks.all { it.status == TaskStatus.CANCELLED })
    }

    @Test
    fun `cancelWorkflow returns false for non-RUNNING workflow`() = runTest {
        val definition = workflow {
            activity("step1") { transition("handler1") }
        }
        val runId = engine.startWorkflow(definition).workflowId

        // Cancel first (moves to CANCELLED)
        engine.cancelWorkflow(runId)
        // Try again
        val result = engine.cancelWorkflow(runId)
        assertFalse(result)
    }

    @Test
    fun `cancelWorkflow returns false for nonexistent workflow`() = runTest {
        val result = engine.cancelWorkflow("nonexistent-id")
        assertFalse(result)
    }

    // ── DispatchNotifier signal tests ────────────────────────────────────

    @Test
    fun `startWorkflow signals notifier with first activity queue`() = runTest {
        val signalCountBefore = notifier.signalCount
        val definition = workflow {
            activity("step1") {
                transition("order.validate")
            }
        }

        engine.startWorkflow(definition).workflowId

        assertEquals(signalCountBefore + 1, notifier.signalCount, "signal() should be called once after startWorkflow")
        assertTrue(notifier.signalledQueues.last() == "default", "Signal should use the first activity's queue name (default)")
    }

    @Test
    fun `startWorkflow signals notifier with custom queue`() = runTest {
        val signalCountBefore = notifier.signalCount
        val definition = workflow {
            activity("step1") {
                transition("order.validate")
                queue("priority")
            }
        }

        engine.startWorkflow(definition).workflowId

        assertEquals(signalCountBefore + 1, notifier.signalCount)
        assertTrue(notifier.signalledQueues.last() == "priority", "Signal should use custom queue name 'priority'")
    }

    @Test
    fun `replayWorkflow signals notifier on success`() = runTest {
        val definition = workflow {
            activity("step1") { transition("handler1") }
        }
        val runId = engine.startWorkflow(definition).workflowId

        // Move to FAILED so we can replay
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE workflow SET status = 'FAILED' WHERE id = :id")
                .bind("id", runId)
                .execute()
            handle.createUpdate("UPDATE task SET status = 'DEAD_LETTER' WHERE workflow_id = :wfId")
                .bind("wfId", runId)
                .execute()
        }

        val signalCountBefore = notifier.signalCount
        val result = engine.replayWorkflow(runId)

        assertTrue(result)
        assertTrue(notifier.signalCount > signalCountBefore, "signal() should be called after successful replayWorkflow")
    }

    @Test
    fun `replayWorkflow does not signal on non-FAILED workflow`() = runTest {
        val definition = workflow {
            activity("step1") { transition("handler1") }
        }
        val runId = engine.startWorkflow(definition).workflowId

        // Workflow is RUNNING, not FAILED — replay should return false
        val signalCountBefore = notifier.signalCount
        val result = engine.replayWorkflow(runId)

        assertFalse(result)
        assertEquals(signalCountBefore, notifier.signalCount, "signal() should NOT be called on failed replay")
    }
}
