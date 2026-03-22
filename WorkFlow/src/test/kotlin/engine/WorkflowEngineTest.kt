package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import com.workflow.dsl.workflow
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkflowEngineTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: WorkflowRepository
    private lateinit var taskRepo: TaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var engine: WorkflowEngine

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = WorkflowRepository(jdbi)
        taskRepo = TaskRepository(jdbi)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
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
        val payload = """{"orderId":"abc-123"}"""

        val runId = engine.startWorkflow(definition, initialPayload = payload)

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
        assertEquals(payload, task.payloadJson)
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
    fun `start fan-out workflow creates scatter task at sequence 1 with fanOut properties`() = runTest {
        val definition = workflow {
            activity("scatter-gather") {
                transition("batch.parallel-worker")
                retries(1)
                deadline(Duration.ofMinutes(15))
                fanOut {
                    transition("batch.scatter")
                    retries(5)
                    failurePolicy(FailurePolicy.BEST_EFFORT)
                    deadline(Duration.ofMinutes(60))
                    joinPolicy(JoinPolicy.Percentage(95))
                }
            }
        }
        val payload = """{"batchId":"batch-001"}"""

        val runId = engine.startWorkflow(definition, initialPayload = payload)

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
        // Scatter task uses fanOut.transition, fanOut.retries, fanOut.deadline
        assertEquals("batch.scatter", scatterTask.handlerKey)
        assertEquals(payload, scatterTask.payloadJson)
        assertEquals(5, scatterTask.maxRetries)
        assertEquals(0, scatterTask.retryCount)
        assertNotNull(scatterTask.deadlineAt)
        assertTrue(scatterTask.deadlineAt!! > Instant.now().plusSeconds(3500), "deadline should be ~60 min from now")
        assertTrue(scatterTask.deadlineAt!! < Instant.now().plusSeconds(3660), "deadline should not be too far in the future")
        assertNull(scatterTask.claimedBy)
        assertNull(scatterTask.claimedAt)
        assertNull(scatterTask.completedAt)
        assertNull(scatterTask.resultJson)

        // No tasks at sequence 2 (parallel phase not yet created)
        val seq2Tasks = taskRepo.findByWorkflowAndSequence(runId, 2)
        assertTrue(seq2Tasks.isEmpty())
    }

    @Test
    fun `start linear workflow with null payload creates task with null payloadJson`() = runTest {
        val definition = workflow {
            activity("init") {
                transition("system.init")
            }
        }

        val runId = engine.startWorkflow(definition)

        val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
        assertEquals(1, tasks.size)
        assertNull(tasks.single().payloadJson)
    }

    @Test
    fun `start workflow returns unique IDs for each invocation`() = runTest {
        val definition = workflow {
            activity("step") {
                transition("do.work")
            }
        }

        val id1 = engine.startWorkflow(definition)
        val id2 = engine.startWorkflow(definition)

        assertTrue(id1.isNotBlank())
        assertTrue(id2.isNotBlank())
        assertTrue(id1 != id2)
    }
}
