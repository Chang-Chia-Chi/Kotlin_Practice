package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dsl.workflow
import com.workflow.worker.FakeDispatchNotifier
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdempotencyKeyTest {

    private lateinit var engine: WorkflowEngine
    private lateinit var workflowRepo: WorkflowRepository
    private lateinit var taskRepo: TaskRepository
    private lateinit var notifier: FakeDispatchNotifier
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private val jdbi = OracleTestContainer.jdbi

    @BeforeAll
    fun setup() {
        workflowRepo = WorkflowRepository(jdbi)
        taskRepo = TaskRepository(jdbi)
        notifier = FakeDispatchNotifier()
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
    }

    @AfterEach
    fun cleanup() {
        jdbi.useHandle<Exception> { h ->
            h.execute("DELETE FROM task")
            h.execute("DELETE FROM workflow")
        }
    }

    private val definition = workflow {
        activity("step1") { transition("test-handler") }
    }

    @Test
    fun `first call with idempotencyKey returns Created`() = runTest {
        val result = engine.startWorkflow(definition, "test-key-1")
        assertIs<StartResult.Created>(result)
    }

    @Test
    fun `second call with same key returns AlreadyExists with same workflowId`() = runTest {
        val first = engine.startWorkflow(definition, "test-key-2")
        val second = engine.startWorkflow(definition, "test-key-2")

        assertIs<StartResult.Created>(first)
        assertIs<StartResult.AlreadyExists>(second)
        assertEquals(first.workflowId, second.workflowId)
    }

    @Test
    fun `different keys create different workflows`() = runTest {
        val r1 = engine.startWorkflow(definition, "key-A")
        val r2 = engine.startWorkflow(definition, "key-B")

        assertIs<StartResult.Created>(r1)
        assertIs<StartResult.Created>(r2)
        assertNotEquals(r1.workflowId, r2.workflowId)
    }

    @Test
    fun `null idempotencyKey always creates`() = runTest {
        val r1 = engine.startWorkflow(definition)
        val r2 = engine.startWorkflow(definition)

        assertIs<StartResult.Created>(r1)
        assertIs<StartResult.Created>(r2)
        assertNotEquals(r1.workflowId, r2.workflowId)
    }
}
