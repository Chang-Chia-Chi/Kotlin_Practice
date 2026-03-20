package com.mapreduce.workflow.api

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.StepStatus
import com.mapreduce.queue.model.WorkflowStep
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.workflow.api.dto.SubmitJobRequest
import com.mapreduce.workflow.model.FailurePolicy
import com.mapreduce.workflow.registry.WorkflowRegistry
import com.mapreduce.workflow.spi.WorkflowDefinition
import jakarta.ws.rs.core.Response
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

class JobResourceTest {

    private lateinit var workflowStepRepository: WorkflowStepRepository
    private lateinit var registry: WorkflowRegistry
    private lateinit var config: FrameworkConfig
    private lateinit var resource: JobResource

    @BeforeEach
    fun setUp() {
        workflowStepRepository = mock()
        registry = mock()
        config = mock()
        val workflowConfig = mock<FrameworkConfig.WorkflowConfig>()
        whenever(config.workflow()).thenReturn(workflowConfig)
        whenever(workflowConfig.defaultStepDeadline()).thenReturn(Duration.ofHours(1))
        resource = JobResource(workflowStepRepository, registry, config)
    }

    private fun mockDefinition(
        taskPayloads: List<WorkflowDefinition.TaskPayload> = listOf(
            WorkflowDefinition.TaskPayload("input1"),
            WorkflowDefinition.TaskPayload("input2"),
        ),
    ): WorkflowDefinition<Any> {
        val def = mock<WorkflowDefinition<Any>>()
        whenever(def.workflowName).thenReturn("wc")
        whenever(def.deserializeParams(any())).thenReturn("params")

        val stepSpec = WorkflowDefinition.StepSpec(
            name = "map",
            handler = "wc.map",
            queue = "mr",
            maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_STEP,
            failureThreshold = 0.0,
        )
        whenever(def.pipeline()).thenReturn(listOf(stepSpec))

        val obj = object : suspend (Any) -> List<WorkflowDefinition.TaskPayload> {
            override suspend fun invoke(p1: Any) = taskPayloads
        }
        // Use a real implementation for initialTasks
        return object : WorkflowDefinition<Any> {
            override val workflowName = "wc"
            override fun serializeParams(params: Any) = "{}"
            override fun deserializeParams(json: String) = "params" as Any
            override fun pipeline() = listOf(stepSpec)
            override suspend fun initialTasks(params: Any) = taskPayloads
            override suspend fun transitionTasks(
                stepIndex: Int, previousStepParams: String, previousOutputs: Flow<WorkflowDefinition.TaskOutput>,
            ) = WorkflowDefinition.StepTransition(emptyList())
            override suspend fun onCompleted(lastStepParams: String, finalOutputs: Flow<WorkflowDefinition.TaskOutput>) {}
        }
    }

    // ── submitJob ──────────────────────────────────────────────────

    @Nested
    inner class SubmitJob {

        @Test
        fun `returns 400 for unknown workflow type`() = runTest {
            whenever(registry.getDefinition("unknown")).thenReturn(null)

            val response = resource.submitJob(SubmitJobRequest("unknown", "{}"))

            assertEquals(Response.Status.BAD_REQUEST.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertTrue(entity["error"].toString().contains("Unknown workflow"))
        }

        @Test
        fun `returns 400 when initialTasks produces zero tasks`() = runTest {
            val def = mockDefinition(taskPayloads = emptyList())
            whenever(registry.getDefinition("wc")).thenReturn(def)

            val response = resource.submitJob(SubmitJobRequest("wc", """{"text":"hi"}"""))

            assertEquals(Response.Status.BAD_REQUEST.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertTrue(entity["error"].toString().contains("zero tasks"))
        }

        @Test
        fun `returns 201 with runId and task count`() = runTest {
            val def = mockDefinition(taskPayloads = listOf(
                WorkflowDefinition.TaskPayload("a"),
                WorkflowDefinition.TaskPayload("b"),
                WorkflowDefinition.TaskPayload("c"),
            ))
            whenever(registry.getDefinition("wc")).thenReturn(def)

            val response = resource.submitJob(SubmitJobRequest("wc", """{"text":"hello"}"""))

            assertEquals(Response.Status.CREATED.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertNotNull(entity["runId"])
            assertNotNull(entity["stepId"])
            assertEquals(3, entity["totalTasks"])
        }

        @Test
        fun `returns 201 with single task`() = runTest {
            val def = mockDefinition(taskPayloads = listOf(WorkflowDefinition.TaskPayload("only-one")))
            whenever(registry.getDefinition("single")).thenReturn(def)

            val response = resource.submitJob(SubmitJobRequest("single", "{}"))

            assertEquals(Response.Status.CREATED.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertEquals(1, entity["totalTasks"])
        }
    }

    // ── getJob ─────────────────────────────────────────────────────

    @Nested
    inner class GetJob {

        @Test
        fun `returns 404 when no steps found for runId`() = runTest {
            whenever(workflowStepRepository.findStepsByRunId("non-existent")).thenReturn(emptyList())

            val response = resource.getJob("non-existent")

            assertEquals(Response.Status.NOT_FOUND.statusCode, response.status)
        }

        @Test
        fun `returns 200 with step list when steps found`() = runTest {
            val step = WorkflowStep(
                stepId = "step-1",
                workflowName = "wordcount",
                runId = "run-1",
                status = StepStatus.ACTIVE,
                stepLabel = "map",
                stepTotal = 5,
                tasksPending = 3,
                tasksFailed = 1,
                failurePolicy = "FAIL_STEP",
                resultMetadata = null,
                createdAt = Instant.now(),
                updatedAt = Instant.now(),
            )
            whenever(workflowStepRepository.findStepsByRunId("run-1")).thenReturn(listOf(step))

            val response = resource.getJob("run-1")

            assertEquals(Response.Status.OK.statusCode, response.status)
        }
    }

    // ── listJobs ───────────────────────────────────────────────────

    @Nested
    inner class ListJobs {

        @Test
        fun `returns 200 with all steps when no status filter`() = runTest {
            val steps = listOf(
                WorkflowStep("s-1", "wc", "r-1", StepStatus.ACTIVE, stepLabel = "map"),
                WorkflowStep("s-2", "wc", "r-2", StepStatus.COMPLETED, stepLabel = "reduce"),
            )
            whenever(workflowStepRepository.findAllSteps(any())).thenReturn(steps)

            val response = resource.listJobs(null)

            assertEquals(Response.Status.OK.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val list = response.entity as List<*>
            assertEquals(2, list.size)
        }

        @Test
        fun `returns 200 with filtered steps when valid status provided`() = runTest {
            val steps = listOf(
                WorkflowStep("s-1", "wc", "r-1", StepStatus.ACTIVE, stepLabel = "map"),
            )
            whenever(workflowStepRepository.findStepsByStatus(StepStatus.ACTIVE)).thenReturn(steps)

            val response = resource.listJobs("ACTIVE")

            assertEquals(Response.Status.OK.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val list = response.entity as List<*>
            assertEquals(1, list.size)
        }

        @Test
        fun `returns 200 with case-insensitive status`() = runTest {
            whenever(workflowStepRepository.findStepsByStatus(StepStatus.COMPLETED)).thenReturn(emptyList())

            val response = resource.listJobs("completed")

            assertEquals(Response.Status.OK.statusCode, response.status)
        }

        @Test
        fun `returns 400 for invalid status value`() = runTest {
            val response = resource.listJobs("INVALID_STATUS")

            assertEquals(Response.Status.BAD_REQUEST.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertTrue(entity["error"].toString().contains("Invalid status"))
        }

        @Test
        fun `returns empty list when no steps exist`() = runTest {
            whenever(workflowStepRepository.findAllSteps(any())).thenReturn(emptyList())

            val response = resource.listJobs(null)

            assertEquals(Response.Status.OK.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val list = response.entity as List<*>
            assertTrue(list.isEmpty())
        }
    }
}
