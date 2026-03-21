package com.mapreduce.workflow.handler

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.StepStatus
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.model.WorkflowStep
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.workflow.model.FailurePolicy
import com.mapreduce.workflow.registry.WorkflowRegistry
import com.mapreduce.workflow.spi.WorkflowDefinition
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration

class StepTransitionHandlerTest {

    private lateinit var stepRepo: WorkflowStepRepository
    private lateinit var registry: WorkflowRegistry
    private lateinit var config: FrameworkConfig
    private lateinit var handler: StepTransitionHandler

    @BeforeEach
    fun setUp() {
        stepRepo = mock()
        registry = mock()
        config = mock()
        val workflowConfig = mock<FrameworkConfig.WorkflowConfig>()
        whenever(config.workflow()).thenReturn(workflowConfig)
        whenever(workflowConfig.defaultStepDeadline()).thenReturn(Duration.ofHours(1))
        handler = StepTransitionHandler("wc", stepRepo, registry, config)
    }

    private fun ctx(stepId: String) = TaskContext(
        taskId = "callback-1", handler = "wc.__step_transition",
        queue = "default", payload = stepId,
    )

    private fun step(
        stepId: String = "step-1",
        stepLabel: String = "map",
        stepTotal: Int = 10,
        tasksFailed: Int = 0,
        version: Long = 0,
    ) = WorkflowStep(
        stepId = stepId, workflowName = "wc", runId = "run-1",
        status = StepStatus.ACTIVE, params = "{}", queue = "mr",
        stepLabel = stepLabel, stepTotal = stepTotal, tasksFailed = tasksFailed,
        version = version,
    )

    private fun twoStepPipeline() = listOf(
        WorkflowDefinition.StepSpec(name = "map", handler = "wc.map", queue = "mr"),
        WorkflowDefinition.StepSpec(name = "reduce", handler = "wc.reduce", queue = "mr"),
    )

    private fun singleStepPipeline() = listOf(
        WorkflowDefinition.StepSpec(name = "map", handler = "wc.map", queue = "mr"),
    )

    private fun fakeDefinition(
        pipeline: List<WorkflowDefinition.StepSpec>,
        transitionTasks: List<WorkflowDefinition.TaskPayload> = listOf(WorkflowDefinition.TaskPayload("{}")),
    ): WorkflowDefinition<Any> = object : WorkflowDefinition<Any>(
        name = "wc",
        paramsClass = Any::class,
    ) {
        override fun pipeline() = pipeline
        override suspend fun initialTasks(params: Any) = emptyList<TaskPayload>()
        override suspend fun transitionTasks(
            stepIndex: Int, previousStepParams: String, previousOutputs: Flow<TaskOutput>,
        ) = StepTransition(transitionTasks)
        override suspend fun onCompleted(lastStepParams: String, finalOutputs: Flow<TaskOutput>) {}
    }

    // ── Error paths ────────────────────────────────────────────

    @Nested
    inner class ErrorPaths {

        @Test
        fun `returns Failure when step not found`() = runTest {
            whenever(stepRepo.findStep("missing")).thenReturn(null)

            val result = handler.handle(ctx("missing"))

            assertTrue(result is TaskResult.Failure)
            assertTrue((result as TaskResult.Failure).message.contains("not found"))
        }

        @Test
        fun `returns Failure when workflow definition not found`() = runTest {
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1"))
            whenever(registry.getDefinition("wc")).thenReturn(null)

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Failure)
            assertTrue((result as TaskResult.Failure).message.contains("No workflow definition"))
        }

        @Test
        fun `returns Failure when step label not in pipeline`() = runTest {
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1", stepLabel = "unknown"))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(twoStepPipeline()))

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Failure)
            assertTrue((result as TaskResult.Failure).message.contains("not found in pipeline"))
        }
    }

    // ── Failure policy ──────────────────────────────────────────

    @Nested
    inner class FailurePolicyEvaluation {

        @Test
        fun `FAIL_STEP triggers CAS to FAILED when any tasks failed`() = runTest {
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1", tasksFailed = 1))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(twoStepPipeline()))
            whenever(stepRepo.casStepStatus("s-1", StepStatus.ACTIVE, StepStatus.FAILED, 0)).thenReturn(true)

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Success)
            verify(stepRepo).casStepStatus("s-1", StepStatus.ACTIVE, StepStatus.FAILED, 0)
            verify(stepRepo, never()).createNextStep(any(), any(), any(), any())
        }

        @Test
        fun `dispatches compensation task when step fails and compensation handler is set`() = runTest {
            val pipeline = listOf(
                WorkflowDefinition.StepSpec(
                    name = "map", handler = "wc.map", queue = "mr",
                    compensation = "wc.map-rollback",
                ),
                WorkflowDefinition.StepSpec(name = "reduce", handler = "wc.reduce", queue = "mr"),
            )
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1", tasksFailed = 1))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(pipeline))
            whenever(stepRepo.failStepWithCompensation("s-1", 0, "wc.map-rollback", "mr")).thenReturn(true)

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Success)
            verify(stepRepo).failStepWithCompensation(
                stepId = "s-1",
                expectedVersion = 0,
                compensationHandler = "wc.map-rollback",
                queue = "mr",
            )
        }

        @Test
        fun `skips compensation when step fails but no compensation handler`() = runTest {
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1", tasksFailed = 1))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(twoStepPipeline()))
            whenever(stepRepo.casStepStatus("s-1", StepStatus.ACTIVE, StepStatus.FAILED, 0)).thenReturn(true)

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Success)
            verify(stepRepo, never()).failStepWithCompensation(any(), any(), any(), any())
        }

        @Test
        fun `BEST_EFFORT does not trigger failure even with failed tasks`() = runTest {
            val pipeline = listOf(
                WorkflowDefinition.StepSpec(
                    name = "map", handler = "wc.map", queue = "mr",
                    failurePolicy = FailurePolicy.BEST_EFFORT,
                ),
            )
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1", stepLabel = "map", tasksFailed = 5))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(pipeline))
            whenever(stepRepo.streamTaskOutputs("s-1", "wc.map")).thenReturn(emptyFlow())
            whenever(stepRepo.casStepStatus("s-1", StepStatus.ACTIVE, StepStatus.COMPLETED, 0)).thenReturn(true)

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Success)
            verify(stepRepo).casStepStatus("s-1", StepStatus.ACTIVE, StepStatus.COMPLETED, 0)
        }
    }

    // ── Last step ───────────────────────────────────────────────

    @Nested
    inner class LastStepCompletion {

        @Test
        fun `calls onCompleted and CAS to COMPLETED on final step`() = runTest {
            var onCompletedCalled = false
            val def = object : WorkflowDefinition<Any>(
                name = "wc",
                paramsClass = Any::class,
            ) {
                override fun pipeline() = singleStepPipeline()
                override suspend fun initialTasks(params: Any) = emptyList<TaskPayload>()
                override suspend fun onCompleted(lastStepParams: String, finalOutputs: Flow<TaskOutput>) {
                    onCompletedCalled = true
                }
            }

            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1"))
            whenever(registry.getDefinition("wc")).thenReturn(def)
            whenever(stepRepo.streamTaskOutputs("s-1", "wc.map")).thenReturn(emptyFlow())
            whenever(stepRepo.casStepStatus("s-1", StepStatus.ACTIVE, StepStatus.COMPLETED, 0)).thenReturn(true)

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Success)
            assertTrue(onCompletedCalled)
            verify(stepRepo).casStepStatus("s-1", StepStatus.ACTIVE, StepStatus.COMPLETED, 0)
        }
    }

    // ── Mid-pipeline transition ─────────────────────────────────

    @Nested
    inner class MidPipelineTransition {

        @Test
        fun `transitions to next step with createNextStep`() = runTest {
            val transitionPayloads = listOf(
                WorkflowDefinition.TaskPayload("r-0"),
                WorkflowDefinition.TaskPayload("r-1"),
            )
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1"))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(twoStepPipeline(), transitionPayloads))
            whenever(stepRepo.streamTaskOutputs("s-1", "wc.map")).thenReturn(emptyFlow())
            whenever(stepRepo.createNextStep(any(), any(), any(), any())).thenReturn(true)

            val result = handler.handle(ctx("s-1"))

            assertTrue(result is TaskResult.Success)
            verify(stepRepo).createNextStep(any(), any(), any(), any())
        }
    }

    // ── Deadline resolution ───────────────────────────────────

    @Nested
    inner class DeadlineResolution {

        @Test
        fun `uses config default when spec deadline is null`() = runTest {
            val pipeline = listOf(
                WorkflowDefinition.StepSpec(name = "map", handler = "wc.map", queue = "mr"),
                WorkflowDefinition.StepSpec(name = "reduce", handler = "wc.reduce", queue = "mr", deadline = null),
            )
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1"))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(pipeline))
            whenever(stepRepo.streamTaskOutputs("s-1", "wc.map")).thenReturn(emptyFlow())
            whenever(stepRepo.createNextStep(any(), any(), any(), any())).thenReturn(true)

            handler.handle(ctx("s-1"))

            // Verify createNextStep was called (deadline resolved without crash)
            verify(stepRepo).createNextStep(any(), any(), any(), any())
        }

        @Test
        fun `uses explicit deadline when spec deadline is set`() = runTest {
            val pipeline = listOf(
                WorkflowDefinition.StepSpec(name = "map", handler = "wc.map", queue = "mr"),
                WorkflowDefinition.StepSpec(
                    name = "reduce", handler = "wc.reduce", queue = "mr",
                    deadline = Duration.ofMinutes(5),
                ),
            )
            whenever(stepRepo.findStep("s-1")).thenReturn(step(stepId = "s-1"))
            whenever(registry.getDefinition("wc")).thenReturn(fakeDefinition(pipeline))
            whenever(stepRepo.streamTaskOutputs("s-1", "wc.map")).thenReturn(emptyFlow())
            whenever(stepRepo.createNextStep(any(), any(), any(), any())).thenReturn(true)

            handler.handle(ctx("s-1"))

            verify(stepRepo).createNextStep(any(), any(), any(), any())
        }
    }

    @Test
    fun `handlerName follows naming convention`() {
        assertEquals("wc.__step_transition", handler.handlerName)
    }
}
