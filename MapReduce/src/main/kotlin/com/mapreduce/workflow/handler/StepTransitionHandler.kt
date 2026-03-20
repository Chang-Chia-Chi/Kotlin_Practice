package com.mapreduce.workflow.handler

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.StepStatus.ACTIVE
import com.mapreduce.queue.model.StepStatus.COMPLETED
import com.mapreduce.queue.model.StepStatus.FAILED
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.model.WorkflowStep
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.queue.spi.TaskHandler
import com.mapreduce.workflow.model.FailurePolicy
import com.mapreduce.workflow.model.evaluateFailurePolicy
import com.mapreduce.workflow.registry.WorkflowRegistry
import com.mapreduce.workflow.spi.WorkflowDefinition
import kotlinx.coroutines.flow.map
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Generic step barrier callback handler.
 * Registered as `"{workflowName}.__step_transition"` by [WorkflowRegistry].
 *
 * When a step's countdown barrier fires, the framework creates a callback task
 * with payload = step_id. This handler picks it up and drives the pipeline:
 *
 * 1. Fetch the completed step row.
 * 2. Look up the WorkflowDefinition and find the current step index.
 * 3. Evaluate failure policy from the in-memory StepSpec.
 * 4. If more steps remain: call transitionTasks, INSERT next step row.
 * 5. If this was the last step: call onCompleted, CAS to COMPLETED.
 */
class StepTransitionHandler(
    private val workflowName: String,
    private val workflowStepRepository: WorkflowStepRepository,
    private val workflowRegistry: WorkflowRegistry,
    private val config: FrameworkConfig,
) : TaskHandler {

    private val log = Logger.getLogger(StepTransitionHandler::class.java)

    override val handlerName: String = "$workflowName.__step_transition"

    @Suppress("UNCHECKED_CAST")
    override suspend fun handle(ctx: TaskContext): TaskResult {
        val stepId = ctx.payload
        val step = workflowStepRepository.findStep(stepId)
            ?: return TaskResult.Failure("Step $stepId not found")

        val definition = workflowRegistry.getDefinition(step.workflowName)
            ?: return TaskResult.Failure("No workflow definition for '${step.workflowName}'")

        val pipeline = definition.pipeline()
        val currentIndex = pipeline.indexOfFirst { it.name == step.stepLabel }
        if (currentIndex < 0) {
            return TaskResult.Failure("Step label '${step.stepLabel}' not found in pipeline for '${step.workflowName}'")
        }

        val currentSpec = pipeline[currentIndex]

        // Evaluate failure policy from in-memory StepSpec (not from step row)
        val failureReason = evaluateFailurePolicy(
            currentSpec.failurePolicy,
            step.tasksFailed,
            step.stepTotal,
            currentSpec.failureThreshold,
        )
        if (failureReason != null) {
            val transitioned = workflowStepRepository.casStepStatus(step.stepId, ACTIVE, FAILED, step.version)
            if (transitioned) {
                log.warnf("Step %s failed during '%s': %s", step.stepId, step.stepLabel, failureReason)
            }
            return TaskResult.Success()
        }

        val isLastStep = currentIndex == pipeline.lastIndex

        if (isLastStep) {
            // Final step: call onCompleted, then CAS to COMPLETED
            val finalOutputs = workflowStepRepository.streamTaskOutputs(stepId, currentSpec.handler)
                .map { WorkflowDefinition.TaskOutput(it.uri, it.metadata) }
            (definition as WorkflowDefinition<Any>).onCompleted(step.params ?: "", finalOutputs)

            val transitioned = workflowStepRepository.casStepStatus(step.stepId, ACTIVE, COMPLETED, step.version)
            if (transitioned) {
                log.infof("Workflow run %s completed (step '%s')", step.runId, step.stepLabel)
            }
        } else {
            // More steps: build transition tasks for the next step
            val nextIndex = currentIndex + 1
            val nextSpec = pipeline[nextIndex]

            val previousOutputs = workflowStepRepository.streamTaskOutputs(stepId, currentSpec.handler)
                .map { WorkflowDefinition.TaskOutput(it.uri, it.metadata) }

            val transition = (definition as WorkflowDefinition<Any>).transitionTasks(
                nextIndex, step.params ?: "", previousOutputs,
            )

            val newStepId = UUID.randomUUID().toString()
            val resolvedDeadline = resolveDeadline(nextSpec.deadline)

            val newStep = WorkflowStep(
                stepId = newStepId,
                workflowName = step.workflowName,
                runId = step.runId,
                status = ACTIVE,
                params = transition.stepParams,
                queue = nextSpec.queue,
                stepLabel = nextSpec.name,
                stepTotal = transition.tasks.size,
                onCompleteHandler = "$workflowName.__step_transition",
                failurePolicy = nextSpec.failurePolicy.name,
                failureThreshold = nextSpec.failureThreshold,
                deadlineAt = Instant.now().plus(resolvedDeadline),
            )

            val tasks = transition.tasks.map { payload ->
                EnqueueRequest(
                    handler = nextSpec.handler,
                    payload = payload.payload,
                    queue = nextSpec.queue,
                    stepId = newStepId,
                    metadata = payload.metadata,
                    maxRetries = nextSpec.maxRetries,
                )
            }

            val transitioned = workflowStepRepository.createNextStep(
                previousStepId = step.stepId,
                expectedVersion = step.version,
                newStep = newStep,
                tasks = tasks,
            )
            if (transitioned) {
                log.infof(
                    "Workflow run %s transitioned to step '%s' (%d tasks)",
                    step.runId, nextSpec.name, transition.tasks.size,
                )
            }
        }

        return TaskResult.Success()
    }

    private fun resolveDeadline(specDeadline: Duration): Duration {
        val hardcodedDefault = Duration.ofHours(1)
        return if (specDeadline == hardcodedDefault) {
            config.workflow().defaultStepDeadline()
        } else {
            specDeadline
        }
    }
}
