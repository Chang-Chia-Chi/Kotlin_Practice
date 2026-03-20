package com.mapreduce.workflow.spi

import com.mapreduce.workflow.model.FailurePolicy
import kotlinx.coroutines.flow.Flow
import java.time.Duration

/**
 * Pipeline definition interface for the workflow engine.
 *
 * Replaces the four-type-param MapReduceDefinition with a single type param
 * [P] (submission params). Subsequent steps work with raw strings — each step
 * handler interprets its own params.
 *
 * @param P Job parameters type (input at submission time)
 */
interface WorkflowDefinition<P> {

    val workflowName: String

    fun serializeParams(params: P): String
    fun deserializeParams(json: String): P

    /** Steps in execution order. */
    fun pipeline(): List<StepSpec>

    /** Produce tasks for step 0 (called at submit time). */
    suspend fun initialTasks(params: P): List<TaskPayload>

    /**
     * Produce tasks for step N when step N-1 completes.
     * Called for steps at index 1..last.
     */
    suspend fun transitionTasks(
        stepIndex: Int,
        previousStepParams: String,
        previousOutputs: Flow<TaskOutput>,
    ): StepTransition

    /** Called when the final step completes. */
    suspend fun onCompleted(lastStepParams: String, finalOutputs: Flow<TaskOutput>)

    data class StepSpec(
        val name: String,
        /** TaskHandler bean name registered in HandlerRegistry (NOT the callback handler). */
        val handler: String,
        val queue: String = "default",
        val maxRetries: Int = 3,
        val failurePolicy: FailurePolicy = FailurePolicy.FAIL_STEP,
        val failureThreshold: Double = 0.0,
        val deadline: Duration = Duration.ofHours(1),
    )

    data class TaskPayload(
        val payload: String,
        val metadata: String? = null,
    )

    data class StepTransition(
        val tasks: List<TaskPayload>,
        val stepParams: String? = null,
    )

    data class TaskOutput(
        val uri: String,
        val metadata: String?,
    )
}
