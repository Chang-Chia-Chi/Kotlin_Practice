package com.mapreduce.workflow.spi

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.mapreduce.workflow.model.FailurePolicy
import kotlinx.coroutines.flow.Flow
import java.time.Duration
import kotlin.reflect.KClass

abstract class WorkflowDefinition<P : Any>(
    name: String,
    private val paramsClass: KClass<P>,
) {
    val workflowName: String = name

    private val mapper = jacksonObjectMapper()

    open fun serializeParams(params: P): String = mapper.writeValueAsString(params)
    open fun deserializeParams(json: String): P = mapper.readValue(json, paramsClass.java)

    abstract fun pipeline(): List<StepSpec>
    abstract suspend fun initialTasks(params: P): List<TaskPayload>

    open suspend fun transitionTasks(
        stepIndex: Int,
        previousStepParams: String,
        previousOutputs: Flow<TaskOutput>,
    ): StepTransition = StepTransition(emptyList())

    open suspend fun onCompleted(lastStepParams: String, finalOutputs: Flow<TaskOutput>) {}

    protected fun workflow(block: WorkflowBuilder.() -> Unit): List<StepSpec> =
        WorkflowBuilder().apply(block).build()

    class WorkflowBuilder {
        private val steps = mutableListOf<StepSpec>()
        fun step(name: String, block: StepBuilder.() -> Unit) {
            steps += StepBuilder(name).apply(block).build()
        }
        fun build(): List<StepSpec> = steps.toList()
    }

    class StepBuilder(private val name: String) {
        private var handler: String = ""
        private var queue: String = "default"
        private var maxRetries: Int = 3
        private var failurePolicy: FailurePolicy = FailurePolicy.FAIL_STEP
        private var failureThreshold: Double = 0.0
        private var deadline: Duration? = null
        private var compensation: String? = null

        fun handler(h: String) { handler = h }
        fun queue(q: String) { queue = q }
        fun retries(n: Int) { maxRetries = n }
        fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
        fun failureThreshold(t: Double) { failureThreshold = t }
        fun deadline(d: Duration) { deadline = d }
        fun compensation(h: String) { compensation = h }

        fun build() = StepSpec(
            name = name, handler = handler, queue = queue,
            maxRetries = maxRetries, failurePolicy = failurePolicy,
            failureThreshold = failureThreshold, deadline = deadline,
            compensation = compensation,
        )
    }

    data class StepSpec(
        val name: String,
        val handler: String,
        val queue: String = "default",
        val maxRetries: Int = 3,
        val failurePolicy: FailurePolicy = FailurePolicy.FAIL_STEP,
        val failureThreshold: Double = 0.0,
        val deadline: Duration? = null,
        val compensation: String? = null,
    )

    data class TaskPayload(val payload: String, val metadata: String? = null)
    data class StepTransition(val tasks: List<TaskPayload>, val stepParams: String? = null)
    data class TaskOutput(val uri: String, val metadata: String?)
}
