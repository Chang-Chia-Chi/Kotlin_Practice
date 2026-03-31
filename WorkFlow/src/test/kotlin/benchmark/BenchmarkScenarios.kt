package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.dsl.workflow
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.service.execution.HandlerRegistry
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import kotlinx.coroutines.delay

object BenchmarkScenarios {

    fun singleActivityDefinition(): WorkflowDefinition = workflow {
        activity("process") { transition("bench.single.process") }
    }

    fun fanOutDefinition(fanOutFactor: Int): WorkflowDefinition = workflow {
        activity("scatter") {
            transition("bench.fanout.scatter")
            fanOut("parallel")
        }
        activity("parallel") {
            transition("bench.fanout.parallel")
            joinPolicy(JoinPolicy.All)
        }
        activity("join") {
            transition("bench.fanout.join")
            inputs {
                "results" from "parallel.result"
            }
        }
    }

    fun multiStepDefinition(stepCount: Int): WorkflowDefinition = workflow {
        for (i in 1..stepCount) {
            activity("step-$i") { transition("bench.multistep.step") }
        }
    }

    fun registerHandlers(
        registry: HandlerRegistry,
        objectMapper: ObjectMapper,
        point: MatrixPoint,
    ) {
        val baseHandler = payloadHandler(point.payloadSizeBytes)
        val handler = if (point.handlerLatencyMs > 0) {
            latencyHandler(point.handlerLatencyMs.toLong(), baseHandler)
        } else {
            baseHandler
        }

        when (point.scenarioName) {
            "single" -> {
                registry.register("bench.single.process", handler)
            }
            "fanout" -> {
                registry.register("bench.fanout.scatter", scatterHandler(point.fanOutFactor, objectMapper))
                registry.register("bench.fanout.parallel", handler)
                registry.register("bench.fanout.join", handler)
            }
            "multistep" -> {
                registry.register("bench.multistep.step", handler)
            }
        }
    }

    fun definitionFor(point: MatrixPoint): WorkflowDefinition = when (point.scenarioName) {
        "single" -> singleActivityDefinition()
        "fanout" -> fanOutDefinition(point.fanOutFactor)
        "multistep" -> multiStepDefinition(point.stepCount)
        else -> throw IllegalArgumentException("Unknown scenario: ${point.scenarioName}")
    }

    private fun payloadHandler(sizeBytes: Int): TransitionHandler = object : TransitionHandler {
        private val payload = """{"data":"${"x".repeat((sizeBytes - 10).coerceAtLeast(0))}"}"""
        override suspend fun execute(input: HandlerInput): HandlerOutput =
            HandlerOutput(result = payload)
    }

    private fun latencyHandler(delayMs: Long, delegate: TransitionHandler): TransitionHandler =
        object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                delay(delayMs)
                return delegate.execute(input)
            }
        }

    private fun scatterHandler(fanOutFactor: Int, objectMapper: ObjectMapper): TransitionHandler =
        object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val items = (1..fanOutFactor).map { mapOf("item" to it) }
                return HandlerOutput(result = objectMapper.writeValueAsString(items))
            }
        }
}
