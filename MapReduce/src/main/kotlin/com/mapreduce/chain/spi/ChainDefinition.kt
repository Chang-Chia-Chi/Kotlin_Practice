package com.mapreduce.chain.spi

import com.mapreduce.chain.model.ChainFailurePolicy
import com.mapreduce.chain.model.PayloadTransformStrategy

/**
 * Defines an ordered sequence of task steps that execute one after another.
 *
 * Each step references an existing [com.mapreduce.queue.spi.TaskHandler] by
 * its handler name. The framework wraps invocations in a [com.mapreduce.chain.handler.ChainStepHandler]
 * that captures output and advances the chain — handlers themselves are chain-unaware.
 *
 * Register implementations as CDI beans; [com.mapreduce.chain.registry.ChainRegistrar]
 * discovers them at startup.
 */
interface ChainDefinition {

    /** Unique identifier — e.g. `"etl-pipeline"`, `"document-flow"`. */
    val chainType: String

    /** Ordered list of steps in the chain. */
    val steps: List<StepDefinition>

    /** What to do when a step is dead-lettered. Default: fail the entire chain. */
    val failurePolicy: ChainFailurePolicy get() = ChainFailurePolicy.FAIL_CHAIN
}

/**
 * A single step within a chain definition.
 *
 * @property stepIndex Position in the chain (0-based).
 * @property handler Task handler routing key for this step. Must be registered
 *   in [com.mapreduce.queue.registry.HandlerRegistry].
 * @property queue Queue to enqueue on.
 * @property maxRetries Per-step retry limit before dead-lettering.
 * @property payloadTransform How to derive this step's payload from the previous step's output.
 * @property staticPayload Static payload used when [payloadTransform] is [PayloadTransformStrategy.STATIC].
 */
data class StepDefinition(
    val stepIndex: Int,
    val handler: String,
    val queue: String = "default",
    val maxRetries: Int = 3,
    val payloadTransform: PayloadTransformStrategy = PayloadTransformStrategy.PASS_OUTPUT,
    val staticPayload: String? = null,
)
