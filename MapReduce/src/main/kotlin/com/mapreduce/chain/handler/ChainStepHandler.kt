package com.mapreduce.chain.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.chain.model.ChainFailurePolicy
import com.mapreduce.chain.registry.ChainRegistrar
import com.mapreduce.chain.repository.ChainRepository
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.spi.TaskHandler
import io.micrometer.core.instrument.MeterRegistry
import org.jboss.logging.Logger

/**
 * Single auto-generated [TaskHandler] that handles ALL chain step tasks.
 *
 * Registered as `"chain.step"`. Every task in a chain uses this handler name;
 * the actual handler routing key is determined from the chain definition's
 * step list via metadata (`chainType`, `stepIndex`).
 *
 * On success: captures the handler's output, determines the next step,
 * computes the next payload (applying transform strategy), and atomically
 * advances the chain — all within the task completion flow on the worker.
 * No leader orchestration required.
 *
 * On dead-letter (retries exhausted): applies the chain's failure policy
 * (FAIL_CHAIN or SKIP_STEP) before returning the failure to the dispatcher.
 */
class ChainStepHandler(
    private val chainRegistrar: ChainRegistrar,
    private val chainRepository: ChainRepository,
    private val handlerRegistry: HandlerRegistry,
    private val objectMapper: ObjectMapper,
    private val meterRegistry: MeterRegistry,
) : TaskHandler {

    private val log = Logger.getLogger(ChainStepHandler::class.java)

    override val handlerName: String = "chain.step"

    override suspend fun handle(ctx: TaskContext): TaskResult {
        // ── Extract chain metadata ──────────────────────────────────
        val chainId = extractString(ctx.metadata, "chainId")
            ?: return TaskResult.Failure("Chain step task ${ctx.taskId} missing chainId in metadata")
        val chainType = extractString(ctx.metadata, "chainType")
            ?: return TaskResult.Failure("Chain step task ${ctx.taskId} missing chainType in metadata")
        val stepIndex = extractInt(ctx.metadata, "stepIndex")
            ?: return TaskResult.Failure("Chain step task ${ctx.taskId} missing stepIndex in metadata")

        // ── Look up definition and step ─────────────────────────────
        val definition = chainRegistrar.getDefinition(chainType)
            ?: return TaskResult.Failure("Unknown chain type: $chainType")

        val step = definition.steps.find { it.stepIndex == stepIndex }
            ?: return TaskResult.Failure("Invalid step index $stepIndex for chain $chainType")

        // ── Resolve the actual handler ──────────────────────────────
        val actualHandler = handlerRegistry.resolve(step.handler)
            ?: return TaskResult.Failure("No handler '${step.handler}' registered for chain $chainType step $stepIndex")

        // ── Resume a FAILED chain if this is a replayed dead-letter ─
        val chainJob = chainRepository.findById(chainId)
        if (chainJob != null && chainJob.status == com.mapreduce.chain.model.ChainStatus.FAILED) {
            chainRepository.resumeChain(chainId)
            log.infof("Resumed FAILED chain %s from step %d", chainId, stepIndex)
        }

        // ── Execute the actual handler ──────────────────────────────
        val stepStart = System.nanoTime()
        val result: TaskResult
        try {
            result = actualHandler.handle(ctx)
        } catch (e: Exception) {
            log.errorf(e, "Chain %s step %d handler '%s' threw", chainId, stepIndex, step.handler)
            handleDeadLetterIfLast(ctx, chainId, chainType, stepIndex, e.message ?: "Unknown error")
            throw e // Re-throw; TaskDispatcher will handle retry/dead-letter
        }

        return when (result) {
            is TaskResult.Success -> {
                recordStepDuration(chainType, stepIndex, stepStart)
                handleStepSuccess(chainId, chainType, stepIndex, result.output, definition)
                result
            }
            is TaskResult.Retry -> result // Pass through — dispatcher handles retry
            is TaskResult.Failure -> {
                handleDeadLetterIfLast(ctx, chainId, chainType, stepIndex, result.message)
                result // Pass through — dispatcher handles retry/dead-letter
            }
            is TaskResult.DeadLetter -> {
                handleDeadLetterIfLast(ctx, chainId, chainType, stepIndex, result.reason)
                result // Pass through — dispatcher handles dead-letter
            }
        }
    }

    /**
     * Advance the chain after a successful step:
     * - If there's a next step: enqueue it with the computed payload.
     * - If this was the last step: mark the chain as COMPLETED.
     */
    private fun handleStepSuccess(
        chainId: String,
        chainType: String,
        stepIndex: Int,
        output: String?,
        definition: com.mapreduce.chain.spi.ChainDefinition,
    ) {
        val nextStepIndex = stepIndex + 1
        val nextStep = definition.steps.find { it.stepIndex == nextStepIndex }

        if (nextStep != null) {
            // Determine original params for MERGE_WITH_ORIGINAL strategy
            val chainJob = chainRepository.findById(chainId)
            val originalParams = chainJob?.chainParams ?: "{}"
            val nextPayload = chainRepository.computeNextPayload(output, originalParams, nextStep)

            chainRepository.advanceChain(chainId, chainType, stepIndex, output, nextStep, nextPayload)
            log.debugf("Chain %s advanced: step %d → %d", chainId, stepIndex, nextStepIndex)
        } else {
            // Last step — mark chain as COMPLETED
            chainRepository.completeChain(chainId, stepIndex, output)
            meterRegistry.counter("taskqueue.chain.completed", "chain_type", chainType).increment()
            log.infof("Chain %s COMPLETED (type=%s, steps=%d)", chainId, chainType, stepIndex + 1)
        }
    }

    /**
     * If this failure will cause dead-lettering (retries exhausted), apply
     * the chain's failure policy BEFORE the dispatcher dead-letters the task.
     */
    private fun handleDeadLetterIfLast(
        ctx: TaskContext,
        chainId: String,
        chainType: String,
        stepIndex: Int,
        errorMessage: String,
    ) {
        // After dispatcher increments retry_count, it will be ctx.retryCount + 1.
        // Dead-letter occurs when (retryCount + 1) >= maxRetries.
        if (ctx.retryCount + 1 < ctx.maxRetries) return // Will retry — no action needed

        val chainJob = chainRepository.findById(chainId) ?: return
        val definition = chainRegistrar.getDefinition(chainType) ?: return

        when (chainJob.failurePolicy) {
            ChainFailurePolicy.FAIL_CHAIN -> {
                chainRepository.failChain(chainId, "Step $stepIndex failed: $errorMessage")
                meterRegistry.counter(
                    "taskqueue.chain.failed", "chain_type", chainType, "failed_step", stepIndex.toString(),
                ).increment()
                log.warnf("Chain %s FAILED at step %d: %s", chainId, stepIndex, errorMessage)
            }
            ChainFailurePolicy.SKIP_STEP -> {
                val nextStepIndex = stepIndex + 1
                val nextStep = definition.steps.find { it.stepIndex == nextStepIndex }

                if (nextStep != null) {
                    val previousOutput = chainJob.lastStepOutput
                    val originalParams = chainJob.chainParams
                    val nextPayload = chainRepository.computeNextPayload(previousOutput, originalParams, nextStep)

                    chainRepository.skipStepAndAdvance(
                        chainId, chainType, stepIndex, previousOutput, nextStep, nextPayload,
                    )
                    log.warnf("Chain %s skipped failed step %d → advancing to step %d", chainId, stepIndex, nextStepIndex)
                } else {
                    // Failed step was the last — complete chain with previous output
                    chainRepository.skipLastStepAndComplete(chainId, stepIndex, chainJob.lastStepOutput)
                    meterRegistry.counter("taskqueue.chain.completed", "chain_type", chainType).increment()
                    log.warnf("Chain %s skipped failed last step %d → COMPLETED", chainId, stepIndex)
                }
            }
        }
    }

    private fun recordStepDuration(chainType: String, stepIndex: Int, startNanos: Long) {
        val durationNanos = System.nanoTime() - startNanos
        io.micrometer.core.instrument.Timer.builder("taskqueue.chain.step_duration")
            .tag("chain_type", chainType)
            .tag("step_index", stepIndex.toString())
            .register(meterRegistry)
            .record(durationNanos, java.util.concurrent.TimeUnit.NANOSECONDS)
    }

    private fun extractString(metadata: String?, field: String): String? {
        if (metadata == null) return null
        return try {
            objectMapper.readTree(metadata).get(field)?.asText()
        } catch (_: Exception) {
            null
        }
    }

    private fun extractInt(metadata: String?, field: String): Int? {
        if (metadata == null) return null
        return try {
            objectMapper.readTree(metadata).get(field)?.asInt()
        } catch (_: Exception) {
            null
        }
    }
}
