package com.mapreduce.chain.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.chain.model.ChainFailurePolicy
import com.mapreduce.chain.model.ChainJob
import com.mapreduce.chain.model.ChainStatus
import com.mapreduce.chain.model.PayloadTransformStrategy
import com.mapreduce.chain.spi.StepDefinition
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.util.UUID

/**
 * Layer 2 persistence — chained tasks specific.
 *
 * Unlike MR/DAG repositories, this does NOT extend [com.mapreduce.leader.FencedRepository]
 * because chain advancement is handler-driven (runs on the worker), not leader-driven.
 */
@ApplicationScoped
class ChainRepository(
    private val jdbi: Jdbi,
    private val objectMapper: ObjectMapper,
) {

    /**
     * Atomic chain start: create `chain_job` row + enqueue first step's task.
     * Both in one transaction.
     *
     * Called from the REST endpoint (any pod, no leader required).
     */
    fun startChain(
        chainId: String,
        chainType: String,
        chainParams: String,
        totalSteps: Int,
        failurePolicy: ChainFailurePolicy,
        firstStep: StepDefinition,
    ) {
        jdbi.useTransaction<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO chain_job (chain_id, chain_type, status, current_step, total_steps,
                    chain_params, failure_policy, version, created_at, updated_at)
                VALUES (:chainId, :chainType, 'RUNNING', 0, :totalSteps,
                    :chainParams, :failurePolicy, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            )
                .bind("chainId", chainId)
                .bind("chainType", chainType)
                .bind("totalSteps", totalSteps)
                .bind("chainParams", chainParams)
                .bind("failurePolicy", failurePolicy.name)
                .execute()

            val metadata = objectMapper.writeValueAsString(
                mapOf("chainId" to chainId, "chainType" to chainType, "stepIndex" to 0),
            )

            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, 'chain.step', :queue, :payload, 'PENDING', 0,
                    :groupId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
            )
                .bind("taskId", UUID.randomUUID().toString())
                .bind("queue", firstStep.queue)
                .bind("payload", chainParams)
                .bind("groupId", chainId)
                .bind("metadata", metadata)
                .bind("maxRetries", firstStep.maxRetries)
                .execute()
        }
    }

    /**
     * Advance the chain after a successful step completion.
     *
     * Atomically: update `chain_job` (increment current_step, store output) +
     * enqueue next step's task. Both in one transaction.
     *
     * Runs on the worker that completed the current step — no leader required.
     *
     * @param output The current step's output (from [com.mapreduce.queue.model.TaskResult.Success.output]).
     * @param nextStep The next step definition from the chain.
     * @param nextPayload Pre-computed payload for the next step.
     */
    fun advanceChain(
        chainId: String,
        chainType: String,
        completedStepIndex: Int,
        output: String?,
        nextStep: StepDefinition,
        nextPayload: String,
    ) {
        jdbi.useTransaction<Exception> { h ->
            val updated = h.createUpdate(
                """
                UPDATE chain_job
                SET current_step = :nextStep, last_step_output = :output,
                    version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE chain_id = :chainId AND status = 'RUNNING'
                  AND current_step = :currentStep
                """,
            )
                .bind("chainId", chainId)
                .bind("nextStep", completedStepIndex + 1)
                .bind("currentStep", completedStepIndex)
                .bind("output", output)
                .execute()

            if (updated == 0) return@useTransaction // Already advanced (idempotent)

            val metadata = objectMapper.writeValueAsString(
                mapOf(
                    "chainId" to chainId,
                    "chainType" to chainType,
                    "stepIndex" to nextStep.stepIndex,
                ),
            )

            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, 'chain.step', :queue, :payload, 'PENDING', 0,
                    :groupId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
            )
                .bind("taskId", UUID.randomUUID().toString())
                .bind("queue", nextStep.queue)
                .bind("payload", nextPayload)
                .bind("groupId", chainId)
                .bind("metadata", metadata)
                .bind("maxRetries", nextStep.maxRetries)
                .execute()
        }
    }

    /**
     * Mark the chain as COMPLETED after the last step succeeds.
     * Stores the final step's output as `last_step_output`.
     */
    fun completeChain(chainId: String, completedStepIndex: Int, output: String?) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE chain_job
                SET status = 'COMPLETED', last_step_output = :output,
                    current_step = :step, version = version + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE chain_id = :chainId AND status = 'RUNNING'
                  AND current_step = :step
                """,
            )
                .bind("chainId", chainId)
                .bind("step", completedStepIndex)
                .bind("output", output)
                .execute()
        }
    }

    /**
     * Mark the chain as FAILED with an error message.
     * Called when a step is dead-lettered and the failure policy is FAIL_CHAIN.
     */
    fun failChain(chainId: String, errorMessage: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE chain_job
                SET status = 'FAILED', error_message = :error,
                    version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE chain_id = :chainId AND status = 'RUNNING'
                """,
            )
                .bind("chainId", chainId)
                .bind("error", errorMessage.take(4000))
                .execute()
        }
    }

    /**
     * Resume a FAILED chain by transitioning it back to RUNNING.
     * Called when a dead-lettered step task is replayed.
     */
    fun resumeChain(chainId: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE chain_job
                SET status = 'RUNNING', error_message = NULL,
                    version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE chain_id = :chainId AND status = 'FAILED'
                """,
            )
                .bind("chainId", chainId)
                .execute()
        }
    }

    /**
     * Skip a failed step and advance to the next.
     * Used when failure policy is SKIP_STEP. Passes the previous step's
     * output (before the failed step) to the next step.
     */
    fun skipStepAndAdvance(
        chainId: String,
        chainType: String,
        failedStepIndex: Int,
        previousOutput: String?,
        nextStep: StepDefinition,
        nextPayload: String,
    ) {
        jdbi.useTransaction<Exception> { h ->
            val updated = h.createUpdate(
                """
                UPDATE chain_job
                SET current_step = :nextStep,
                    version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE chain_id = :chainId AND status = 'RUNNING'
                  AND current_step = :currentStep
                """,
            )
                .bind("chainId", chainId)
                .bind("nextStep", failedStepIndex + 1)
                .bind("currentStep", failedStepIndex)
                .execute()

            if (updated == 0) return@useTransaction

            val metadata = objectMapper.writeValueAsString(
                mapOf(
                    "chainId" to chainId,
                    "chainType" to chainType,
                    "stepIndex" to nextStep.stepIndex,
                ),
            )

            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, 'chain.step', :queue, :payload, 'PENDING', 0,
                    :groupId, :metadata, 0, :maxRetries, CURRENT_TIMESTAMP)
                """,
            )
                .bind("taskId", UUID.randomUUID().toString())
                .bind("queue", nextStep.queue)
                .bind("payload", nextPayload)
                .bind("groupId", chainId)
                .bind("metadata", metadata)
                .bind("maxRetries", nextStep.maxRetries)
                .execute()
        }
    }

    /**
     * Skip a failed step when it's the last step — complete the chain.
     */
    fun skipLastStepAndComplete(chainId: String, failedStepIndex: Int, previousOutput: String?) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE chain_job
                SET status = 'COMPLETED', current_step = :step, last_step_output = :output,
                    version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE chain_id = :chainId AND status = 'RUNNING'
                  AND current_step = :step
                """,
            )
                .bind("chainId", chainId)
                .bind("step", failedStepIndex)
                .bind("output", previousOutput)
                .execute()
        }
    }

    // ── Query methods ──────────────────────────────────────────────

    fun findById(chainId: String): ChainJob? =
        jdbi.withHandle<ChainJob?, Exception> { h ->
            h.createQuery("SELECT * FROM chain_job WHERE chain_id = :chainId")
                .bind("chainId", chainId)
                .mapTo(ChainJob::class.java)
                .findOne()
                .orElse(null)
        }

    fun findByStatus(status: ChainStatus): List<ChainJob> =
        jdbi.withHandle<List<ChainJob>, Exception> { h ->
            h.createQuery("SELECT * FROM chain_job WHERE status = :status")
                .bind("status", status.name)
                .mapTo(ChainJob::class.java)
                .list()
        }

    fun findAll(limit: Int = 100): List<ChainJob> =
        jdbi.withHandle<List<ChainJob>, Exception> { h ->
            h.createQuery("SELECT * FROM chain_job ORDER BY created_at DESC FETCH FIRST :limit ROWS ONLY")
                .bind("limit", limit)
                .mapTo(ChainJob::class.java)
                .list()
        }

    /**
     * Compute the next step's payload based on the transform strategy.
     *
     * @param previousOutput Output from the completed step.
     * @param originalParams The chain's original input parameters.
     * @param step The next step definition.
     */
    fun computeNextPayload(
        previousOutput: String?,
        originalParams: String,
        step: StepDefinition,
    ): String =
        when (step.payloadTransform) {
            PayloadTransformStrategy.PASS_OUTPUT -> previousOutput ?: originalParams
            PayloadTransformStrategy.MERGE_WITH_ORIGINAL -> mergePayloads(originalParams, previousOutput)
            PayloadTransformStrategy.STATIC -> step.staticPayload ?: "{}"
        }

    private fun mergePayloads(original: String, stepOutput: String?): String {
        val base = objectMapper.readTree(original)
        if (stepOutput == null) return original
        val overlay = objectMapper.readTree(stepOutput)
        val merged = (base as com.fasterxml.jackson.databind.node.ObjectNode).deepCopy()
        overlay.fields().forEach { (key, value) -> merged.set<com.fasterxml.jackson.databind.JsonNode>(key, value) }
        return objectMapper.writeValueAsString(merged)
    }
}
