package com.mapreduce.chain.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

enum class ChainStatus {
    RUNNING, COMPLETED, FAILED
}

enum class ChainFailurePolicy {
    FAIL_CHAIN, SKIP_STEP
}

enum class PayloadTransformStrategy {
    PASS_OUTPUT, MERGE_WITH_ORIGINAL, STATIC
}

/**
 * A single chained-task execution. Tracks the ordered sequence of steps,
 * current progress, and the output of the most recently completed step.
 *
 * The `chain_id` doubles as `group_id` in the generic task table, enabling
 * correlation queries across Layer 1 and Layer 2.
 */
data class ChainJob(
    @ColumnName("chain_id") val chainId: String,
    @ColumnName("chain_type") val chainType: String,
    val status: ChainStatus,
    @ColumnName("current_step") val currentStep: Int,
    @ColumnName("total_steps") val totalSteps: Int,
    @ColumnName("chain_params") val chainParams: String,
    @ColumnName("failure_policy") val failurePolicy: ChainFailurePolicy = ChainFailurePolicy.FAIL_CHAIN,
    @ColumnName("last_step_output") val lastStepOutput: String? = null,
    @ColumnName("error_message") val errorMessage: String? = null,
    val version: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)
