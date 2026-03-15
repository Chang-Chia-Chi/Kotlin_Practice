package com.mapreduce.dag.spi

import com.mapreduce.dag.model.TriggerRule
import java.time.Duration

/**
 * The static, declarative definition of nodes and directed edges.
 *
 * Implement this interface as a CDI bean. The framework discovers all
 * blueprints at startup via [com.mapreduce.dag.registry.DagRegistrar].
 * At runtime, a user submits a "run" for a specific [dagId].
 */
interface DagBlueprint {
    val dagId: String

    /** Hierarchical namespace for organization (dot-separated). */
    val namespace: String get() = ""

    /** Human-readable description. */
    val description: String get() = ""

    /** Labels for filtering and grouping. */
    val labels: Map<String, String> get() = emptyMap()

    fun nodes(): List<DagNodeDef>

    /** Execution defaults inherited by all nodes unless overridden. */
    fun defaults(): DagDefaults = DagDefaults()

    /** Concurrency controls for this DAG. */
    fun concurrency(): DagConcurrency = DagConcurrency()
}

/**
 * Execution defaults applied to nodes that don't override them.
 *
 * @param timeout Default per-node timeout.
 * @param maxAttempts Default retry count.
 * @param retryBackoff Backoff strategy between retries.
 */
data class DagDefaults(
    val timeout: Duration = Duration.ofMinutes(30),
    val maxAttempts: Int = 3,
    val retryBackoff: RetryBackoff = RetryBackoff(),
)

/**
 * Retry backoff configuration.
 */
data class RetryBackoff(
    val strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    val initialDelay: Duration = Duration.ofSeconds(10),
    val maxDelay: Duration = Duration.ofMinutes(5),
)

enum class BackoffStrategy {
    FIXED, LINEAR, EXPONENTIAL,
}

/**
 * Concurrency controls for a DAG.
 *
 * @param maxParallelRuns Maximum concurrent RUNNING Runs for this dag_id.
 * @param maxParallelNodes Maximum concurrent QUEUED + RUNNING nodes within a single Run.
 */
data class DagConcurrency(
    val maxParallelRuns: Int = Int.MAX_VALUE,
    val maxParallelNodes: Int = Int.MAX_VALUE,
)

/**
 * Inline error handler for a node (dispatched when the node exhausts retries).
 */
data class OnFailureHandler(
    val handler: String,
    val config: Map<String, Any> = emptyMap(),
)

/**
 * A single node definition within a [DagBlueprint].
 *
 * @param taskKey Logical identifier for this node within the blueprint.
 * @param nodeType Resolves to a [DagNodeHandler] at runtime (handler = `"dag.{nodeType}"`).
 * @param dependencies List of [taskKey]s that must complete before this node is evaluated.
 * @param triggerRule Defines failure tolerance for upstream parents.
 * @param config Static configuration for this node.
 * @param timeout Per-node timeout (overrides blueprint default).
 * @param maxAttempts Per-node retry count (overrides blueprint default).
 * @param onFailure Inline error handler dispatched when the node exhausts all retries.
 */
data class DagNodeDef(
    val taskKey: String,
    val nodeType: String,
    val dependencies: List<String> = emptyList(),
    val triggerRule: TriggerRule = TriggerRule.ALL_SUCCESS,
    val config: Map<String, Any> = emptyMap(),
    val timeout: Duration? = null,
    val maxAttempts: Int? = null,
    val onFailure: OnFailureHandler? = null,
)
