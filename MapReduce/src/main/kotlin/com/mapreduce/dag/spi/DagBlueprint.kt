package com.mapreduce.dag.spi

import com.mapreduce.dag.model.TriggerRule

/**
 * The static, declarative definition of nodes and directed edges.
 *
 * Implement this interface as a CDI bean. The framework discovers all
 * blueprints at startup via [com.mapreduce.dag.registry.DagRegistrar].
 * At runtime, a user submits a "run" for a specific [dagId].
 */
interface DagBlueprint {
    val dagId: String
    fun nodes(): List<DagNodeDef>
}

/**
 * A single node definition within a [DagBlueprint].
 *
 * @param taskKey Logical identifier for this node within the blueprint.
 * @param nodeType Resolves to a [DagNodeHandler] at runtime (handler = `"dag.{nodeType}"`).
 * @param dependencies List of [taskKey]s that must complete before this node is evaluated.
 * @param triggerRule Defines failure tolerance for upstream parents.
 */
data class DagNodeDef(
    val taskKey: String,
    val nodeType: String,
    val dependencies: List<String> = emptyList(),
    val triggerRule: TriggerRule = TriggerRule.ALL_SUCCESS,
)
