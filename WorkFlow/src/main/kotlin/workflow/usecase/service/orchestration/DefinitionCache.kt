package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.buildSequenceMap
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

/**
 * Parsed WorkflowDefinition plus its derived sequence indices.
 *
 * Cached per workflow because (a) parsing definition JSON and (b) building the
 * sequence map are both non-trivial and fully deterministic for a given
 * definition. The cache entry is invalidated once the workflow reaches a
 * terminal state; no further tasks will reference it.
 */
data class CachedDefinition(
    val definition: WorkflowDefinition,
    val sequenceMap: Map<Int, SequenceInfo>,
    val seqByName: Map<String, SequenceInfo>,
)

/**
 * Shared per-workflow cache of parsed [WorkflowDefinition] and derived sequence
 * indices. Both [DefaultPhaseGate] and
 * [WorkerLoop][com.workflow.worker.usecase.service.execution.WorkerLoop] consult
 * this cache to avoid re-parsing on the hot path.
 *
 * **Lifetime:** Entries live until [invalidate] is called — typically when the
 * workflow reaches a terminal state. Unbounded growth is acceptable because
 * entries are small and bounded by active-workflow count.
 */
@ApplicationScoped
class DefinitionCache(
    private val objectMapper: ObjectMapper,
) {
    private val cache = ConcurrentHashMap<String, CachedDefinition>()

    /** Fast path: return the cached entry if present, else null. */
    fun getOrNull(workflowId: String): CachedDefinition? = cache[workflowId]

    /**
     * Cache miss path: parse [definitionJson], build derived indices, and
     * store the entry. Safe under concurrent load — the first winner wins and
     * subsequent callers receive the same instance.
     */
    fun load(workflowId: String, definitionJson: String): CachedDefinition {
        cache[workflowId]?.let { return it }
        val definition = objectMapper.readValue<WorkflowDefinition>(definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqByName = sequenceMap.values
            .filter { it.phaseType != PhaseType.PARALLEL }
            .associateBy { it.activityName }
        val entry = CachedDefinition(definition, sequenceMap, seqByName)
        return cache.putIfAbsent(workflowId, entry) ?: entry
    }

    /** Drop the cached entry for a workflow that has reached a terminal state. */
    fun invalidate(workflowId: String) {
        cache.remove(workflowId)
    }
}
