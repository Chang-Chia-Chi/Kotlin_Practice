package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory

@ApplicationScoped
class ActivityInputResolver(
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(ActivityInputResolver::class.java)

    suspend fun resolve(
        inputs: Map<String, String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: suspend (Int) -> List<Task>,
    ): String? {
        if (inputs.isEmpty()) return null

        val resultNode = objectMapper.createObjectNode()

        for ((inputName, ref) in inputs) {
            val (activityName, fieldPath) = parseRef(ref)
            val resolved = resolveActivity(activityName, fieldPath, sequenceMap, tasksBySequence)
            resultNode.set<JsonNode>(inputName, resolved)
        }

        return objectMapper.writeValueAsString(resultNode)
    }

    private fun parseRef(ref: String): Pair<String, List<String>> {
        val parts = ref.split('.')
        return parts.first() to parts.drop(1)
    }

    private suspend fun resolveActivity(
        activityName: String,
        fieldPath: List<String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: suspend (Int) -> List<Task>,
    ): JsonNode {
        val seqEntry = sequenceMap.values.firstOrNull { it.activity.name == activityName }
            ?: throw IllegalArgumentException(
                "Input reference '$activityName' does not match any activity in the workflow. " +
                    "Available activities: ${sequenceMap.values.map { it.activity.name }}"
            )

        return when (seqEntry.phaseType) {
            PhaseType.PARALLEL -> {
                val tasks = tasksBySequence(seqEntry.sequenceNumber)
                    .filter { it.status == TaskStatus.COMPLETED }
                aggregateFanOut(tasks, fieldPath)
            }
            PhaseType.LINEAR -> {
                val task = tasksBySequence(seqEntry.sequenceNumber)
                    .firstOrNull { it.status == TaskStatus.COMPLETED }
                val resultJson = task?.resultJson
                if (resultJson == null) return objectMapper.nullNode()
                val resultTree = objectMapper.readTree(resultJson)
                traversePath(resultTree, fieldPath)
            }
            PhaseType.SCATTER -> {
                val task = tasksBySequence(seqEntry.sequenceNumber)
                    .firstOrNull { it.status == TaskStatus.COMPLETED }
                val resultJson = task?.resultJson
                if (resultJson == null) return objectMapper.nullNode()
                val resultTree = objectMapper.readTree(resultJson)
                traversePath(resultTree, fieldPath)
            }
        }
    }

    private fun traversePath(node: JsonNode, fieldPath: List<String>): JsonNode {
        var current = node
        for (key in fieldPath) {
            current = current.path(key)
            if (current.isMissingNode) {
                log.warn("Field path segment '{}' not found in result. Full path: {}", key, fieldPath.joinToString("."))
                return current
            }
        }
        return current
    }

    private fun aggregateFanOut(
        tasks: List<Task>,
        fieldPath: List<String>,
    ): ArrayNode {
        val arrayNode = objectMapper.createArrayNode()
        for (task in tasks) {
            if (task.status != TaskStatus.COMPLETED) continue
            val resultJson = task.resultJson ?: continue
            val resultTree = objectMapper.readTree(resultJson)
            arrayNode.add(traversePath(resultTree, fieldPath))
        }
        return arrayNode
    }
}
