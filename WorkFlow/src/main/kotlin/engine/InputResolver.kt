package com.workflow.engine

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class InputResolver(
    private val objectMapper: ObjectMapper,
) {

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

    private fun parseRef(ref: String): Pair<String, String?> {
        val dot = ref.indexOf('.')
        return if (dot < 0) ref to null
        else ref.substring(0, dot) to ref.substring(dot + 1)
    }

    private suspend fun resolveActivity(
        activityName: String,
        fieldPath: String?,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: suspend (Int) -> List<Task>,
    ): JsonNode {
        val seqEntry = sequenceMap.values.first { it.activity.name == activityName }
        val isFanOut = seqEntry.activity.fanOut != null

        if (isFanOut) {
            val parallelSeq = findParallelSequence(seqEntry, sequenceMap)
            val tasks = tasksBySequence(parallelSeq)
                .filter { it.status == TaskStatus.COMPLETED }
            return aggregateFanOut(tasks, fieldPath)
        }

        val tasks = tasksBySequence(seqEntry.sequenceNumber)
        val task = tasks.firstOrNull { it.status == TaskStatus.COMPLETED }
        val resultJson = task?.resultJson

        if (resultJson == null) return objectMapper.nullNode()

        val resultTree = objectMapper.readTree(resultJson)
        return if (fieldPath != null) resultTree.path(fieldPath) else resultTree
    }

    private fun findParallelSequence(
        scatterSeqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
    ): Int {
        val nextSeq = scatterSeqInfo.nextSequence
            ?: throw IllegalStateException("Fan-out activity '${scatterSeqInfo.activity.name}' has no parallel sequence")
        val nextInfo = sequenceMap[nextSeq]!!
        require(nextInfo.phaseType == PhaseType.PARALLEL) {
            "Expected PARALLEL at sequence $nextSeq but found ${nextInfo.phaseType}"
        }
        return nextSeq
    }

    private fun aggregateFanOut(
        tasks: List<Task>,
        fieldPath: String?,
    ): ArrayNode {
        val arrayNode = objectMapper.createArrayNode()
        for (task in tasks) {
            val resultJson = task.resultJson ?: continue
            val resultTree = objectMapper.readTree(resultJson)
            if (fieldPath != null) {
                arrayNode.add(resultTree.path(fieldPath))
            } else {
                arrayNode.add(resultTree)
            }
        }
        return arrayNode
    }
}
