package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dsl.JoinPolicy

class ParallelPhaseStrategy(
    private val objectMapper: ObjectMapper,
) : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val fanOut = context.currentSeqInfo.activity.fanOut
            ?: throw IllegalStateException("PARALLEL phase at seq ${context.currentSeqInfo.sequenceNumber} has no fanOut definition")
        val joinPolicy = fanOut.joinPolicy
        val succeeded = evaluateJoinPolicy(joinPolicy, context.failedCount, context.totalCount)

        if (!succeeded) {
            context.failOrAdvance(payload = null)?.let { return it }
        }

        // Aggregate completed task results into JSON array (R3)
        val arrayNode = objectMapper.createArrayNode()
        context.tasks
            .filter { it.status == TaskStatus.COMPLETED }
            .mapNotNull { it.resultJson }
            .forEach { arrayNode.add(objectMapper.readTree(it)) }
        val aggregatedPayload = objectMapper.writeValueAsString(arrayNode)

        return context.advanceOrComplete(payload = aggregatedPayload)
    }

    private fun evaluateJoinPolicy(joinPolicy: JoinPolicy, failedCount: Int, totalCount: Int): Boolean {
        val succeededCount = totalCount - failedCount
        return when (joinPolicy) {
            is JoinPolicy.All -> failedCount == 0
            is JoinPolicy.Threshold -> succeededCount >= joinPolicy.n
            is JoinPolicy.Percentage -> {
                val successPct = if (totalCount > 0) (succeededCount * 100) / totalCount else 0
                successPct >= joinPolicy.pct
            }
        }
    }
}
