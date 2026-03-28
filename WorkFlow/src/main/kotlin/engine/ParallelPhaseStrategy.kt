package com.workflow.engine

import com.workflow.dsl.JoinPolicy

class ParallelPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val joinPolicy = context.currentSeqInfo.activity.joinPolicy
        val succeeded = evaluateJoinPolicy(joinPolicy, context.failedCount, context.totalCount)

        if (!succeeded) {
            context.failOrAdvance()?.let { return it }
        }

        return context.advanceOrComplete()
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
