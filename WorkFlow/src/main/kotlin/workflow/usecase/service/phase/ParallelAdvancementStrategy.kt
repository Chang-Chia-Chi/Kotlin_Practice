package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseContext
import com.workflow.workflow.model.advanceOrComplete
import com.workflow.workflow.model.failOrAdvance
import com.workflow.workflow.usecase.port.inbound.phase.AdvancementStrategy

class ParallelAdvancementStrategy : AdvancementStrategy {

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
