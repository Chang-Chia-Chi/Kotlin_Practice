package com.workflow.engine

import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.WorkflowDefinition

interface PhaseStrategy {
    fun resolve(context: PhaseContext): AdvancementDecision
}

data class PhaseContext(
    val workflow: WorkflowRun,
    val definition: WorkflowDefinition,
    val currentSeqInfo: SequenceInfo,
    val sequenceMap: Map<Int, SequenceInfo>,
    val failedCount: Int,
    val totalCount: Int,
)

sealed interface AdvancementDecision {
    data class Advance(val nextSequence: Int) : AdvancementDecision
    data object Complete : AdvancementDecision
    data class Abort(val reason: String) : AdvancementDecision
}

fun PhaseContext.failOrAdvance(): AdvancementDecision? {
    if (failedCount == 0) return null
    return when (currentSeqInfo.activity.failurePolicy) {
        FailurePolicy.ABORT -> AdvancementDecision.Abort(
            "$failedCount task(s) failed at sequence ${currentSeqInfo.sequenceNumber}",
        )
        FailurePolicy.BEST_EFFORT -> advanceOrComplete()
    }
}

fun PhaseContext.advanceOrComplete(): AdvancementDecision {
    val nextSeq = currentSeqInfo.nextSequence ?: return AdvancementDecision.Complete
    return AdvancementDecision.Advance(nextSeq)
}
