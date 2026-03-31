package com.workflow.workflow.model

data class PhaseContext(
    val workflow: WorkflowRun,
    val definition: WorkflowDefinition,
    val currentSeqInfo: SequenceInfo,
    val sequenceMap: Map<Int, SequenceInfo>,
    val failedCount: Int,
    val totalCount: Int,
)

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
