package com.workflow.engine

import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.WorkflowDefinition
import java.time.Instant
import java.time.temporal.ChronoUnit

interface PhaseStrategy {
    fun resolve(context: PhaseContext): AdvancementDecision
}

/**
 * All inputs the strategy needs. Immutable snapshot.
 * [tasks] reflect the committed state within the current transaction, including any self-update.
 */
data class PhaseContext(
    val workflow: WorkflowRun,
    val definition: WorkflowDefinition,
    val currentSeqInfo: SequenceInfo,
    val sequenceMap: Map<Int, SequenceInfo>,
    val failedCount: Int,
    val totalCount: Int,
    val tasks: List<Task>,
)

sealed interface AdvancementDecision {
    data class Advance(val nextSequence: Int, val tasks: List<Task>) : AdvancementDecision
    data object Complete : AdvancementDecision
    data class Abort(val reason: String) : AdvancementDecision
}

/**
 * Shared failure-policy check. Returns null if no failure (caller continues to normal advance).
 * Returns [AdvancementDecision.Abort] for ABORT, or advance/complete for BEST_EFFORT.
 */
fun PhaseContext.failOrAdvance(): AdvancementDecision? {
    if (failedCount == 0) return null
    return when (currentSeqInfo.activity.failurePolicy) {
        FailurePolicy.ABORT -> AdvancementDecision.Abort(
            "$failedCount task(s) failed at sequence ${currentSeqInfo.sequenceNumber}",
        )
        FailurePolicy.BEST_EFFORT -> advanceOrComplete()
    }
}

/**
 * Build an [AdvancementDecision.Advance] to the next sequence, or [AdvancementDecision.Complete]
 * if this is the last sequence. Creates a single task for the next sequence's activity.
 */
fun PhaseContext.advanceOrComplete(): AdvancementDecision {
    val nextSeq = currentSeqInfo.nextSequence ?: return AdvancementDecision.Complete
    val nextSeqInfo = sequenceMap[nextSeq]!!
    val task = createTaskForActivity(
        workflowId = workflow.id,
        sequenceNumber = nextSeq,
        activity = nextSeqInfo.activity,
        now = Instant.now().truncatedTo(ChronoUnit.MICROS),
    )
    return AdvancementDecision.Advance(nextSeq, listOf(task))
}
