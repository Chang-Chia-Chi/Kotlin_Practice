package com.workflow.workflow.model

enum class PhaseType { LINEAR, SCATTER, PARALLEL }

data class SequenceInfo(
    val sequenceNumber: Int,
    val activityIndex: Int,
    val activity: ActivityDefinition,
    val phaseType: PhaseType,
    val nextSequence: Int?,
    val branchSequences: Map<String, Int>? = null,
)

fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
    val fanOutTargets = definition.activities.mapNotNull { it.fanOut }.toSet()
    val map = mutableMapOf<Int, SequenceInfo>()
    for ((i, activity) in definition.activities.withIndex()) {
        val seq = i + 1
        val nextSeq = if (i + 1 < definition.activities.size) i + 2 else null
        val phaseType = if (activity.name in fanOutTargets) PhaseType.PARALLEL else PhaseType.LINEAR
        map[seq] = SequenceInfo(
            sequenceNumber = seq,
            activityIndex = i,
            activity = activity,
            phaseType = phaseType,
            nextSequence = nextSeq,
        )
    }
    return map
}
