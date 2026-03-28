package com.workflow.engine

import com.workflow.dsl.WorkflowDefinition

enum class PhaseType { LINEAR, SCATTER, PARALLEL }

data class SequenceInfo(
    val sequenceNumber: Int,
    val activityIndex: Int,
    val activity: com.workflow.dsl.ActivityDefinition,
    val phaseType: PhaseType,
    val nextSequence: Int?,
    val branchSequences: Map<String, Int>? = null,
)

fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
    data class Entry(val activityIndex: Int, val phaseType: PhaseType, val seq: Int)
    val entries = mutableListOf<Entry>()
    var seq = 1
    for ((i, activity) in definition.activities.withIndex()) {
        if (activity.fanOut == null) {
            entries += Entry(i, PhaseType.LINEAR, seq++)
        } else {
            entries += Entry(i, PhaseType.SCATTER, seq++)
            entries += Entry(i, PhaseType.PARALLEL, seq++)
        }
    }
    val map = mutableMapOf<Int, SequenceInfo>()
    for ((idx, entry) in entries.withIndex()) {
        val nextSeq = if (idx + 1 < entries.size) entries[idx + 1].seq else null
        map[entry.seq] = SequenceInfo(
            sequenceNumber = entry.seq,
            activityIndex = entry.activityIndex,
            activity = definition.activities[entry.activityIndex],
            phaseType = entry.phaseType,
            nextSequence = nextSeq,
        )
    }
    return map
}
