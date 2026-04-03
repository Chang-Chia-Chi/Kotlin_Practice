package com.workflow.workflow.model

enum class PhaseType { LINEAR, SCATTER, PARALLEL }

data class SequenceInfo(
    val sequenceNumber: Int,
    val activityName: String,
    val activity: ActivityDefinition,
    val phaseType: PhaseType,
    val predecessorSequences: List<Int>,
)

fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
    val topoOrder = topologicalSort(definition)

    // Build predecessor map: activityName -> list of activity names that have edges to it
    val predecessorNames: Map<String, MutableList<String>> =
        definition.activities.keys.associateWith { mutableListOf() }
    for ((actName, activity) in definition.activities) {
        for (edge in activity.successors) {
            predecessorNames[edge.target]!!.add(actName)
        }
    }

    // Assign sequence numbers in topological order
    var seqCounter = 1
    val linearSeq = mutableMapOf<String, Int>()
    val scatterSeq = mutableMapOf<String, Int>()
    val parallelSeq = mutableMapOf<String, Int>()

    for (actName in topoOrder) {
        val activity = definition.activities[actName]!!
        if (activity.fanOut != null) {
            scatterSeq[actName] = seqCounter++
            parallelSeq[actName] = seqCounter++
        } else {
            linearSeq[actName] = seqCounter++
        }
    }

    // The "output seq" of a predecessor: what successors must wait for
    fun outputSeq(name: String): Int = parallelSeq[name] ?: linearSeq[name]!!

    val map = mutableMapOf<Int, SequenceInfo>()

    for (actName in topoOrder) {
        val activity = definition.activities[actName]!!
        val predSeqs = predecessorNames[actName]!!.map { outputSeq(it) }

        if (activity.fanOut != null) {
            val sSeq = scatterSeq[actName]!!
            val pSeq = parallelSeq[actName]!!

            map[sSeq] = SequenceInfo(
                sequenceNumber = sSeq,
                activityName = actName,
                activity = activity,
                phaseType = PhaseType.SCATTER,
                predecessorSequences = predSeqs,
            )

            // Synthetic activity for parallel tasks — uses FanOutDefinition settings.
            // The transition (handlerKey) for each parallel worker comes from fanOut.transition.
            // The scatter activity's own failurePolicy applies at join evaluation time.
            val fanOut = activity.fanOut!!
            val parallelActivity = ActivityDefinition(
                name = "$actName.__parallel__",
                transition = fanOut.transition,
                retries = fanOut.retries,
                failurePolicy = activity.failurePolicy, // scatter activity's policy governs join failure
                deadline = fanOut.deadline,
                backoffBase = fanOut.backoffBase,
                backoffCap = fanOut.backoffCap,
                queue = fanOut.queue,
            )
            map[pSeq] = SequenceInfo(
                sequenceNumber = pSeq,
                activityName = "$actName.__parallel__",
                activity = parallelActivity,
                phaseType = PhaseType.PARALLEL,
                predecessorSequences = listOf(sSeq),
            )
        } else {
            val seq = linearSeq[actName]!!
            map[seq] = SequenceInfo(
                sequenceNumber = seq,
                activityName = actName,
                activity = activity,
                phaseType = PhaseType.LINEAR,
                predecessorSequences = predSeqs,
            )
        }
    }

    return map
}
