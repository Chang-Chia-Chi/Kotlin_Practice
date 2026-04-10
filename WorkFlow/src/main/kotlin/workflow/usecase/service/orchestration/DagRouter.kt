package com.workflow.workflow.usecase.service.orchestration

import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.createSkippedTaskForActivity
import com.workflow.workflow.model.createTaskForActivity
import java.time.Instant

/**
 * Immutable snapshot of workflow state at transaction-read time.
 * Built by the transaction orchestrator, consumed by pure routing functions.
 */
data class GateSnapshot(
    val workflowId: String,
    val definition: WorkflowDefinition,
    val sequenceMap: Map<Int, SequenceInfo>,
    val seqByName: Map<String, SequenceInfo>,
    val allCounts: Map<Int, TaskStatusCounts>,
    val tasksBySeq: Map<Int, List<Task>>,
    /** Pre-extracted branch labels from task results. Key = task ID, value = branch label or null. */
    val resultBranches: Map<String, String?>,
    val now: Instant,
)

sealed interface PhaseDecision {
    data object Abort : PhaseDecision
    data class ScatterExpand(val items: List<String>, val parallelInfo: SequenceInfo) : PhaseDecision
    data object Normal : PhaseDecision
}

data class SuccessorResult(
    val tasksToInsert: List<Task>,
    val signalQueues: Set<String>,
    val hasTerminalCompletion: Boolean,
)

// -- Edge evaluation ----------------------------------------------------------

/**
 * Determines if a single edge is "taken" given a predecessor task's terminal state.
 * Pure -- uses pre-extracted [resultBranch] instead of parsing JSON.
 */
fun isEdgeTaken(
    taskStatus: TaskStatus,
    resultBranch: String?,
    edgeLabel: String,
): Boolean {
    if (!taskStatus.isTerminal) return false
    if (taskStatus != TaskStatus.COMPLETED) return false
    if (edgeLabel == DEFAULT_BRANCH) return true
    return resultBranch == edgeLabel
}

/**
 * Checks whether any predecessor edge to [successor] is "taken" based on
 * predecessor task results and failure policies.
 */
fun isAnyEdgeTaken(
    tasksBySeq: Map<Int, List<Task>>,
    resultBranches: Map<String, String?>,
    successor: SequenceInfo,
    sequenceMap: Map<Int, SequenceInfo>,
    definition: WorkflowDefinition,
): Boolean {
    val targetActName = successor.activityName
    for ((predActName, predActivity) in definition.activities) {
        val edgesToTarget = predActivity.successors.filter { it.target == targetActName }
        if (edgesToTarget.isEmpty()) continue

        val predOutputSeq = sequenceMap.values
            .firstOrNull { si ->
                val name = si.sourceActivityName
                name == predActName && (si.phaseType == PhaseType.PARALLEL || si.phaseType == PhaseType.LINEAR)
            }?.sequenceNumber ?: continue

        val predTasks = tasksBySeq[predOutputSeq] ?: continue
        for (predTask in predTasks) {
            val branch = resultBranches[predTask.id]
            for (edge in edgesToTarget) {
                if (isEdgeTaken(predTask.status, branch, edge.label)) return true
            }
        }
    }
    return false
}

/**
 * Returns the [SequenceInfo] entries for all successor activities of the given sequence.
 * Strips the `.__parallel__` suffix to find the original scatter activity's successors.
 * Returns only SCATTER or LINEAR entries (not PARALLEL) to avoid double-dispatching.
 */
fun successorsOf(
    seqInfo: SequenceInfo,
    seqByName: Map<String, SequenceInfo>,
    definition: WorkflowDefinition,
): List<SequenceInfo> {
    val actName = seqInfo.sourceActivityName
    val activity = definition.activities[actName] ?: return emptyList()
    return activity.successors.mapNotNull { edge ->
        seqByName[edge.target]
    }.distinctBy { it.sequenceNumber }
}

// -- Phase decision -----------------------------------------------------------

/**
 * Resolves the phase-type-specific decision for the completing sequence.
 * Collapses SCATTER success/failure, PARALLEL join evaluation, and LINEAR
 * failure checks into a single discriminated result.
 *
 * Pure -- scatter items are pre-deserialized by the caller.
 */
fun resolvePhaseDecision(
    snapshot: GateSnapshot,
    seqInfo: SequenceInfo,
    status: TaskStatus,
    scatterItems: List<String>?,
): PhaseDecision = when (seqInfo.phaseType) {
    PhaseType.SCATTER -> if (status == TaskStatus.COMPLETED) {
        val items = requireNotNull(scatterItems) {
            "SCATTER phase requires scatter result for workflow ${snapshot.workflowId}"
        }
        val parallelSeq = seqInfo.sequenceNumber + 1
        // Both guards return Abort so TX2 finishes cleanly and the workflow is
        // marked FAILED, rather than throwing and leaving it permanently stuck.
        val parallelInfo = snapshot.sequenceMap[parallelSeq]
        when {
            items.isEmpty() -> PhaseDecision.Abort
            parallelInfo == null -> PhaseDecision.Abort
            else -> PhaseDecision.ScatterExpand(items, parallelInfo)
        }
    } else {
        PhaseDecision.Abort
    }

    PhaseType.PARALLEL -> {
        val counts = snapshot.allCounts[seqInfo.sequenceNumber] ?: TaskStatusCounts(0, 0, 0, 0)
        val joinPassed = counts.completed == counts.total
        if (joinPassed) PhaseDecision.Normal else PhaseDecision.Abort
    }

    PhaseType.LINEAR -> {
        if (status != TaskStatus.COMPLETED && status != TaskStatus.SKIPPED) {
            PhaseDecision.Abort
        } else {
            PhaseDecision.Normal
        }
    }
}

// -- Successor dispatch -------------------------------------------------------

/**
 * Walks DAG successors using indegree-based topological BFS (Kahn's algorithm).
 *
 * A node is enqueued only when all its predecessors are "resolved" (terminal in
 * DB or decided as SKIPPED in this loop). This avoids stale-snapshot bugs: the
 * pre-fetched allCounts doesn't reflect in-loop inserts, so polling predecessors
 * at dequeue time is fragile. With indegree tracking, structural correctness is
 * guaranteed -- a node is never evaluated until every predecessor is decided.
 *
 * PENDING nodes stop the walk (they'll trigger a new round when completed).
 * SKIPPED nodes propagate: they decrement their successors' indegrees and
 * enqueue any that reach zero.
 */
fun dispatchSuccessors(
    snapshot: GateSnapshot,
    seqInfo: SequenceInfo,
): SuccessorResult {
    val resolvedSeqs = mutableSetOf<Int>()
    for ((seq, counts) in snapshot.allCounts) {
        if (counts.total > 0 && counts.nonTerminal == 0) resolvedSeqs += seq
    }
    resolvedSeqs += seqInfo.sequenceNumber

    val pendingInserts = mutableListOf<Task>()
    val visitedSeqs = mutableSetOf<Int>()
    val signalQueues = mutableSetOf<String>()
    var hasTerminalCompletion = false

    val indegree = mutableMapOf<Int, Int>()
    val discovered = mutableMapOf<Int, SequenceInfo>()
    val evalQueue = ArrayDeque<Int>()

    fun discoverSuccessors(successors: List<SequenceInfo>) {
        for (succ in successors) {
            val sSeq = succ.sequenceNumber
            if ((snapshot.allCounts[sSeq]?.total ?: 0) > 0 || sSeq in visitedSeqs) continue
            if (sSeq in discovered) {
                val newDeg = (indegree[sSeq] ?: 0) - 1
                indegree[sSeq] = newDeg
                if (newDeg <= 0) evalQueue += sSeq
            } else {
                discovered[sSeq] = succ
                val deg = succ.predecessorSequences.count { it !in resolvedSeqs }
                indegree[sSeq] = deg
                if (deg <= 0) evalQueue += sSeq
            }
        }
    }

    discoverSuccessors(successorsOf(seqInfo, snapshot.seqByName, snapshot.definition))

    while (evalQueue.isNotEmpty()) {
        val sSeq = evalQueue.removeFirst()
        if (sSeq in visitedSeqs) continue
        val successor = discovered[sSeq] ?: continue

        val edgeTaken = isAnyEdgeTaken(
            snapshot.tasksBySeq, snapshot.resultBranches, successor, snapshot.sequenceMap, snapshot.definition,
        )

        if (edgeTaken) {
            val task = createTaskForActivity(
                snapshot.workflowId, successor.activityName, sSeq, successor.activity, snapshot.now,
            )
            pendingInserts += task
            visitedSeqs += sSeq
            signalQueues += successor.activity.queue
        } else {
            val skipped = createSkippedTaskForActivity(
                snapshot.workflowId, successor.activityName, sSeq, successor.activity, snapshot.now,
            )
            pendingInserts += skipped
            visitedSeqs += sSeq
            resolvedSeqs += sSeq

            // SCATTER always has a companion PARALLEL at sSeq+1; skip it too
            if (successor.phaseType == PhaseType.SCATTER) {
                val parallelSeq = sSeq + 1
                val parallelInfo = snapshot.sequenceMap[parallelSeq]
                if (parallelInfo != null && parallelInfo.phaseType == PhaseType.PARALLEL) {
                    val parallelSkipped = createSkippedTaskForActivity(
                        snapshot.workflowId, parallelInfo.activityName, parallelSeq,
                        parallelInfo.activity, snapshot.now,
                    )
                    pendingInserts += parallelSkipped
                    visitedSeqs += parallelSeq
                    resolvedSeqs += parallelSeq
                }
            }

            if (successor.activity.isTerminal) {
                hasTerminalCompletion = true
            } else {
                discoverSuccessors(successorsOf(successor, snapshot.seqByName, snapshot.definition))
            }
        }
    }

    return SuccessorResult(pendingInserts, signalQueues, hasTerminalCompletion)
}
