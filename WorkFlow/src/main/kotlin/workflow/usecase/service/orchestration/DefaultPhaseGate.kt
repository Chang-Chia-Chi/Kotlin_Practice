package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.createSkippedTaskForActivity
import com.workflow.workflow.model.createTaskForActivity
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * DAG-aware phase gate that evaluates successor activities after each task completion.
 *
 * Implements the full algorithm from spec section 5.2:
 * 1. Update task to terminal status (fenced by claimedBy/claimedAt)
 * 2. Barrier probe: wait for all tasks at the same sequence to complete
 * 3. Phase decision: SCATTER/PARALLEL special cases, failure policy
 * 4. Successor evaluation: walk DAG edges, insert PENDING or SKIPPED tasks
 * 5. Completion check: if all tasks are terminal, mark workflow COMPLETED
 * 6. CAS guard: version increment prevents lost updates
 *
 * All steps execute within a single ACID transaction. CAS loss triggers
 * a retry (up to 10 attempts) from the beginning of the transaction.
 */
@ApplicationScoped
class DefaultPhaseGate(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: WorkerNotifier,
) : PhaseGate {

    private val log = LoggerFactory.getLogger(DefaultPhaseGate::class.java)

    override suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String?,
        claimedAt: Instant?,
    ) {
        val signalQueues = withCasRetry(workflowId) { handle ->
            val updated = taskRepo.updateStatusWithHandle(
                handle, taskId, status, resultJson, claimedBy, claimedAt,
            )
            if (!updated) return@withCasRetry emptyList()

            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@withCasRetry emptyList()

            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
            if (workflow.status != WorkflowStatus.RUNNING) return@withCasRetry emptyList()

            val ctx = buildGateContext(handle, workflowId, workflow.version, workflow.definitionJson)
            val seqInfo = ctx.sequenceMap[sequenceNumber]
                ?: throw IllegalStateException("Seq $sequenceNumber not in definition for $workflowId")

            val decision = resolvePhaseDecision(ctx, seqInfo, status, resultJson)

            when (decision) {
                PhaseDecision.Abort -> {
                    abortWorkflow(handle, workflowId)
                    return@withCasRetry emptyList()
                }
                is PhaseDecision.ScatterExpand -> {
                    val parallelTasks = decision.items.map {
                        createTaskForActivity(
                            workflowId, decision.parallelInfo.activityName,
                            decision.parallelInfo.sequenceNumber,
                            decision.parallelInfo.activity, ctx.now, item = it,
                        )
                    }
                    taskRepo.insertBatchWithHandle(handle, parallelTasks)
                    requireCasWin(handle, workflowId, ctx.workflowVersion)
                    return@withCasRetry listOf(decision.parallelInfo.activity.queue)
                }
                PhaseDecision.ForceDefaultBranch,
                PhaseDecision.Normal -> { /* fall through to successor evaluation */ }
            }

            val forceDefault = decision == PhaseDecision.ForceDefaultBranch
            val result = dispatchSuccessors(ctx, seqInfo, forceDefault)

            if (result.tasksToInsert.isNotEmpty()) {
                insertMixedTaskBatch(handle, result.tasksToInsert)
            }

            val checkCompletion = result.hasTerminalCompletion || seqInfo.activity.isTerminal
            if (checkCompletion) {
                val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
                if (globalNonTerminal == 0) {
                    workflowRepo.updateStatusWithHandle(
                        handle, workflowId, WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING,
                    )
                    return@withCasRetry emptyList()
                }
            }

            requireCasWin(handle, workflowId, ctx.workflowVersion)
            result.signalQueues.toList()
        }

        signalQueues.forEach { notifier.signal(it) }
    }

    override suspend fun recoverStuckWorkflow(workflowId: String) {
        val signalQueues = withCasRetry(workflowId) { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: run {
                    log.warn("Workflow not found during recovery: {}", workflowId)
                    return@withCasRetry emptyList()
                }
            if (workflow.status != WorkflowStatus.RUNNING) return@withCasRetry emptyList()

            val ctx = buildGateContext(handle, workflowId, workflow.version, workflow.definitionJson)
            val signalQueueSet = mutableSetOf<String>()

            for ((seq, seqInfo) in ctx.sequenceMap.entries.sortedBy { it.key }) {
                if ((ctx.allCounts[seq]?.total ?: 0) > 0) continue

                val allPredTerminal = seqInfo.predecessorSequences.isEmpty() ||
                    seqInfo.predecessorSequences.all { predSeq ->
                        (ctx.allCounts[predSeq]?.total ?: 0) > 0 &&
                            (ctx.allCounts[predSeq]?.nonTerminal ?: 0) == 0
                    }
                if (!allPredTerminal) continue

                when (seqInfo.phaseType) {
                    PhaseType.SCATTER -> {
                        val task = createTaskForActivity(
                            workflowId, seqInfo.activityName, seq, seqInfo.activity, ctx.now,
                        )
                        taskRepo.insertBatchWithHandle(handle, listOf(task))
                        signalQueueSet += seqInfo.activity.queue
                    }
                    PhaseType.PARALLEL -> continue
                    PhaseType.LINEAR -> {
                        val edgeTaken = isAnyEdgeTaken(
                            ctx.tasksBySeq, seqInfo, ctx.sequenceMap, ctx.definition,
                        )
                        if (edgeTaken || seqInfo.predecessorSequences.isEmpty()) {
                            val task = createTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, ctx.now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(task))
                            signalQueueSet += seqInfo.activity.queue
                        } else {
                            val skipped = createSkippedTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, ctx.now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(skipped))
                        }
                    }
                }
            }

            val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
            if (globalNonTerminal == 0) {
                // allCounts is safe: recovery only inserts PENDING/SKIPPED tasks, never FAILED
                val abortFailure = ctx.sequenceMap.entries.any { (seq, seqInfo) ->
                    seqInfo.phaseType != PhaseType.PARALLEL &&
                        seqInfo.activity.failurePolicy == FailurePolicy.ABORT &&
                        (ctx.allCounts[seq]?.failed ?: 0) > 0
                }
                val terminalStatus = if (abortFailure) WorkflowStatus.FAILED else WorkflowStatus.COMPLETED
                workflowRepo.updateStatusWithHandle(
                    handle, workflowId, terminalStatus, WorkflowStatus.RUNNING,
                )
                return@withCasRetry emptyList()
            }

            if (signalQueueSet.isEmpty()) return@withCasRetry emptyList()

            requireCasWin(handle, workflowId, ctx.workflowVersion)
            signalQueueSet.toList()
        }

        signalQueues.forEach { notifier.signal(it) }
    }

    // -- Phase decision ------------------------------------------------------

    private sealed interface PhaseDecision {
        data object Abort : PhaseDecision
        data class ScatterExpand(val items: List<String>, val parallelInfo: SequenceInfo) : PhaseDecision
        data object ForceDefaultBranch : PhaseDecision
        data object Normal : PhaseDecision
    }

    /**
     * Resolves the phase-type-specific decision for the completing sequence.
     * Collapses SCATTER success/failure, PARALLEL join evaluation, and LINEAR
     * failure checks into a single discriminated result.
     */
    private fun resolvePhaseDecision(
        ctx: GateContext,
        seqInfo: SequenceInfo,
        status: TaskStatus,
        resultJson: String?,
    ): PhaseDecision = when (seqInfo.phaseType) {
        PhaseType.SCATTER -> if (status == TaskStatus.COMPLETED) {
            val items: List<String> = objectMapper.readValue(
                resultJson ?: throw IllegalStateException(
                    "SCATTER phase requires scatter result for workflow ${ctx.workflowId}",
                ),
            )
            require(items.isNotEmpty()) {
                "Fan-out produced 0 items for workflow ${ctx.workflowId}"
            }
            val parallelSeq = seqInfo.sequenceNumber + 1
            val parallelInfo = ctx.sequenceMap[parallelSeq]!!
            PhaseDecision.ScatterExpand(items, parallelInfo)
        } else {
            resolveFailureFallback(seqInfo.activity.failurePolicy)
        }

        PhaseType.PARALLEL -> {
            val counts = ctx.allCounts[seqInfo.sequenceNumber] ?: TaskStatusCounts(0, 0, 0, 0)
            val scatterActName = seqInfo.activityName.removeSuffix(".__parallel__")
            val scatterActivity = ctx.definition.activities[scatterActName]
            val joinPolicy = scatterActivity?.fanOut?.joinPolicy ?: JoinPolicy.All
            val joinPassed = evaluateJoinPolicy(joinPolicy, counts.completed, counts.total)
            if (joinPassed) PhaseDecision.Normal
            else resolveFailureFallback(seqInfo.activity.failurePolicy)
        }

        PhaseType.LINEAR -> {
            if (status != TaskStatus.COMPLETED && status != TaskStatus.SKIPPED &&
                seqInfo.activity.failurePolicy == FailurePolicy.ABORT
            ) {
                PhaseDecision.Abort
            } else {
                PhaseDecision.Normal
            }
        }
    }

    private fun resolveFailureFallback(failurePolicy: FailurePolicy): PhaseDecision =
        if (failurePolicy == FailurePolicy.ABORT) PhaseDecision.Abort
        else PhaseDecision.ForceDefaultBranch

    // -- Successor dispatch --------------------------------------------------

    private data class SuccessorResult(
        val tasksToInsert: List<Task>,
        val signalQueues: Set<String>,
        val hasTerminalCompletion: Boolean,
    )

    /**
     * Walks DAG successors using indegree-based topological BFS (Kahn's algorithm).
     *
     * A node is enqueued only when all its predecessors are "resolved" (terminal in
     * DB or decided as SKIPPED in this loop). This avoids stale-snapshot bugs: the
     * pre-fetched allCounts doesn't reflect in-loop inserts, so polling predecessors
     * at dequeue time is fragile. With indegree tracking, structural correctness is
     * guaranteed — a node is never evaluated until every predecessor is decided.
     *
     * PENDING nodes stop the walk (they'll trigger a new round when completed).
     * SKIPPED nodes propagate: they decrement their successors' indegrees and
     * enqueue any that reach zero.
     */
    private fun dispatchSuccessors(
        ctx: GateContext,
        seqInfo: SequenceInfo,
        forceDefault: Boolean,
    ): SuccessorResult {
        // Sequences whose tasks are all terminal — either from DB or decided in this loop
        val resolvedSeqs = mutableSetOf<Int>()
        for ((seq, counts) in ctx.allCounts) {
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

        // Discover successor nodes: compute indegree and enqueue those ready (indegree 0).
        // For already-discovered nodes, decrement indegree for the newly-resolved predecessor.
        fun discoverSuccessors(successors: List<SequenceInfo>) {
            for (succ in successors) {
                val sSeq = succ.sequenceNumber
                if ((ctx.allCounts[sSeq]?.total ?: 0) > 0 || sSeq in visitedSeqs) continue
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

        discoverSuccessors(successorsOf(seqInfo, ctx.seqByName, ctx.definition))

        while (evalQueue.isNotEmpty()) {
            val sSeq = evalQueue.removeFirst()
            if (sSeq in visitedSeqs) continue
            val successor = discovered[sSeq] ?: continue

            val edgeTaken = if (forceDefault) {
                hasDefaultBranchEdge(successor, ctx.definition)
            } else {
                isAnyEdgeTaken(ctx.tasksBySeq, successor, ctx.sequenceMap, ctx.definition)
            }

            if (edgeTaken) {
                val task = createTaskForActivity(
                    ctx.workflowId, successor.activityName, sSeq, successor.activity, ctx.now,
                )
                pendingInserts += task
                visitedSeqs += sSeq
                signalQueues += successor.activity.queue
            } else {
                val skipped = createSkippedTaskForActivity(
                    ctx.workflowId, successor.activityName, sSeq, successor.activity, ctx.now,
                )
                pendingInserts += skipped
                visitedSeqs += sSeq
                resolvedSeqs += sSeq

                // SCATTER always has a companion PARALLEL at sSeq+1; skip it too
                if (successor.phaseType == PhaseType.SCATTER) {
                    val parallelSeq = sSeq + 1
                    val parallelInfo = ctx.sequenceMap[parallelSeq]
                    if (parallelInfo != null && parallelInfo.phaseType == PhaseType.PARALLEL) {
                        val parallelSkipped = createSkippedTaskForActivity(
                            ctx.workflowId, parallelInfo.activityName, parallelSeq,
                            parallelInfo.activity, ctx.now,
                        )
                        pendingInserts += parallelSkipped
                        visitedSeqs += parallelSeq
                        resolvedSeqs += parallelSeq
                    }
                }

                if (successor.activity.isTerminal) {
                    hasTerminalCompletion = true
                } else {
                    discoverSuccessors(successorsOf(successor, ctx.seqByName, ctx.definition))
                }
            }
        }

        return SuccessorResult(pendingInserts, signalQueues, hasTerminalCompletion)
    }

    // -- Gate context --------------------------------------------------------

    private class GateContext(
        val workflowId: String,
        val workflowVersion: Int,
        val definition: WorkflowDefinition,
        val sequenceMap: Map<Int, SequenceInfo>,
        val seqByName: Map<String, SequenceInfo>,
        val allCounts: Map<Int, TaskStatusCounts>,
        val tasksBySeq: Map<Int, List<Task>>,
        val now: Instant,
    )

    /** Builds definition, sequence maps, and pre-fetched task state (eliminates N+1). */
    private fun buildGateContext(
        handle: Handle,
        workflowId: String,
        workflowVersion: Int,
        definitionJson: String,
    ): GateContext {
        val definition = objectMapper.readValue<WorkflowDefinition>(definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqByName = sequenceMap.values
            .filter { it.phaseType != PhaseType.PARALLEL }
            .associateBy { it.activityName }
        // Pre-fetch all task counts and tasks (eliminates N+1)
        val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
        val tasksBySeq = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
            .groupBy { it.sequenceNumber }
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

        return GateContext(
            workflowId, workflowVersion, definition, sequenceMap, seqByName,
            allCounts, tasksBySeq, now,
        )
    }

    // -- Transaction / CAS helpers -------------------------------------------

    /**
     * Executes [block] inside a transaction with CAS retry logic.
     * The block returns the list of queue names to signal after commit.
     * On [RetryableException], retries up to 10 times before failing.
     */
    private suspend fun withCasRetry(
        workflowId: String,
        block: (Handle) -> List<String>,
    ): List<String> {
        var attempts = 0
        while (true) {
            try {
                return jdbi.inTransactionSuspend<List<String>, Exception> { handle -> block(handle) }
            } catch (e: RetryableException) {
                if (++attempts >= MAX_CAS_RETRIES) {
                    throw IllegalStateException("CAS retry exhausted for $workflowId", e)
                }
                log.debug("CAS retry {} for workflow {}", attempts, workflowId)
            }
        }
    }

    /** Throws [RetryableException] if CAS increment fails. */
    private fun requireCasWin(handle: Handle, workflowId: String, expectedVersion: Int) {
        val casWon = workflowRepo.casVersionWithHandle(handle, workflowId, expectedVersion)
        if (!casWon) throw RetryableException("CAS loss for workflow $workflowId")
    }

    /** Marks workflow FAILED and cancels all pending tasks. */
    private fun abortWorkflow(handle: Handle, workflowId: String) {
        val statusUpdated = workflowRepo.updateStatusWithHandle(
            handle, workflowId, WorkflowStatus.FAILED, WorkflowStatus.RUNNING,
        )
        if (statusUpdated) taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
    }

    /**
     * Inserts a mix of PENDING and SKIPPED tasks in two batches.
     * JDBI PreparedBatch requires uniform null/non-null patterns across rows
     * for TIMESTAMP columns (PENDING has completedAt=null, SKIPPED has non-null).
     */
    private fun insertMixedTaskBatch(handle: Handle, tasks: List<Task>) {
        val (skipped, pending) = tasks.partition { it.completedAt != null }
        if (pending.isNotEmpty()) taskRepo.insertBatchWithHandle(handle, pending)
        if (skipped.isNotEmpty()) taskRepo.insertBatchWithHandle(handle, skipped)
    }

    // -- DAG navigation helpers ----------------------------------------------

    /**
     * Returns the [SequenceInfo] entries for all successor activities of the given sequence.
     * Strips the `.__parallel__` suffix to find the original scatter activity's successors.
     * Returns only SCATTER or LINEAR entries (not PARALLEL) to avoid double-dispatching.
     */
    private fun successorsOf(
        seqInfo: SequenceInfo,
        seqByName: Map<String, SequenceInfo>,
        definition: WorkflowDefinition,
    ): List<SequenceInfo> {
        val actName = seqInfo.activityName.removeSuffix(".__parallel__")
        val activity = definition.activities[actName] ?: return emptyList()
        return activity.successors.mapNotNull { edge ->
            seqByName[edge.target]
        }.distinctBy { it.sequenceNumber }
    }

    /**
     * Checks whether any predecessor has a DEFAULT_BRANCH edge to the given [successor].
     * Used in force-dispatch mode (BEST_EFFORT fallthrough) where edge label matching
     * against task results is bypassed.
     */
    private fun hasDefaultBranchEdge(
        successor: SequenceInfo,
        definition: WorkflowDefinition,
    ): Boolean {
        val targetActName = successor.activityName
        for ((_, predActivity) in definition.activities) {
            if (predActivity.successors.any { it.target == targetActName && it.label == DEFAULT_BRANCH }) {
                return true
            }
        }
        return false
    }

    /**
     * Checks whether any predecessor edge to [successor] is "taken" based on
     * predecessor task results and failure policies.
     *
     * An edge is taken when:
     * - Predecessor COMPLETED and edge label is DEFAULT_BRANCH, or
     * - Predecessor COMPLETED and resultJson.branch matches the edge label, or
     * - Predecessor FAILED with BEST_EFFORT policy and edge label is DEFAULT_BRANCH
     */
    private fun isAnyEdgeTaken(
        tasksBySeq: Map<Int, List<Task>>,
        successor: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        definition: WorkflowDefinition,
    ): Boolean {
        val targetActName = successor.activityName
        for ((predActName, predActivity) in definition.activities) {
            val edgesToTarget = predActivity.successors.filter { it.target == targetActName }
            if (edgesToTarget.isEmpty()) continue

            // Find the output sequence of this predecessor (PARALLEL for fan-out, LINEAR otherwise)
            val predOutputSeq = sequenceMap.values
                .firstOrNull { si ->
                    val name = si.activityName.removeSuffix(".__parallel__")
                    name == predActName && (si.phaseType == PhaseType.PARALLEL || si.phaseType == PhaseType.LINEAR)
                }?.sequenceNumber ?: continue

            val predTasks = tasksBySeq[predOutputSeq] ?: continue
            for (predTask in predTasks) {
                for (edge in edgesToTarget) {
                    if (isEdgeTaken(predTask, edge.label, predActivity.failurePolicy)) return true
                }
            }
        }
        return false
    }

    /**
     * Determines if a single edge is "taken" given a predecessor task's terminal state.
     */
    private fun isEdgeTaken(task: Task, edgeLabel: String, predFailurePolicy: FailurePolicy): Boolean {
        if (!task.status.isTerminal) return false
        // BEST_EFFORT failed predecessor: treat as DEFAULT_BRANCH taken
        if (task.status == TaskStatus.FAILED && predFailurePolicy == FailurePolicy.BEST_EFFORT) {
            return edgeLabel == DEFAULT_BRANCH
        }
        if (task.status != TaskStatus.COMPLETED) return false
        if (edgeLabel == DEFAULT_BRANCH) return true

        // Conditional edge: check resultJson for {"branch": "<label>"} match
        val result = task.resultJson ?: return false
        return try {
            val map = objectMapper.readValue<Map<String, Any>>(result)
            map["branch"]?.toString() == edgeLabel
        } catch (_: Exception) {
            false
        }
    }

    /**
     * Evaluates whether a fan-out join policy is satisfied given the failure count.
     */
    private fun evaluateJoinPolicy(joinPolicy: JoinPolicy, completedCount: Int, totalCount: Int): Boolean =
        when (joinPolicy) {
            is JoinPolicy.All -> completedCount == totalCount
            is JoinPolicy.Threshold -> completedCount >= joinPolicy.n
            is JoinPolicy.Percentage -> {
                val pct = if (totalCount > 0) (completedCount * 100) / totalCount else 0
                pct >= joinPolicy.pct
            }
        }
}

private const val MAX_CAS_RETRIES = 10

/** Thrown inside a transaction to trigger the CAS retry loop. */
private class RetryableException(msg: String) : RuntimeException(msg)
