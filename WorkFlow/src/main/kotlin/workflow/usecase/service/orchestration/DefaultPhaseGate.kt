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
 * 3. SCATTER/PARALLEL special cases: expand fan-out or evaluate join policy
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
            // Step 1: Update task to terminal status
            val updated = taskRepo.updateStatusWithHandle(
                handle, taskId, status, resultJson, claimedBy, claimedAt,
            )
            if (!updated) return@withCasRetry emptyList()

            // Step 2: Barrier probe -- are all tasks at this sequence terminal?
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@withCasRetry emptyList()

            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
            if (workflow.status != WorkflowStatus.RUNNING) return@withCasRetry emptyList()

            val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
            val sequenceMap = buildSequenceMap(definition)
            val seqByName = buildSeqByName(sequenceMap)
            val seqInfo = sequenceMap[sequenceNumber]
                ?: throw IllegalStateException("Seq $sequenceNumber not in definition for $workflowId")

            // Pre-fetch all task counts and tasks for this workflow (eliminates N+1)
            val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
            val tasksBySeq = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
                .groupBy { it.sequenceNumber }

            val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
            val signalQueueSet = mutableSetOf<String>()
            val completionCheckSeqs = mutableSetOf<Int>()
            // When true, successor eval treats DEFAULT_BRANCH edges as unconditionally taken
            var forceDefaultBranch = false

            // Step 3a: SCATTER failure -- apply scatter's failurePolicy
            if (seqInfo.phaseType == PhaseType.SCATTER && status != TaskStatus.COMPLETED) {
                if (seqInfo.activity.failurePolicy == FailurePolicy.ABORT) {
                    abortWorkflow(handle, workflowId)
                    return@withCasRetry emptyList()
                }
                // BEST_EFFORT: no parallel tasks; fall through to successor eval
                forceDefaultBranch = true
            }

            // Step 3b: SCATTER completed -- expand into parallel tasks
            if (seqInfo.phaseType == PhaseType.SCATTER && status == TaskStatus.COMPLETED) {
                val items: List<String> = objectMapper.readValue(
                    resultJson ?: throw IllegalStateException(
                        "SCATTER phase requires scatter result for workflow $workflowId",
                    ),
                )
                require(items.isNotEmpty()) {
                    "Fan-out produced 0 items for workflow $workflowId"
                }
                val parallelSeq = sequenceNumber + 1
                val parallelInfo = sequenceMap[parallelSeq]!!
                val parallelTasks = items.map {
                    createTaskForActivity(
                        workflowId, parallelInfo.activityName, parallelSeq,
                        parallelInfo.activity, now, item = it,
                    )
                }
                taskRepo.insertBatchWithHandle(handle, parallelTasks)
                signalQueueSet += parallelInfo.activity.queue

                requireCasWin(handle, workflowId, workflow.version)
                return@withCasRetry signalQueueSet.toList()
            }

            // Step 3c: PARALLEL phase -- evaluate JoinPolicy before successor dispatch
            if (seqInfo.phaseType == PhaseType.PARALLEL) {
                val counts = allCounts[sequenceNumber] ?: TaskStatusCounts(0, 0, 0, 0)
                val completedCount = counts.completed
                val totalCount = counts.total
                val scatterActName = seqInfo.activityName.removeSuffix(".__parallel__")
                val scatterActivity = definition.activities[scatterActName]
                val joinPolicy = scatterActivity?.fanOut?.joinPolicy ?: JoinPolicy.All
                val joinPassed = evaluateJoinPolicy(joinPolicy, completedCount, totalCount)
                if (!joinPassed) {
                    if (seqInfo.activity.failurePolicy == FailurePolicy.ABORT) {
                        abortWorkflow(handle, workflowId)
                        return@withCasRetry emptyList()
                    }
                    // BEST_EFFORT: force-dispatch unconditional successors
                    forceDefaultBranch = true
                }
            }

            // Step 3d: LINEAR failure with ABORT -- workflow FAILED
            // (skipped when forceDefaultBranch is set, e.g. SCATTER/PARALLEL BEST_EFFORT)
            if (!forceDefaultBranch
                && seqInfo.phaseType == PhaseType.LINEAR
                && status != TaskStatus.COMPLETED
                && status != TaskStatus.SKIPPED
                && seqInfo.activity.failurePolicy == FailurePolicy.ABORT
            ) {
                abortWorkflow(handle, workflowId)
                return@withCasRetry emptyList()
            }

            // Step 4: Successor evaluation
            val evalQueue = ArrayDeque<SequenceInfo>()
            evalQueue += successorsOf(seqInfo, seqByName, definition)

            while (evalQueue.isNotEmpty()) {
                val successor = evalQueue.removeFirst()
                val sSeq = successor.sequenceNumber

                // a. Dispatch guard: task already exists at this sequence
                //    (safe with pre-fetched data: DAG traversal evaluates each sequence at most once)
                if ((allCounts[sSeq]?.total ?: 0) > 0) continue

                // b. Predecessor gate: all predecessor sequences must be terminal
                val allPredTerminal = successor.predecessorSequences.all { predSeq ->
                    (allCounts[predSeq]?.nonTerminal ?: 0) == 0
                }
                if (!allPredTerminal) continue

                // c. Fate decision: check if any edge to this successor is "taken"
                val edgeTaken = if (forceDefaultBranch) {
                    hasDefaultBranchEdge(successor, definition)
                } else {
                    isAnyEdgeTaken(tasksBySeq, successor, sequenceMap, definition)
                }

                if (edgeTaken) {
                    val task = createTaskForActivity(
                        workflowId, successor.activityName, sSeq, successor.activity, now,
                    )
                    taskRepo.insertBatchWithHandle(handle, listOf(task))
                    signalQueueSet += successor.activity.queue
                } else {
                    val skipped = createSkippedTaskForActivity(
                        workflowId, successor.activityName, sSeq, successor.activity, now,
                    )
                    taskRepo.insertBatchWithHandle(handle, listOf(skipped))

                    // If skipping a SCATTER activity, also skip its companion PARALLEL sequence
                    if (successor.phaseType == PhaseType.SCATTER) {
                        val parallelSeq = sSeq + 1
                        val parallelInfo = sequenceMap[parallelSeq]
                        if (parallelInfo != null && parallelInfo.phaseType == PhaseType.PARALLEL) {
                            val parallelSkipped = createSkippedTaskForActivity(
                                workflowId, parallelInfo.activityName, parallelSeq,
                                parallelInfo.activity, now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(parallelSkipped))
                        }
                    }

                    if (successor.activity.isTerminal) {
                        completionCheckSeqs += sSeq
                    } else {
                        // Cascade skip: add this successor's successors to the eval queue
                        evalQueue += successorsOf(successor, seqByName, definition)
                    }
                }
            }

            // Also add completed terminal activity to completion check
            if (seqInfo.activity.isTerminal) {
                completionCheckSeqs += sequenceNumber
            }

            // Step 5: Completion check
            if (completionCheckSeqs.isNotEmpty()) {
                val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
                if (globalNonTerminal == 0) {
                    workflowRepo.updateStatusWithHandle(
                        handle, workflowId, WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING,
                    )
                    return@withCasRetry emptyList()
                }
            }

            // Step 6: CAS guard
            requireCasWin(handle, workflowId, workflow.version)
            signalQueueSet.toList()
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

            val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
            val sequenceMap = buildSequenceMap(definition)
            val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
            val signalQueueSet = mutableSetOf<String>()

            // Pre-fetch all task counts and tasks (eliminates N+1)
            val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
            val tasksBySeq = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
                .groupBy { it.sequenceNumber }

            for ((seq, seqInfo) in sequenceMap.entries.sortedBy { it.key }) {
                if ((allCounts[seq]?.total ?: 0) > 0) continue

                val allPredTerminal = seqInfo.predecessorSequences.isEmpty() ||
                    seqInfo.predecessorSequences.all { predSeq ->
                        (allCounts[predSeq]?.total ?: 0) > 0 &&
                            (allCounts[predSeq]?.nonTerminal ?: 0) == 0
                    }
                if (!allPredTerminal) continue

                when (seqInfo.phaseType) {
                    PhaseType.SCATTER -> {
                        val task = createTaskForActivity(
                            workflowId, seqInfo.activityName, seq, seqInfo.activity, now,
                        )
                        taskRepo.insertBatchWithHandle(handle, listOf(task))
                        signalQueueSet += seqInfo.activity.queue
                    }
                    PhaseType.PARALLEL -> continue
                    PhaseType.LINEAR -> {
                        val edgeTaken = isAnyEdgeTaken(tasksBySeq, seqInfo, sequenceMap, definition)
                        if (edgeTaken || seqInfo.predecessorSequences.isEmpty()) {
                            val task = createTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(task))
                            signalQueueSet += seqInfo.activity.queue
                        } else {
                            val skipped = createSkippedTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(skipped))
                        }
                    }
                }
            }

            // Completion / failure check -- must query DB to see newly inserted tasks
            val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
            if (globalNonTerminal == 0) {
                // allCounts is safe: recovery only inserts PENDING/SKIPPED tasks, never FAILED
                val abortFailure = sequenceMap.entries.any { (seq, seqInfo) ->
                    seqInfo.phaseType != PhaseType.PARALLEL &&
                        seqInfo.activity.failurePolicy == FailurePolicy.ABORT &&
                        (allCounts[seq]?.failed ?: 0) > 0
                }
                val terminalStatus = if (abortFailure) WorkflowStatus.FAILED else WorkflowStatus.COMPLETED
                workflowRepo.updateStatusWithHandle(
                    handle, workflowId, terminalStatus, WorkflowStatus.RUNNING,
                )
                return@withCasRetry emptyList()
            }

            // No new work dispatched -- skip CAS bump
            if (signalQueueSet.isEmpty()) return@withCasRetry emptyList()

            requireCasWin(handle, workflowId, workflow.version)
            signalQueueSet.toList()
        }

        signalQueues.forEach { notifier.signal(it) }
    }

    // -- Transaction / CAS helpers --------------------------------------------

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

    // -- DAG navigation helpers -----------------------------------------------

    private fun buildSeqByName(sequenceMap: Map<Int, SequenceInfo>): Map<String, SequenceInfo> =
        sequenceMap.values
            .filter { it.phaseType != PhaseType.PARALLEL }
            .associateBy { it.activityName }

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
