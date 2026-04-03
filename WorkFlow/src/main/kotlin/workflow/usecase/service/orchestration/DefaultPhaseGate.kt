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
        var signalQueues: List<String> = emptyList()

        var attempts = 0
        while (true) {
            try {
                jdbi.inTransactionSuspend<Unit, Exception> { handle ->
                    // Step 1: Update task to terminal status
                    val updated = taskRepo.updateStatusWithHandle(
                        handle, taskId, status, resultJson, claimedBy, claimedAt,
                    )
                    if (!updated) return@inTransactionSuspend

                    // Step 2: Barrier probe -- are all tasks at this sequence terminal?
                    val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
                    if (nonTerminal > 0) return@inTransactionSuspend

                    val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                        ?: throw IllegalStateException("Workflow not found: $workflowId")
                    if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

                    val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
                    val sequenceMap = buildSequenceMap(definition)
                    val seqInfo = sequenceMap[sequenceNumber]
                        ?: throw IllegalStateException("Seq $sequenceNumber not in definition for $workflowId")

                    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
                    val signalQueueSet = mutableSetOf<String>()
                    val completionCheckSeqs = mutableSetOf<Int>()
                    // When true, successor eval treats DEFAULT_BRANCH edges as unconditionally taken
                    var forceDefaultBranch = false

                    // Step 3a: SCATTER failure -- apply scatter's failurePolicy
                    if (seqInfo.phaseType == PhaseType.SCATTER && status != TaskStatus.COMPLETED) {
                        if (seqInfo.activity.failurePolicy == FailurePolicy.ABORT) {
                            val statusUpdated = workflowRepo.updateStatusWithHandle(
                                handle, workflowId, WorkflowStatus.FAILED, WorkflowStatus.RUNNING,
                            )
                            if (statusUpdated) taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
                            return@inTransactionSuspend
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

                        val casWon = workflowRepo.casVersionWithHandle(handle, workflowId, workflow.version)
                        if (!casWon) {
                            log.debug("CAS lost on SCATTER for workflow {}", workflowId)
                            throw RetryableException("CAS loss")
                        }
                        signalQueues = signalQueueSet.toList()
                        return@inTransactionSuspend
                    }

                    // Step 3c: PARALLEL phase -- evaluate JoinPolicy before successor dispatch
                    if (seqInfo.phaseType == PhaseType.PARALLEL) {
                        val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
                        val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)
                        val scatterActName = seqInfo.activityName.removeSuffix(".__parallel__")
                        val scatterActivity = definition.activities[scatterActName]
                        val joinPolicy = scatterActivity?.fanOut?.joinPolicy ?: JoinPolicy.All
                        // seqInfo.activity.failurePolicy carries the scatter activity's policy
                        // (set by buildSequenceMap), which governs join failure behavior
                        val joinPassed = evaluateJoinPolicy(joinPolicy, failedCount, totalCount)
                        if (!joinPassed) {
                            if (seqInfo.activity.failurePolicy == FailurePolicy.ABORT) {
                                val statusUpdated = workflowRepo.updateStatusWithHandle(
                                    handle, workflowId, WorkflowStatus.FAILED, WorkflowStatus.RUNNING,
                                )
                                if (statusUpdated) taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
                                return@inTransactionSuspend
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
                        val statusUpdated = workflowRepo.updateStatusWithHandle(
                            handle, workflowId, WorkflowStatus.FAILED, WorkflowStatus.RUNNING,
                        )
                        if (statusUpdated) taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
                        return@inTransactionSuspend
                    }

                    // Step 4: Successor evaluation
                    val evalQueue = ArrayDeque<SequenceInfo>()
                    evalQueue += successorsOf(seqInfo, sequenceMap, definition)

                    while (evalQueue.isNotEmpty()) {
                        val successor = evalQueue.removeFirst()
                        val sSeq = successor.sequenceNumber

                        // a. Dispatch guard: task already exists at this sequence
                        if (taskRepo.countTotalWithHandle(handle, workflowId, sSeq) > 0) continue

                        // b. Predecessor gate: all predecessor sequences must be terminal
                        val allPredTerminal = successor.predecessorSequences.all { predSeq ->
                            taskRepo.countNonTerminalWithHandle(handle, workflowId, predSeq) == 0
                        }
                        if (!allPredTerminal) continue

                        // c. Fate decision: check if any edge to this successor is "taken"
                        val edgeTaken = if (forceDefaultBranch) {
                            // When force-dispatching (BEST_EFFORT fallthrough), all DEFAULT_BRANCH
                            // edges are taken. Check if any predecessor has a DEFAULT_BRANCH edge.
                            hasDefaultBranchEdge(successor, definition)
                        } else {
                            isAnyEdgeTaken(handle, workflowId, successor, sequenceMap, definition)
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
                                evalQueue += successorsOf(successor, sequenceMap, definition)
                            }
                        }
                    }

                    // Also add completed terminal activity to completion check
                    if (seqInfo.activity.isTerminal) {
                        completionCheckSeqs += sequenceNumber
                    }

                    // Step 5: Completion check
                    if (completionCheckSeqs.isNotEmpty()) {
                        val globalNonTerminal = countGlobalNonTerminal(handle, workflowId)
                        if (globalNonTerminal == 0) {
                            workflowRepo.updateStatusWithHandle(
                                handle, workflowId, WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING,
                            )
                            signalQueues = emptyList()
                            return@inTransactionSuspend
                        }
                    }

                    // Step 6: CAS guard
                    val casWon = workflowRepo.casVersionWithHandle(handle, workflowId, workflow.version)
                    if (!casWon) {
                        log.debug("CAS lost for workflow {} at seq {}", workflowId, sequenceNumber)
                        throw RetryableException("CAS loss")
                    }

                    signalQueues = signalQueueSet.toList()
                }
                break
            } catch (e: RetryableException) {
                if (++attempts >= 10) {
                    throw IllegalStateException("CAS retry exhausted for $workflowId", e)
                }
                log.debug("CAS retry {} for workflow {}", attempts, workflowId)
            }
        }

        // Step 7: Signal queues after commit
        signalQueues.forEach { notifier.signal(it) }
    }

    override suspend fun recoverStuckWorkflow(workflowId: String) {
        // Implemented in Plan 5 (dag-p5-watchdog-sweeper)
        throw UnsupportedOperationException("recoverStuckWorkflow implemented in Plan 5")
    }

    // -- Private helpers -------------------------------------------------------

    /**
     * Returns the [SequenceInfo] entries for all successor activities of the given sequence.
     * Strips the `.__parallel__` suffix to find the original scatter activity's successors.
     * Returns only SCATTER or LINEAR entries (not PARALLEL) to avoid double-dispatching.
     */
    private fun successorsOf(
        seqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        definition: WorkflowDefinition,
    ): List<SequenceInfo> {
        val actName = seqInfo.activityName.removeSuffix(".__parallel__")
        val activity = definition.activities[actName] ?: return emptyList()
        return activity.successors.mapNotNull { edge ->
            sequenceMap.values.firstOrNull {
                it.activityName == edge.target && it.phaseType != PhaseType.PARALLEL
            }
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
        handle: Handle,
        workflowId: String,
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

            val predTasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, predOutputSeq)
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
    private fun evaluateJoinPolicy(joinPolicy: JoinPolicy, failedCount: Int, totalCount: Int): Boolean {
        val succeededCount = totalCount - failedCount
        return when (joinPolicy) {
            is JoinPolicy.All -> failedCount == 0
            is JoinPolicy.Threshold -> succeededCount >= joinPolicy.n
            is JoinPolicy.Percentage -> {
                val pct = if (totalCount > 0) (succeededCount * 100) / totalCount else 0
                pct >= joinPolicy.pct
            }
        }
    }

    /**
     * Counts all non-terminal tasks across the entire workflow (not sequence-scoped).
     */
    private fun countGlobalNonTerminal(handle: Handle, workflowId: String): Int =
        handle.createQuery(
            """
            SELECT COUNT(*) FROM task
            WHERE workflow_id = :wfId
              AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
            """,
        )
            .bind("wfId", workflowId)
            .mapTo(Int::class.java)
            .one()
}

/** Thrown inside a transaction to trigger the CAS retry loop. */
private class RetryableException(msg: String) : RuntimeException(msg)
