package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.TaskCompletionEvent
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
import java.util.concurrent.ConcurrentHashMap

/**
 * DAG-aware phase gate that evaluates successor activities after each task completion.
 *
 * Uses a two-transaction design to eliminate the READ COMMITTED visibility gap:
 * - TX1 commits the task status update so it is visible to all concurrent readers.
 * - TX2 performs the fast-path probe (now accurate), acquires the workflow row lock
 *   only when nonTerminal == 0, and routes the DAG.
 *
 * Because TX1 commits before TX2 counts, the fast-path probe is accurate and no
 * threshold is needed — a simple nonTerminal > 0 check short-circuits the vast
 * majority of calls without acquiring a lock. All routing decisions are delegated
 * to [DagRouter] (pure functions).
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

    private data class CachedDefinition(
        val definition: WorkflowDefinition,
        val sequenceMap: Map<Int, SequenceInfo>,
        val seqByName: Map<String, SequenceInfo>,
    )

    private val definitionCache = ConcurrentHashMap<String, CachedDefinition>()

    override suspend fun onTaskCompleted(event: TaskCompletionEvent) {
        val (taskId, workflowId, sequenceNumber, status, resultJson, claimedBy, claimedAt, fanOutPayloadsJson) = event
        // TX1: Commit task status update — including fan-out payloads — so both are visible to all concurrent readers.
        val updated =
            jdbi.inTransactionSuspend<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt, fanOutPayloadsJson)
            }
        if (!updated) return

        // TX2: Fast-path probe + lock + route.
        // The count query now sees all committed TX1s from concurrent completers.
        val signalQueues =
            jdbi.inTransactionSuspend<List<String>, Exception> { handle ->
                // Fast-path probe (no lock) — accurate because TX1 committed
                val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
                if (nonTerminal > 0) return@inTransactionSuspend emptyList()

                // All tasks at this sequence are terminal — acquire workflow lock to serialize DAG routing
                val workflow =
                    workflowRepo.findByIdForUpdate(handle, workflowId)
                        ?: throw IllegalStateException("Workflow not found: $workflowId")
                if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend emptyList()

                // Recount under lock to handle concurrent completers who also saw nonTerminal == 0
                val confirmedNonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
                if (confirmedNonTerminal > 0) return@inTransactionSuspend emptyList()

                // Build snapshot and route
                val snapshot = buildSnapshot(handle, workflowId, workflow.definitionJson)
                val seqInfo =
                    snapshot.sequenceMap[sequenceNumber]
                        ?: throw IllegalStateException("Seq $sequenceNumber not in definition for $workflowId")

                val scatterItems =
                    if (seqInfo.phaseType == PhaseType.SCATTER && status == TaskStatus.COMPLETED) {
                        val jsonStr =
                            fanOutPayloadsJson ?: throw IllegalStateException(
                                "SCATTER phase requires scatter result for workflow $workflowId",
                            )

                        val rootNode = objectMapper.readTree(jsonStr)
                        if (!rootNode.isArray) {
                            throw IllegalStateException("SCATTER phase result must be a JSON array")
                        }

                        rootNode.map { node ->
                            if (node.isTextual) node.asText() else node.toString()
                        }
                    } else {
                        null
                    }

                val decision = resolvePhaseDecision(snapshot, seqInfo, status, scatterItems)

                when (decision) {
                    PhaseDecision.Abort -> {
                        abortWorkflow(handle, workflowId)
                        return@inTransactionSuspend emptyList()
                    }

                    is PhaseDecision.ScatterExpand -> {
                        // Live recount under lock: recoverStuckWorkflow may have committed PARALLEL tasks
                        // between snapshot build and now. snapshot.allCounts is stale under READ COMMITTED.
                        // Use total count (not non-terminal) to catch already-terminal PARALLEL tasks too.
                        val existingParallelCount =
                            taskRepo.countTotalBySequenceWithHandle(handle, workflowId, decision.parallelInfo.sequenceNumber)
                        if (existingParallelCount > 0) return@inTransactionSuspend emptyList()
                        val parallelTasks =
                            decision.items.map {
                                createTaskForActivity(
                                    workflowId,
                                    decision.parallelInfo.activityName,
                                    decision.parallelInfo.sequenceNumber,
                                    decision.parallelInfo.activity,
                                    snapshot.now,
                                    taskPayload = assembleChildItem(it, resultJson),
                                )
                            }
                        taskRepo.insertBatchWithHandle(handle, parallelTasks)
                        workflowRepo.incrementVersionWithHandle(handle, workflowId)
                        return@inTransactionSuspend listOf(decision.parallelInfo.activity.queue)
                    }

                    PhaseDecision.Normal -> { /* fall through to successor evaluation */ }
                }

                val result = dispatchSuccessors(snapshot, seqInfo)

                // No-op guard: another concurrent completer already routed this sequence
                if (result.tasksToInsert.isEmpty()) {
                    if (seqInfo.activity.isTerminal) {
                        val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
                        if (globalNonTerminal == 0) {
                            workflowRepo.updateStatusWithHandle(
                                handle,
                                workflowId,
                                WorkflowStatus.COMPLETED,
                                WorkflowStatus.RUNNING,
                            )
                            definitionCache.remove(workflowId)
                        }
                    }
                    return@inTransactionSuspend emptyList()
                }

                taskRepo.insertBatchWithHandle(handle, result.tasksToInsert)

                // Check global completion
                val checkCompletion = result.hasTerminalCompletion || seqInfo.activity.isTerminal
                if (checkCompletion) {
                    val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
                    if (globalNonTerminal == 0) {
                        workflowRepo.updateStatusWithHandle(
                            handle,
                            workflowId,
                            WorkflowStatus.COMPLETED,
                            WorkflowStatus.RUNNING,
                        )
                        definitionCache.remove(workflowId)
                        return@inTransactionSuspend emptyList()
                    }
                }

                workflowRepo.incrementVersionWithHandle(handle, workflowId)
                result.signalQueues.toList()
            }

        notifier.signalAll(signalQueues, log)
    }

    override suspend fun recoverStuckWorkflow(workflowId: String) {
        val signalQueues =
            jdbi.inTransactionSuspend<List<String>, Exception> { handle ->
                val workflow =
                    workflowRepo.findByIdForUpdate(handle, workflowId)
                        ?: run {
                            log.warn("Workflow not found during recovery: {}", workflowId)
                            return@inTransactionSuspend emptyList()
                        }
                if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend emptyList()

                val snapshot = buildSnapshot(handle, workflowId, workflow.definitionJson)
                val signalQueueSet = mutableSetOf<String>()

                for ((seq, seqInfo) in snapshot.sequenceMap.entries.sortedBy { it.key }) {
                    if ((snapshot.allCounts[seq]?.total ?: 0) > 0) continue

                    val allPredTerminal =
                        seqInfo.predecessorSequences.isEmpty() ||
                            seqInfo.predecessorSequences.all { predSeq ->
                                (snapshot.allCounts[predSeq]?.total ?: 0) > 0 &&
                                    (snapshot.allCounts[predSeq]?.nonTerminal ?: 0) == 0
                            }
                    if (!allPredTerminal) continue

                    when (seqInfo.phaseType) {
                        PhaseType.SCATTER -> {
                            val task =
                                createTaskForActivity(
                                    workflowId,
                                    seqInfo.activityName,
                                    seq,
                                    seqInfo.activity,
                                    snapshot.now,
                                )
                            taskRepo.insertBatchWithHandle(handle, listOf(task))
                            signalQueueSet += seqInfo.activity.queue
                        }

                        PhaseType.PARALLEL -> {
                            // SCATTER predecessor is always exactly one sequence.
                            val scatterSeq = seqInfo.predecessorSequences.firstOrNull() ?: continue
                            val scatterTask =
                                snapshot.tasksBySeq[scatterSeq]
                                    ?.firstOrNull { it.status == TaskStatus.COMPLETED }
                                    ?: continue

                            val storedFanOutJson = scatterTask.fanOutPayloadsJson ?: run {
                                log.warn(
                                    "SCATTER task {} has no stored fan-out payloads; skipping PARALLEL recovery for workflow {}",
                                    scatterTask.id,
                                    workflowId,
                                )
                                continue
                            }
                            val itemList: List<String> =
                                try {
                                    objectMapper.readValue(storedFanOutJson)
                                } catch (e: Exception) {
                                    log.warn(
                                        "recoverStuckWorkflow: corrupt fan-out payloads JSON for workflow={} seq={} — skipping",
                                        workflowId,
                                        seq,
                                        e,
                                    )
                                    continue
                                }

                            val parallelTasks =
                                itemList.map { rawItem ->
                                    createTaskForActivity(
                                        workflowId,
                                        seqInfo.activityName,
                                        seq,
                                        seqInfo.activity,
                                        snapshot.now,
                                        taskPayload = assembleChildItem(rawItem, scatterTask.resultJson),
                                    )
                                }
                            taskRepo.insertBatchWithHandle(handle, parallelTasks)
                            signalQueueSet += seqInfo.activity.queue
                        }

                        PhaseType.LINEAR -> {
                            val edgeTaken =
                                isAnyEdgeTaken(
                                    snapshot.tasksBySeq,
                                    snapshot.resultBranches,
                                    seqInfo,
                                    snapshot.sequenceMap,
                                    snapshot.definition,
                                )
                            if (edgeTaken || seqInfo.predecessorSequences.isEmpty()) {
                                val task =
                                    createTaskForActivity(
                                        workflowId,
                                        seqInfo.activityName,
                                        seq,
                                        seqInfo.activity,
                                        snapshot.now,
                                    )
                                taskRepo.insertBatchWithHandle(handle, listOf(task))
                                signalQueueSet += seqInfo.activity.queue
                            } else {
                                val skipped =
                                    createSkippedTaskForActivity(
                                        workflowId,
                                        seqInfo.activityName,
                                        seq,
                                        seqInfo.activity,
                                        snapshot.now,
                                    )
                                taskRepo.insertBatchWithHandle(handle, listOf(skipped))
                            }
                        }
                    }
                }

                val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
                if (globalNonTerminal == 0) {
                    val hasFailure =
                        snapshot.sequenceMap.entries.any { (seq, seqInfo) ->
                            seqInfo.phaseType != PhaseType.PARALLEL &&
                                (snapshot.allCounts[seq]?.failed ?: 0) > 0
                        }
                    val terminalStatus = if (hasFailure) WorkflowStatus.FAILED else WorkflowStatus.COMPLETED
                    workflowRepo.updateStatusWithHandle(
                        handle,
                        workflowId,
                        terminalStatus,
                        WorkflowStatus.RUNNING,
                    )
                    definitionCache.remove(workflowId)
                    return@inTransactionSuspend emptyList()
                }

                if (signalQueueSet.isEmpty()) return@inTransactionSuspend emptyList()

                workflowRepo.incrementVersionWithHandle(handle, workflowId)
                signalQueueSet.toList()
            }

        notifier.signalAll(signalQueues, log)
    }

    // -- Snapshot builder ---------------------------------------------------------

    private fun buildSnapshot(
        handle: Handle,
        workflowId: String,
        definitionJson: String,
    ): GateSnapshot {
        val cached = definitionCache.computeIfAbsent(workflowId) { _ ->
            val definition = objectMapper.readValue<WorkflowDefinition>(definitionJson)
            val sequenceMap = buildSequenceMap(definition)
            val seqByName = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
            CachedDefinition(definition, sequenceMap, seqByName)
        }
        val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
        val allTasks = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
        val tasksBySeq = allTasks.groupBy { it.sequenceNumber }
        val resultBranches =
            allTasks.associate { task ->
                task.id to
                    task.resultJson?.let { json ->
                        try {
                            objectMapper.readValue<Map<String, Any>>(json)["branch"]?.toString()
                        } catch (_: Exception) {
                            null
                        }
                    }
            }
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

        return GateSnapshot(
            workflowId,
            cached.definition,
            cached.sequenceMap,
            cached.seqByName,
            allCounts,
            tasksBySeq,
            resultBranches,
            now,
        )
    }

    // -- Transaction helpers ------------------------------------------------------

    private fun abortWorkflow(
        handle: Handle,
        workflowId: String,
    ) {
        val statusUpdated =
            workflowRepo.updateStatusWithHandle(
                handle,
                workflowId,
                WorkflowStatus.FAILED,
                WorkflowStatus.RUNNING,
            )
        if (statusUpdated) {
            taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
            definitionCache.remove(workflowId)
        }
    }

    /**
     * Assembles a PARALLEL child task's item string by merging two JSON objects.
     *
     * [rawItem] must be a JSON object string produced by the scatter handler. [scatterResultJson]
     * carries shared context for all children (e.g. a batch token). Fields from [scatterResultJson]
     * form the base; fields from [rawItem] are applied on top so item-specific values always win
     * on collision.
     *
     * If either argument is null, blank, or not a JSON object, [rawItem] is returned as-is.
     * This preserves backward compatibility for workflows where children carry self-contained
     * item strings or where no shared scatter result exists.
     */
    private fun assembleChildItem(
        rawItem: String,
        scatterResultJson: String?,
    ): String {
        if (scatterResultJson.isNullOrBlank()) return rawItem
        val resultNode =
            try {
                objectMapper.readTree(scatterResultJson)
            } catch (_: Exception) {
                return rawItem
            }
        if (!resultNode.isObject || resultNode.size() == 0) return rawItem
        val itemNode =
            try {
                objectMapper.readTree(rawItem)
            } catch (_: Exception) {
                return rawItem
            }
        if (!itemNode.isObject) {
            log.warn("assembleChildItem: rawItem is not a JSON object — scatter context will not be merged: {}", rawItem.take(200))
            return rawItem
        }
        val assembled = objectMapper.createObjectNode()
        resultNode.fields().forEach { (k, v) -> assembled.set<JsonNode>(k, v) } // shared context base
        itemNode.fields().forEach { (k, v) -> assembled.set<JsonNode>(k, v) }   // item fields win
        return assembled.toString()
    }

}
