package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import com.workflow.extension.inTransactionSuspend
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@ApplicationScoped
class BarrierService(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
) {

    private val log = LoggerFactory.getLogger(BarrierService::class.java)

    suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
    ) {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            // 1. Self-update
            val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
            if (!updated) return@inTransactionSuspend  // already finalized by another actor

            // 2. Lock-free probe
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            // 3. Load workflow and compute sequence metadata
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend
            val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
            val sequenceMap = buildSequenceMap(definition)
            val seqInfo = sequenceMap[sequenceNumber]
                ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow $workflowId")

            // 4. Evaluate outcome
            val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
            val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)
            val outcomeSuccess = evaluateOutcome(seqInfo.phaseType, seqInfo.activity, failedCount, totalCount)

            // 5. CAS advance
            val nextSequence = sequenceNumber + 1
            val casWon = workflowRepo.casAdvanceWithHandle(
                handle, workflowId, sequenceNumber, nextSequence, workflow.version,
            )
            if (!casWon) {
                log.debug("CAS lost for workflow {} at sequence {}", workflowId, sequenceNumber)
                return@inTransactionSuspend
            }

            // 6. Advance workflow (CAS winner only)
            advanceWorkflow(handle, workflow, sequenceMap, seqInfo, outcomeSuccess, resultJson)
        }
    }

    /**
     * Recover a stuck workflow detected by the [Sweeper].
     *
     * Same evaluate → CAS → advance logic as [onTaskCompleted], but without
     * the self-update step (all tasks are already terminal). Re-reads the
     * workflow and re-probes inside the transaction for TOCTOU safety.
     */
    internal suspend fun recoverStuckWorkflow(workflowId: String) {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: run {
                    log.warn("Workflow not found during recovery: {}", workflowId)
                    return@inTransactionSuspend
                }
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

            val seq = workflow.currentSequence

            // Re-probe: all tasks must be terminal (TOCTOU safety vs findStuck)
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, seq)
            if (nonTerminal > 0) return@inTransactionSuspend

            val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
            val sequenceMap = buildSequenceMap(definition)
            val seqInfo = sequenceMap[seq] ?: return@inTransactionSuspend

            val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, seq)
            val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, seq)
            val outcomeSuccess = evaluateOutcome(seqInfo.phaseType, seqInfo.activity, failedCount, totalCount)

            val nextSeq = seq + 1
            val casWon = workflowRepo.casAdvanceWithHandle(handle, workflowId, seq, nextSeq, workflow.version)
            if (!casWon) {
                log.debug("Sweeper CAS lost for workflow {} at sequence {}", workflowId, seq)
                return@inTransactionSuspend
            }

            // Resolve payload: LINEAR/SCATTER → completed task's result; PARALLEL → null
            val payload = when (seqInfo.phaseType) {
                PhaseType.LINEAR, PhaseType.SCATTER -> {
                    taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, seq)
                        .firstOrNull { it.status == TaskStatus.COMPLETED }?.resultJson
                }
                PhaseType.PARALLEL -> null
            }

            advanceWorkflow(handle, workflow, sequenceMap, seqInfo, outcomeSuccess, payload)
        }
    }

    private fun advanceWorkflow(
        handle: Handle,
        workflow: WorkflowRun,
        sequenceMap: Map<Int, SequenceInfo>,
        currentSeqInfo: SequenceInfo,
        outcomeSuccess: Boolean,
        payload: String?,
    ) {
        val currentSeq = workflow.currentSequence
        val nextSeq = currentSeq + 1
        var effectiveSuccess = outcomeSuccess

        // Handle failure
        if (!effectiveSuccess) {
            val failurePolicy = currentSeqInfo.activity.failurePolicy
            when (failurePolicy) {
                FailurePolicy.ABORT -> {
                    val updated = workflowRepo.updateStatusWithHandle(
                        handle, workflow.id, WorkflowStatus.FAILED, expectedStatus = WorkflowStatus.RUNNING,
                    )
                    if (updated) {
                        taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                    }
                    return
                }
                FailurePolicy.BEST_EFFORT -> effectiveSuccess = true
            }
        }

        // Check if this was the last sequence
        if (!sequenceMap.containsKey(nextSeq)) {
            workflowRepo.updateStatusWithHandle(
                handle, workflow.id, WorkflowStatus.COMPLETED, expectedStatus = WorkflowStatus.RUNNING,
            )
            return
        }

        // Insert tasks for next sequence
        val nextSeqInfo = sequenceMap[nextSeq]!!
        val nextPayload = if (currentSeqInfo.phaseType == PhaseType.PARALLEL) null else payload
        insertTasksForSequence(handle, workflow.id, nextSeq, nextSeqInfo, nextPayload)
    }

    private fun insertTasksForSequence(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
        seqInfo: SequenceInfo,
        payload: String?,
    ) {
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

        when (seqInfo.phaseType) {
            PhaseType.LINEAR, PhaseType.SCATTER -> {
                val task = createTaskForActivity(
                    workflowId = workflowId,
                    sequenceNumber = sequenceNumber,
                    activity = seqInfo.activity,
                    payload = payload,
                    now = now,
                )
                taskRepo.insertBatchWithHandle(handle, listOf(task))
            }

            PhaseType.PARALLEL -> {
                // Read scatter result from preceding SCATTER sequence
                val scatterSeq = sequenceNumber - 1
                val scatterTasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, scatterSeq)
                val scatterTask = scatterTasks.firstOrNull()
                    ?: throw IllegalStateException("No scatter task found at sequence $scatterSeq for workflow $workflowId")
                val payloads: List<String> = objectMapper.readValue(
                    scatterTask.resultJson
                        ?: throw IllegalStateException("Scatter task ${scatterTask.id} has no result for workflow $workflowId"),
                )
                val tasks = payloads.map { payload ->
                    Task(
                        id = UUID.randomUUID().toString(),
                        workflowId = workflowId,
                        sequenceNumber = sequenceNumber,
                        status = TaskStatus.PENDING,
                        handlerKey = seqInfo.activity.fanOut!!.transition,
                        payloadJson = payload,
                        resultJson = null,
                        claimedBy = null,
                        claimedAt = null,
                        completedAt = null,
                        retryCount = 0,
                        maxRetries = seqInfo.activity.fanOut!!.retries,
                        deadlineAt = now.plus(seqInfo.activity.fanOut!!.deadline),
                        backoffBase = seqInfo.activity.fanOut!!.backoffBase.seconds.toInt(),
                        backoffCap = seqInfo.activity.fanOut!!.backoffCap.seconds.toInt(),
                    )
                }
                taskRepo.insertBatchWithHandle(handle, tasks)
            }
        }
    }

    // ── Internal types and helpers ──

    private enum class PhaseType { LINEAR, SCATTER, PARALLEL }

    private data class SequenceInfo(
        val activityIndex: Int,
        val activity: ActivityDefinition,
        val phaseType: PhaseType,
    )

    private fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
        val map = mutableMapOf<Int, SequenceInfo>()
        var seq = 1
        for ((i, activity) in definition.activities.withIndex()) {
            if (activity.fanOut == null) {
                map[seq++] = SequenceInfo(i, activity, PhaseType.LINEAR)
            } else {
                map[seq++] = SequenceInfo(i, activity, PhaseType.SCATTER)
                map[seq++] = SequenceInfo(i, activity, PhaseType.PARALLEL)
            }
        }
        return map
    }

    private fun evaluateOutcome(
        phaseType: PhaseType,
        activity: ActivityDefinition,
        failedCount: Int,
        totalCount: Int,
    ): Boolean = when (phaseType) {
        PhaseType.LINEAR, PhaseType.SCATTER -> failedCount == 0
        PhaseType.PARALLEL -> {
            val joinPolicy = activity.fanOut!!.joinPolicy
            val succeededCount = totalCount - failedCount
            when (joinPolicy) {
                is JoinPolicy.All -> failedCount == 0
                is JoinPolicy.Threshold -> succeededCount >= joinPolicy.n
                is JoinPolicy.Percentage -> {
                    val successPct = if (totalCount > 0) (succeededCount * 100) / totalCount else 0
                    successPct >= joinPolicy.pct
                }
            }
        }
    }
}
