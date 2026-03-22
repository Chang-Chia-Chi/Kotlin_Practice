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
        result: TaskStatus,
        resultJson: String?,
    ) {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            // 1. Self-update
            taskRepo.updateStatusWithHandle(handle, taskId, result, resultJson)

            // 2. Lock-free probe
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            // 3. Load workflow and compute sequence metadata
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
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
                    workflowRepo.updateStatusWithHandle(handle, workflow.id, WorkflowStatus.FAILED)
                    return
                }
                FailurePolicy.BEST_EFFORT -> effectiveSuccess = true
            }
        }

        // Check if this was the last sequence
        if (!sequenceMap.containsKey(nextSeq)) {
            workflowRepo.updateStatusWithHandle(handle, workflow.id, WorkflowStatus.COMPLETED)
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
                    isScatter = seqInfo.phaseType == PhaseType.SCATTER,
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
                        handlerKey = seqInfo.activity.transition,
                        payloadJson = payload,
                        resultJson = null,
                        claimedBy = null,
                        claimedAt = null,
                        completedAt = null,
                        retryCount = 0,
                        maxRetries = seqInfo.activity.retries,
                        deadlineAt = now.plus(seqInfo.activity.deadline),
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
        val totalSequences: Int,
    )

    private fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
        val map = mutableMapOf<Int, SequenceInfo>()
        var seq = 1
        // First pass: count total sequences
        var total = 0
        for (activity in definition.activities) {
            total += if (activity.fanOut != null) 2 else 1
        }
        // Second pass: populate map
        for ((i, activity) in definition.activities.withIndex()) {
            if (activity.fanOut == null) {
                map[seq] = SequenceInfo(i, activity, PhaseType.LINEAR, total)
                seq++
            } else {
                map[seq] = SequenceInfo(i, activity, PhaseType.SCATTER, total)
                seq++
                map[seq] = SequenceInfo(i, activity, PhaseType.PARALLEL, total)
                seq++
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
