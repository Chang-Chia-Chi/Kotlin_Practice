package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.dsl.WorkflowDefinition
import com.workflow.extension.inTransactionSuspend
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant

private val FAILED_STATUSES = setOf(TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.DEAD_LETTER)

@ApplicationScoped
class BarrierService(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val strategyRegistry: PhaseStrategyRegistry,
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
            if (!updated) return@inTransactionSuspend

            // 2. Lock-free probe
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            // 3. All tasks terminal — evaluate and advance
            evaluateAndAdvance(handle, workflowId, sequenceNumber)
        }
    }

    internal suspend fun recoverStuckWorkflow(workflowId: String) {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: run {
                    log.warn("Workflow not found during recovery: {}", workflowId)
                    return@inTransactionSuspend
                }
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

            val seq = workflow.currentSequence

            // TOCTOU safety: fast-return if any tasks still non-terminal
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, seq)
            if (nonTerminal > 0) return@inTransactionSuspend

            // All terminal — fetch full list for strategy delegation
            val tasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, seq)
            val failedCount = tasks.count { it.status in FAILED_STATUSES }
            resolveAndExecute(handle, workflow, seq, tasks, failedCount)
        }
    }

    private fun evaluateAndAdvance(handle: Handle, workflowId: String, sequenceNumber: Int) {
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: throw IllegalStateException("Workflow not found: $workflowId")
        if (workflow.status != WorkflowStatus.RUNNING) return
        if (sequenceNumber != workflow.currentSequence) return

        val tasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, sequenceNumber)
        val failedCount = tasks.count { it.status in FAILED_STATUSES }

        resolveAndExecute(handle, workflow, sequenceNumber, tasks, failedCount)
    }

    private fun resolveAndExecute(
        handle: Handle,
        workflow: WorkflowRun,
        sequenceNumber: Int,
        tasks: List<Task>,
        failedCount: Int,
    ) {
        val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqInfo = sequenceMap[sequenceNumber]
            ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

        val strategy = strategyRegistry.resolve(seqInfo.phaseType)
        val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, tasks.size, tasks)
        val decision = strategy.resolve(context)

        executeDecision(handle, workflow, seqInfo, decision)
    }

    private fun executeDecision(
        handle: Handle,
        workflow: WorkflowRun,
        seqInfo: SequenceInfo,
        decision: AdvancementDecision,
    ) {
        when (decision) {
            is AdvancementDecision.Advance -> {
                val casWon = workflowRepo.casAdvanceWithHandle(
                    handle, workflow.id, seqInfo.sequenceNumber, decision.nextSequence, workflow.version,
                )
                if (!casWon) {
                    log.debug("CAS lost for workflow {} at sequence {}", workflow.id, seqInfo.sequenceNumber)
                    return
                }
                taskRepo.insertBatchWithHandle(handle, decision.tasks)
            }
            is AdvancementDecision.Complete -> {
                workflowRepo.updateStatusWithHandle(
                    handle, workflow.id, WorkflowStatus.COMPLETED, expectedStatus = WorkflowStatus.RUNNING,
                )
            }
            is AdvancementDecision.Abort -> {
                log.warn("Workflow {} failed at sequence {}: {}", workflow.id, seqInfo.sequenceNumber, decision.reason)
                val updated = workflowRepo.updateStatusWithHandle(
                    handle, workflow.id, WorkflowStatus.FAILED, expectedStatus = WorkflowStatus.RUNNING,
                )
                if (updated) {
                    taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                }
            }
        }
    }
}
