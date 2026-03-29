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
import java.time.temporal.ChronoUnit

@ApplicationScoped
class BarrierService(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val strategyRegistry: PhaseStrategyRegistry,
    private val notifier: com.workflow.worker.DispatchNotifier,
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
            val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
            if (!updated) return@inTransactionSuspend
        }

        var signalQueue: String? = null

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            signalQueue = evaluateAndAdvance(handle, workflowId, sequenceNumber)
        }

        if (signalQueue != null) notifier.signal(signalQueue!!)
    }

    internal suspend fun recoverStuckWorkflow(workflowId: String) {
        var signalQueue: String? = null

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val workflow =
                workflowRepo.findByIdWithHandle(handle, workflowId)
                    ?: run {
                        log.warn("Workflow not found during recovery: {}", workflowId)
                        return@inTransactionSuspend
                    }
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

            val seq = workflow.currentSequence
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, seq)
            if (nonTerminal > 0) return@inTransactionSuspend

            val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, seq)
            val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, seq)
            signalQueue = resolveAndExecute(handle, workflow, seq, failedCount, totalCount)
        }

        if (signalQueue != null) notifier.signal(signalQueue!!)
    }

    /**
     * Evaluates the current phase and advances the workflow.
     * Returns the queue name of inserted next-phase tasks, or null
     * if the workflow completed, aborted, or no advancement occurred.
     */
    private fun evaluateAndAdvance(
        handle: Handle,
        workflowId: String,
        sequenceNumber: Int,
    ): String? {
        val workflow =
            workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
        if (workflow.status != WorkflowStatus.RUNNING) return null
        if (sequenceNumber != workflow.currentSequence) return null

        val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
        val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)

        return resolveAndExecute(handle, workflow, sequenceNumber, failedCount, totalCount)
    }

    /**
     * Resolves the phase strategy and executes the advancement decision.
     * Returns the queue name of inserted next-phase tasks, or null.
     */
    private fun resolveAndExecute(
        handle: Handle,
        workflow: WorkflowRun,
        sequenceNumber: Int,
        failedCount: Int,
        totalCount: Int,
    ): String? {
        val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqInfo =
            sequenceMap[sequenceNumber]
                ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

        val strategy = strategyRegistry.resolve(seqInfo.phaseType)
        val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, totalCount)
        val decision = strategy.resolve(context)

        return executeDecision(handle, workflow, seqInfo, sequenceMap, decision)
    }

    /**
     * Executes the advancement decision within the current transaction.
     * Returns the queue name of the inserted next-phase tasks (for
     * Advance decisions), or null for Complete/Abort/CAS-lost.
     */
    private fun executeDecision(
        handle: Handle,
        workflow: WorkflowRun,
        seqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        decision: AdvancementDecision,
    ): String? {
        when (decision) {
            is AdvancementDecision.Advance -> {
                val casWon =
                    workflowRepo.casAdvanceWithHandle(
                        handle,
                        workflow.id,
                        seqInfo.sequenceNumber,
                        decision.nextSequence,
                        workflow.version,
                    )
                if (!casWon) {
                    log.debug("CAS lost for workflow {} at sequence {}", workflow.id, seqInfo.sequenceNumber)
                    return null
                }
                val nextSeqInfo = sequenceMap[decision.nextSequence]!!
                val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
                when (nextSeqInfo.phaseType) {
                    PhaseType.PARALLEL -> {
                        taskRepo.insertFanOutFromScatter(
                            handle,
                            workflow.id,
                            seqInfo.sequenceNumber,
                            nextSeqInfo,
                            now,
                        )
                    }

                    PhaseType.LINEAR -> {
                        taskRepo.insertBatchWithHandle(
                            handle,
                            listOf(createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now)),
                        )
                    }
                }
                return nextSeqInfo.activity.queue
            }

            is AdvancementDecision.Complete -> {
                workflowRepo.updateStatusWithHandle(
                    handle,
                    workflow.id,
                    WorkflowStatus.COMPLETED,
                    expectedStatus = WorkflowStatus.RUNNING,
                )
                return null
            }

            is AdvancementDecision.Abort -> {
                log.warn("Workflow {} failed at sequence {}: {}", workflow.id, seqInfo.sequenceNumber, decision.reason)
                val updated =
                    workflowRepo.updateStatusWithHandle(
                        handle,
                        workflow.id,
                        WorkflowStatus.FAILED,
                        expectedStatus = WorkflowStatus.RUNNING,
                    )
                if (updated) {
                    taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                }
                return null
            }
        }
    }
}
