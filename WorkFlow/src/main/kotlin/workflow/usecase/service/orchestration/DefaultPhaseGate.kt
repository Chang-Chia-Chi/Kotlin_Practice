package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.PhaseContext
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.createTaskForActivity
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import com.workflow.workflow.usecase.service.phase.AdvancementStrategyRegistry
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit

@ApplicationScoped
class DefaultPhaseGate(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val strategyRegistry: AdvancementStrategyRegistry,
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
        var signalQueue: String? = null

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
            if (!updated) return@inTransactionSuspend

            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend
            if (sequenceNumber != workflow.currentSequence) return@inTransactionSuspend

            signalQueue = advanceWorkflow(handle, workflow, sequenceNumber, resultJson)
        }

        if (signalQueue != null) notifier.signal(signalQueue!!)
    }

    override suspend fun recoverStuckWorkflow(workflowId: String) {
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

            signalQueue = advanceWorkflow(handle, workflow, seq)
        }

        if (signalQueue != null) notifier.signal(signalQueue!!)
    }

    /**
     * Core advancement pipeline shared by [onTaskCompleted] and [recoverStuckWorkflow].
     * Resolves the phase strategy, produces a decision, and executes it.
     * Returns the queue name to signal, or null.
     */
    private fun advanceWorkflow(
        handle: Handle,
        workflow: WorkflowRun,
        sequenceNumber: Int,
        resultJson: String? = null,
    ): String? {
        val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqInfo = sequenceMap[sequenceNumber]
            ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

        val failedCount = taskRepo.countFailedWithHandle(handle, workflow.id, sequenceNumber)
        val totalCount = taskRepo.countTotalWithHandle(handle, workflow.id, sequenceNumber)

        val strategy = strategyRegistry.resolve(seqInfo.phaseType)
        val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, totalCount)
        val decision = strategy.resolve(context)

        return when (decision) {
            is AdvancementDecision.Advance -> advanceToNextPhase(handle, workflow, seqInfo, sequenceMap, decision, resultJson)
            is AdvancementDecision.Complete -> completeWorkflow(handle, workflow)
            is AdvancementDecision.Abort -> abortWorkflow(handle, workflow, seqInfo, decision)
        }
    }

    private fun advanceToNextPhase(
        handle: Handle,
        workflow: WorkflowRun,
        seqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        decision: AdvancementDecision.Advance,
        resultJson: String?,
    ): String? {
        val casWon = workflowRepo.casAdvanceWithHandle(
            handle, workflow.id, seqInfo.sequenceNumber, decision.nextSequence, workflow.version,
        )
        if (!casWon) {
            log.debug("CAS lost for workflow {} at sequence {}", workflow.id, seqInfo.sequenceNumber)
            return null
        }
        val nextSeqInfo = sequenceMap[decision.nextSequence]!!
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val tasks = createNextPhaseTasks(workflow.id, nextSeqInfo, now, resultJson)
        taskRepo.insertBatchWithHandle(handle, tasks)
        return nextSeqInfo.activity.queue
    }

    private fun createNextPhaseTasks(
        workflowId: String,
        nextSeqInfo: SequenceInfo,
        now: Instant,
        resultJson: String?,
    ): List<Task> = when (nextSeqInfo.phaseType) {
        PhaseType.PARALLEL -> {
            val items: List<String> = objectMapper.readValue(
                resultJson ?: throw IllegalStateException(
                    "PARALLEL phase requires scatter result but none provided for workflow $workflowId"
                )
            )
            require(items.isNotEmpty()) {
                "Fan-out produced 0 items for workflow $workflowId. " +
                    "Scatter handler must return a non-empty JSON array."
            }
            items.map {
                createTaskForActivity(workflowId, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now, item = it)
            }
        }
        PhaseType.LINEAR -> {
            listOf(createTaskForActivity(workflowId, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now))
        }
        PhaseType.SCATTER -> error("SCATTER phase type is not yet supported")
    }

    private fun completeWorkflow(handle: Handle, workflow: WorkflowRun): String? {
        workflowRepo.updateStatusWithHandle(
            handle, workflow.id, WorkflowStatus.COMPLETED, expectedStatus = WorkflowStatus.RUNNING,
        )
        return null
    }

    private fun abortWorkflow(
        handle: Handle,
        workflow: WorkflowRun,
        seqInfo: SequenceInfo,
        decision: AdvancementDecision.Abort,
    ): String? {
        log.warn("Workflow {} failed at sequence {}: {}", workflow.id, seqInfo.sequenceNumber, decision.reason)
        val updated = workflowRepo.updateStatusWithHandle(
            handle, workflow.id, WorkflowStatus.FAILED, expectedStatus = WorkflowStatus.RUNNING,
        )
        if (updated) {
            taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
        }
        return null
    }
}
