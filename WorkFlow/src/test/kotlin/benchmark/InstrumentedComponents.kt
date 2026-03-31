package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.InputResolver
import com.workflow.workflow.usecase.service.phase.AdvancementStrategyRegistry
import com.workflow.worker.usecase.port.outbound.notification.DispatchNotifier
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.time.Instant

class InstrumentedTaskRepository(
    jdbi: Jdbi,
    private val timer: PhaseTimer,
) : JdbiTaskRepository(jdbi) {

    override suspend fun claimNext(workerId: String, limit: Int, queueName: String): List<Task> =
        timer.suspendTime("task.claim") { super.claimNext(workerId, limit, queueName) }

    override fun insertBatchWithHandle(handle: Handle, tasks: List<Task>) =
        timer.time("task.insert") { super.insertBatchWithHandle(handle, tasks) }
}

class InstrumentedWorkflowRepository(
    jdbi: Jdbi,
    private val timer: PhaseTimer,
) : JdbiWorkflowRepository(jdbi) {

    override fun casAdvanceWithHandle(
        handle: Handle, id: String, expectedSequence: Int,
        nextSequence: Int, expectedVersion: Int,
    ): Boolean = timer.time("workflow.cas") {
        super.casAdvanceWithHandle(handle, id, expectedSequence, nextSequence, expectedVersion)
    }
}

class InstrumentedDefaultPhaseGate(
    jdbi: Jdbi,
    workflowRepo: WorkflowRepository,
    taskRepo: TaskRepository,
    objectMapper: ObjectMapper,
    strategyRegistry: AdvancementStrategyRegistry,
    notifier: DispatchNotifier,
    private val timer: PhaseTimer,
) : DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, strategyRegistry, notifier) {

    override suspend fun onTaskCompleted(
        taskId: String, workflowId: String, sequenceNumber: Int,
        status: TaskStatus, resultJson: String?,
        claimedBy: String?, claimedAt: Instant?,
    ) = timer.suspendTime("barrier.evaluate") {
        super.onTaskCompleted(taskId, workflowId, sequenceNumber, status, resultJson, claimedBy, claimedAt)
    }
}

class InstrumentedInputResolver(
    objectMapper: ObjectMapper,
    private val timer: PhaseTimer,
) : InputResolver(objectMapper) {

    override suspend fun resolve(
        inputs: Map<String, String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: suspend (Int) -> List<Task>,
    ): String? = timer.suspendTime("input.resolve") {
        super.resolve(inputs, sequenceMap, tasksBySequence)
    }
}

class TimedHandler(
    private val delegate: TransitionHandler,
    private val timer: PhaseTimer,
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput =
        timer.suspendTime("handler.execute") { delegate.execute(input) }
}
