package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.engine.BarrierService
import com.workflow.engine.InputResolver
import com.workflow.engine.PhaseStrategyRegistry
import com.workflow.engine.SequenceInfo
import com.workflow.engine.Task
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.engine.WorkflowRepository
import com.workflow.worker.DispatchNotifier
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.time.Instant

class InstrumentedTaskRepository(
    jdbi: Jdbi,
    private val timer: PhaseTimer,
) : TaskRepository(jdbi) {

    override suspend fun claimNext(workerId: String, limit: Int, queueName: String): List<Task> =
        timer.suspendTime("task.claim") { super.claimNext(workerId, limit, queueName) }

    override fun insertBatchWithHandle(handle: Handle, tasks: List<Task>) =
        timer.time("task.insert") { super.insertBatchWithHandle(handle, tasks) }
}

class InstrumentedWorkflowRepository(
    jdbi: Jdbi,
    private val timer: PhaseTimer,
) : WorkflowRepository(jdbi) {

    override fun casAdvanceWithHandle(
        handle: Handle, id: String, expectedSequence: Int,
        nextSequence: Int, expectedVersion: Int,
    ): Boolean = timer.time("workflow.cas") {
        super.casAdvanceWithHandle(handle, id, expectedSequence, nextSequence, expectedVersion)
    }
}

class InstrumentedBarrierService(
    jdbi: Jdbi,
    workflowRepo: WorkflowRepository,
    taskRepo: TaskRepository,
    objectMapper: ObjectMapper,
    strategyRegistry: PhaseStrategyRegistry,
    notifier: DispatchNotifier,
    private val timer: PhaseTimer,
) : BarrierService(jdbi, workflowRepo, taskRepo, objectMapper, strategyRegistry, notifier) {

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
