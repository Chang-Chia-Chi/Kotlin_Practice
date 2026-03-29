package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dsl.WorkflowDefinition
import com.workflow.extension.inTransactionSuspend
import com.workflow.extension.withHandleSuspend
import com.workflow.worker.DispatchNotifier
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@ApplicationScoped
class WorkflowEngine(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: DispatchNotifier,
) {

    private val log = LoggerFactory.getLogger(WorkflowEngine::class.java)

    suspend fun startWorkflow(definition: WorkflowDefinition): String {
        require(definition.activities.isNotEmpty()) { "WorkflowDefinition must have at least one activity" }

        val workflowId = UUID.randomUUID().toString()
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val definitionJson = objectMapper.writeValueAsString(definition)

        val queueName = jdbi.inTransactionSuspend<String, Exception> { handle ->
            val run = WorkflowRun(
                id = workflowId,
                definitionJson = definitionJson,
                currentSequence = 1,
                version = 0,
                status = WorkflowStatus.RUNNING,
                createdAt = now,
                updatedAt = now,
                deadlineAt = now.plus(definition.deadline),
            )
            workflowRepo.insertWithHandle(handle, run)

            val firstActivity = definition.activities.first()
            val task = createTaskForActivity(
                workflowId = workflowId,
                sequenceNumber = 1,
                activity = firstActivity,
                now = now,
            )
            taskRepo.insertBatchWithHandle(handle, listOf(task))
            firstActivity.queue
        }

        notifier.signal(queueName)
        log.info("Started workflow {} with {} activities", workflowId, definition.activities.size)
        return workflowId
    }

    suspend fun cancelWorkflow(workflowId: String): Boolean =
        jdbi.inTransactionSuspend<Boolean, Exception> { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: return@inTransactionSuspend false
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend false

            val updated = workflowRepo.updateStatusWithHandle(
                handle, workflowId, WorkflowStatus.CANCELLED, expectedStatus = WorkflowStatus.RUNNING,
            )
            if (updated) {
                taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
                log.info("Cancelled workflow {}", workflowId)
            }
            updated
        }

    suspend fun replayWorkflow(workflowId: String): Boolean {
        val replayed = jdbi.inTransactionSuspend<Boolean, Exception> { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: return@inTransactionSuspend false
            if (workflow.status != WorkflowStatus.FAILED) return@inTransactionSuspend false

            workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.RUNNING, WorkflowStatus.FAILED)
            taskRepo.replayDeadLetterBatchWithHandle(handle, workflowId)
            true
        }
        if (replayed) {
            val queues = jdbi.withHandleSuspend<List<String>, Exception> { handle ->
                taskRepo.findDistinctQueuesByWorkflowId(handle, workflowId, listOf("PENDING"))
            }
            for (queue in queues) {
                notifier.signal(queue)
            }
        }
        return replayed
    }
}
