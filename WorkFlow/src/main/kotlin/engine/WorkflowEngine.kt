package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dsl.WorkflowDefinition
import com.workflow.extension.inTransactionSuspend
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@ApplicationScoped
class WorkflowEngine(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
) {

    suspend fun startWorkflow(definition: WorkflowDefinition, initialPayload: String? = null): String {
        val workflowId = UUID.randomUUID().toString()
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val definitionJson = objectMapper.writeValueAsString(definition)

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val run = WorkflowRun(
                id = workflowId,
                definitionJson = definitionJson,
                currentSequence = 1,
                version = 0,
                status = WorkflowStatus.RUNNING,
                createdAt = now,
                updatedAt = now,
            )
            workflowRepo.insertWithHandle(handle, run)

            val firstActivity = definition.activities.first()
            val isScatter = firstActivity.fanOut != null

            val handlerKey = if (isScatter) firstActivity.fanOut!!.transition else firstActivity.transition
            val maxRetries = if (isScatter) firstActivity.fanOut!!.retries else firstActivity.retries
            val deadline = if (isScatter) firstActivity.fanOut!!.deadline else firstActivity.deadline

            val task = Task(
                id = UUID.randomUUID().toString(),
                workflowId = workflowId,
                sequenceNumber = 1,
                status = TaskStatus.PENDING,
                handlerKey = handlerKey,
                payloadJson = initialPayload,
                resultJson = null,
                claimedBy = null,
                claimedAt = null,
                completedAt = null,
                retryCount = 0,
                maxRetries = maxRetries,
                deadlineAt = now.plus(deadline),
            )
            taskRepo.insertBatchWithHandle(handle, listOf(task))
        }

        return workflowId
    }
}
