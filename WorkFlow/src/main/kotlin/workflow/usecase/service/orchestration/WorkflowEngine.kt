package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.persistence.withHandleSuspend
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.workflow.model.StartResult
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.createTaskForActivity
import com.workflow.workflow.usecase.port.inbound.orchestration.WorkflowLifecycle
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
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
    private val notifier: WorkerNotifier,
) : WorkflowLifecycle {

    private val log = LoggerFactory.getLogger(WorkflowEngine::class.java)

    private data class IdempotentResult(val id: String, val created: Boolean, val queueName: String?)

    override suspend fun startWorkflow(
        definition: WorkflowDefinition,
        idempotencyKey: String?,
    ): StartResult {
        val workflowId = UUID.randomUUID().toString()
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val definitionJson = objectMapper.writeValueAsString(definition)

        val sequenceMap = buildSequenceMap(definition)
        val startSeqInfo = sequenceMap.values.first { it.activityName == definition.start }

        val run = WorkflowRun(
            id = workflowId,
            definitionJson = definitionJson,
            version = 0,
            status = WorkflowStatus.RUNNING,
            createdAt = now,
            updatedAt = now,
            deadlineAt = now.plus(definition.deadline),
        )

        if (idempotencyKey == null) {
            val queueName = jdbi.inTransactionSuspend<String, Exception> { handle ->
                workflowRepo.insertWithHandle(handle, run)
                val task = createTaskForActivity(workflowId, startSeqInfo.activityName, startSeqInfo.sequenceNumber, startSeqInfo.activity, now)
                taskRepo.insertBatchWithHandle(handle, listOf(task))
                startSeqInfo.activity.queue
            }
            notifier.signal(queueName)
            log.info("Started workflow {} with {} activities", workflowId, definition.activities.size)
            return StartResult.Created(workflowId)
        }

        val (mergeId, created, queueName) = jdbi.inTransactionSuspend<IdempotentResult, Exception> { handle ->
            val (mId, isNew) = workflowRepo.mergeIdempotentWithHandle(handle, run, idempotencyKey)
            if (isNew) {
                val task = createTaskForActivity(mId, startSeqInfo.activityName, startSeqInfo.sequenceNumber, startSeqInfo.activity, now)
                taskRepo.insertBatchWithHandle(handle, listOf(task))
                IdempotentResult(mId, true, startSeqInfo.activity.queue)
            } else {
                IdempotentResult(mId, false, null)
            }
        }

        if (queueName != null) {
            notifier.signal(queueName)
            log.info("Started workflow {} (idempotent, key={}) with {} activities", mergeId, idempotencyKey, definition.activities.size)
        } else {
            log.info("Workflow already exists for key {}: {}", idempotencyKey, mergeId)
        }

        return if (created) StartResult.Created(mergeId) else StartResult.AlreadyExists(mergeId)
    }

    override suspend fun cancelWorkflow(workflowId: String): Boolean =
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

    override suspend fun replayWorkflow(workflowId: String): Boolean {
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
            supervisorScope {
                for (queue in queues) {
                    launch { notifier.signal(queue) }
                }
            }
        }
        return replayed
    }
}
