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

    private data class IdempotentResult(val id: String, val created: Boolean, val queueNames: Set<String>)

    override suspend fun startWorkflow(
        definition: WorkflowDefinition,
        idempotencyKey: String?,
        initialItem: String?,
    ): StartResult {
        val workflowId = UUID.randomUUID().toString()
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val definitionJson = objectMapper.writeValueAsString(definition)

        val sequenceMap = buildSequenceMap(definition)
        val startSeqInfos = definition.starts.map { name ->
            sequenceMap.values.first { it.activityName == name }
        }

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
            val queueNames = jdbi.inTransactionSuspend<Set<String>, Exception> { handle ->
                workflowRepo.insertWithHandle(handle, run)
                val tasks = startSeqInfos.map { seqInfo ->
                    createTaskForActivity(
                        workflowId, seqInfo.activityName, seqInfo.sequenceNumber,
                        seqInfo.activity, now, initialItem,
                        staleThresholdSecs = definition.staleThreshold.seconds.toInt(),
                    )
                }
                taskRepo.insertBatchWithHandle(handle, tasks)
                startSeqInfos.map { it.activity.queue }.toSet()
            }
            for (queue in queueNames) notifier.signal(queue)
            log.info(
                "Started workflow {} with {} activities and {} root(s)",
                workflowId, definition.activities.size, startSeqInfos.size,
            )
            return StartResult.Created(workflowId)
        }

        val (mergeId, created, queueNames) = jdbi.inTransactionSuspend<IdempotentResult, Exception> { handle ->
            val (mId, isNew) = workflowRepo.mergeIdempotentWithHandle(handle, run, idempotencyKey)
            if (isNew) {
                val tasks = startSeqInfos.map { seqInfo ->
                    createTaskForActivity(
                        mId, seqInfo.activityName, seqInfo.sequenceNumber,
                        seqInfo.activity, now, initialItem,
                        staleThresholdSecs = definition.staleThreshold.seconds.toInt(),
                    )
                }
                taskRepo.insertBatchWithHandle(handle, tasks)
                IdempotentResult(mId, true, startSeqInfos.map { it.activity.queue }.toSet())
            } else {
                IdempotentResult(mId, false, emptySet())
            }
        }

        if (queueNames.isNotEmpty()) {
            for (queue in queueNames) notifier.signal(queue)
            log.info(
                "Started workflow {} (idempotent, key={}) with {} activities and {} root(s)",
                mergeId, idempotencyKey, definition.activities.size, startSeqInfos.size,
            )
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
