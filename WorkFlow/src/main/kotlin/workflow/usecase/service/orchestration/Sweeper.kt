package com.workflow.workflow.usecase.service.orchestration

import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.leader.NotLeader
import com.workflow.workflow.config.SweeperConfig
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.runBlocking
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant

@ApplicationScoped
class Sweeper(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val barrierService: BarrierService,
    private val sweeperConfig: SweeperConfig,
) {

    private val log = LoggerFactory.getLogger(Sweeper::class.java)

    @Scheduled(every = "{framework.sweeper.interval}", skipExecutionIf = NotLeader::class)
    fun sweep() = runBlocking { patrol() }

    suspend fun patrol() {
        expireOverdueTasks()
        reclaimStaleTasks()
        recoverStuckWorkflows()
        expireOverdueWorkflows()
    }

    private suspend fun expireOverdueTasks() {
        val expired = taskRepo.findExpired(Instant.now())
        for (task in expired) {
            try {
                log.warn("Expiring overdue task {} (deadline={})", task.id, task.deadlineAt)
                barrierService.onTaskCompleted(
                    taskId = task.id,
                    workflowId = task.workflowId,
                    sequenceNumber = task.sequenceNumber,
                    status = TaskStatus.TIMED_OUT,
                    resultJson = null,
                )
            } catch (e: Exception) {
                log.error("Failed to expire task {}", task.id, e)
            }
        }
    }

    private suspend fun reclaimStaleTasks() {
        val threshold = Instant.now().minus(sweeperConfig.staleTaskThreshold())

        val reclaimed = taskRepo.resetStaleTasks(threshold)
        if (reclaimed > 0) {
            log.info("Reclaimed {} stale task(s) for retry", reclaimed)
        }

        val deadLettered = taskRepo.deadLetterExhaustedTasks(threshold)
        if (deadLettered > 0) {
            log.warn("Dead-lettered {} exhausted stale task(s)", deadLettered)
        }
    }

    private suspend fun recoverStuckWorkflows() {
        val gracePeriod = sweeperConfig.gracePeriod()
        val stuck = workflowRepo.findStuck(gracePeriod)
        for (workflow in stuck) {
            try {
                log.warn(
                    "Recovering stuck workflow {} at sequence {} (last updated {})",
                    workflow.id, workflow.currentSequence, workflow.updatedAt,
                )
                barrierService.recoverStuckWorkflow(workflow.id)
            } catch (e: Exception) {
                log.error("Failed to recover stuck workflow {}", workflow.id, e)
            }
        }
    }

    private suspend fun expireOverdueWorkflows() {
        val timedOut = workflowRepo.findTimedOut()
        for (workflow in timedOut) {
            try {
                jdbi.inTransactionSuspend<Unit, Exception> { handle ->
                    val updated = workflowRepo.updateStatusWithHandle(
                        handle, workflow.id, WorkflowStatus.TIMED_OUT, expectedStatus = WorkflowStatus.RUNNING,
                    )
                    if (updated) {
                        taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                        log.warn("Workflow {} timed out (deadline was {})", workflow.id, workflow.deadlineAt)
                    }
                }
            } catch (e: Exception) {
                log.error("Failed to time out workflow {}", workflow.id, e)
            }
        }
    }
}
