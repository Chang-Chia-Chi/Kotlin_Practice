package com.workflow.workflow.usecase.service.orchestration

import com.workflow.infrastructure.coroutine.unorderedMapAsync
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.leader.NotLeader
import com.workflow.workflow.config.WatchdogConfig
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

@ApplicationScoped
class WorkflowWatchdog(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val phaseGate: PhaseGate,
    private val watchdogConfig: WatchdogConfig,
) {

    companion object {
        private const val MAX_TASK_EXPIRY_CONCURRENCY = 4
    }

    private val log = LoggerFactory.getLogger(WorkflowWatchdog::class.java)

    @Scheduled(every = "{framework.watchdog.interval}", skipExecutionIf = NotLeader::class)
    fun sweep() = runBlocking { patrol() }

    suspend fun patrol() {
        expireOverdueTasks()
        reclaimStaleTasks()
        recoverStuckWorkflows()
        expireOverdueWorkflows()
    }

    private suspend fun expireOverdueTasks() {
        val expired = taskRepo.findExpired(Instant.now())
        if (expired.isEmpty()) return

        expired.asFlow()
            .unorderedMapAsync(MAX_TASK_EXPIRY_CONCURRENCY) { task ->
                try {
                    log.warn("Expiring overdue task {} (deadline={})", task.id, task.deadlineAt)
                    phaseGate.onTaskCompleted(
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
            .collect()
    }

    private suspend fun reclaimStaleTasks() {
        val threshold = Instant.now().minus(watchdogConfig.staleTaskThreshold())

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
        val gracePeriod = watchdogConfig.gracePeriod()
        val stuck = workflowRepo.findStuck(gracePeriod)
        for (workflow in stuck) {
            try {
                log.warn(
                    "Recovering stuck workflow {} (last updated {})",
                    workflow.id, workflow.updatedAt,
                )
                phaseGate.recoverStuckWorkflow(workflow.id)
            } catch (e: Exception) {
                log.error("Failed to recover stuck workflow {}", workflow.id, e)
            }
        }
    }

    private suspend fun expireOverdueWorkflows() {
        val (timedOutCount, cancelledCount) = jdbi.inTransactionSuspend<Pair<Int, Int>, Exception> { handle ->
            val now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)

            val cancelled = handle.createUpdate(
                """
                UPDATE task SET status = 'CANCELLED', completed_at = :now
                WHERE status IN ('PENDING', 'WAITING_FOR_SIGNAL')
                  AND workflow_id IN (
                    SELECT id FROM workflow WHERE status = 'RUNNING' AND deadline_at < :now
                  )
                """,
            ).bind("now", now).execute()

            val timedOut = handle.createUpdate(
                """
                UPDATE workflow SET status = 'TIMED_OUT', updated_at = :now
                WHERE status = 'RUNNING' AND deadline_at < :now
                """,
            ).bind("now", now).execute()

            timedOut to cancelled
        }

        if (timedOutCount > 0) {
            log.warn("Timed out {} workflow(s), cancelled {} pending task(s)", timedOutCount, cancelledCount)
        }
    }
}
