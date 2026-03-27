package com.workflow.engine

import com.workflow.config.FrameworkConfig
import com.workflow.leader.NotLeader
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Instant

@ApplicationScoped
class Sweeper(
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val barrierService: BarrierService,
    private val config: FrameworkConfig,
) {

    private val log = LoggerFactory.getLogger(Sweeper::class.java)

    @Scheduled(every = "{framework.sweeper.interval}", skipExecutionIf = NotLeader::class)
    fun sweep() = runBlocking { patrol() }

    suspend fun patrol() {
        failExpiredTasks()
        reclaimStaleTasks()
        recoverStuckWorkflows()
    }

    private suspend fun failExpiredTasks() {
        val expired = taskRepo.findExpired(Instant.now())
        for (task in expired) {
            try {
                log.warn("Failing expired task {} (deadline={})", task.id, task.deadlineAt)
                barrierService.onTaskCompleted(
                    taskId = task.id,
                    workflowId = task.workflowId,
                    sequenceNumber = task.sequenceNumber,
                    status = TaskStatus.FAILED,
                    resultJson = null,
                )
            } catch (e: Exception) {
                log.error("Failed to expire task {}", task.id, e)
            }
        }
    }

    private suspend fun reclaimStaleTasks() {
        val threshold = Instant.now().minus(config.sweeper().staleTaskThreshold())

        val reclaimed = taskRepo.resetStaleTasks(threshold)
        if (reclaimed > 0) {
            log.info("Reclaimed {} stale task(s) for retry", reclaimed)
        }

        val exhausted = taskRepo.findStale(threshold)
        for (task in exhausted) {
            try {
                barrierService.onTaskCompleted(
                    taskId = task.id,
                    workflowId = task.workflowId,
                    sequenceNumber = task.sequenceNumber,
                    status = TaskStatus.FAILED,
                    resultJson = null,
                )
                log.warn(
                    "Stale task {} exhausted retries ({}/{}), marked FAILED",
                    task.id, task.retryCount, task.maxRetries,
                )
            } catch (e: Exception) {
                log.error("Failed to fail exhausted stale task {}", task.id, e)
            }
        }
    }

    private suspend fun recoverStuckWorkflows() {
        val gracePeriod = config.sweeper().gracePeriod()
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
}
