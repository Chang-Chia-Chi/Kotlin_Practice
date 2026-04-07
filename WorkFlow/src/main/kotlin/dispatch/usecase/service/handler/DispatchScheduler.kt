package com.workflow.dispatch.usecase.service.handler

import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.quarkus.scheduler.Scheduled
import io.smallrye.common.annotation.Blocking
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

@ApplicationScoped
class DispatchScheduler(
    private val workflowEngine: WorkflowEngine,
) {
    private val log = LoggerFactory.getLogger(DispatchScheduler::class.java)

    @Blocking
    @Scheduled(cron = "{dispatch.cron}", skipExecutionIf = com.workflow.infrastructure.leader.NotLeader::class)
    fun trigger() =
        runBlocking {
            val batchToken = currentBatchToken()

            val result =
                workflowEngine.startWorkflow(
                    definition = dispatchWorkflow,
                    idempotencyKey = "dispatch-$batchToken",
                )
            log.info("Dispatch trigger: batchToken={}, result={}", batchToken, result)
        }
}
