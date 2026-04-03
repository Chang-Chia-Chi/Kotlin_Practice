package com.workflow.dispatch.usecase.service.handler

import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import com.workflow.workflow.model.workflowId
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

@ApplicationScoped
class DispatchScheduler(
    private val workflowEngine: WorkflowEngine,
) {
    private val log = LoggerFactory.getLogger(DispatchScheduler::class.java)

    @Scheduled(cron = "{dispatch.cron}", skipExecutionIf = com.workflow.infrastructure.leader.NotLeader::class)
    fun trigger() = runBlocking {
        val batchToken =
            LocalDateTime
                .now()
                .truncatedTo(ChronoUnit.HOURS)
                .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

        val result =
            workflowEngine.startWorkflow(
                definition = dispatchWorkflow,
                idempotencyKey = "dispatch-$batchToken",
            )
        log.info("Dispatch trigger: batchToken={}, result={}", batchToken, result)
    }
}
