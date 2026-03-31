package com.workflow.dispatch.usecase.service.handler

import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import com.workflow.workflow.model.workflowId
import io.quarkus.scheduler.Scheduled
import io.smallrye.common.annotation.Blocking
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

    // Quarkus @Scheduled cron — configured via application.properties
    // dispatch.cron = 0 0 0,6,12,18 * * ?  (4x/day)
    @Blocking
    @Scheduled(cron = "{dispatch.cron}")
    suspend fun trigger() {
        val batchToken =
            LocalDateTime
                .now()
                .truncatedTo(ChronoUnit.HOURS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        val result =
            workflowEngine.startWorkflow(
                definition = dispatchWorkflow,
                idempotencyKey = "dispatch-$batchToken",
            )
        log.info("Dispatch trigger: batchToken={}, result={}", batchToken, result)
    }
}
