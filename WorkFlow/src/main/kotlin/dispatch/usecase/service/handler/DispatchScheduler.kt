package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.model.DispatchCategory
import com.workflow.infrastructure.leader.NotLeader
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.quarkus.scheduler.Scheduled
import io.smallrye.common.annotation.Blocking
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

@ApplicationScoped
class DispatchScheduler(
    private val workflowEngine: WorkflowEngine,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(DispatchScheduler::class.java)

    @Blocking
    @Scheduled(cron = "{dispatch.cron.urgent}", skipExecutionIf = NotLeader::class)
    fun triggerUrgent() = runBlocking { trigger(setOf(DispatchCategory.URGENT)) }

    @Blocking
    @Scheduled(cron = "{dispatch.cron.normal}", skipExecutionIf = NotLeader::class)
    fun triggerNormal() = runBlocking { trigger(setOf(DispatchCategory.NORMAL)) }

    @Blocking
    @Scheduled(cron = "{dispatch.cron.background}", skipExecutionIf = NotLeader::class)
    fun triggerBackground() = runBlocking { trigger(setOf(DispatchCategory.BACKGROUND)) }

    // Optional combined or all-categories entry points follow the same shape.
    // Operators add them when a single trigger should cover several categories at once:
    //
    // @Scheduled(cron = "{dispatch.cron.urgent-and-normal}", skipExecutionIf = NotLeader::class)
    // fun triggerUrgentAndNormal() = runBlocking {
    //     trigger(setOf(DispatchCategory.URGENT, DispatchCategory.NORMAL))
    // }
    //
    // @Scheduled(cron = "{dispatch.cron.all}", skipExecutionIf = NotLeader::class)
    // fun triggerAll() = runBlocking { trigger(emptySet()) }

    /** Test-only seam — bypasses @Scheduled and leader gating. Do not call from production. */
    internal suspend fun triggerForTest(categories: Set<DispatchCategory>) = trigger(categories)

    private suspend fun trigger(categories: Set<DispatchCategory>) {
        val batchToken = currentBatchToken()
        val keyCats =
            if (categories.isEmpty()) {
                "ALL"
            } else {
                categories.map { it.name }.sorted().joinToString("-")
            }
        val payload = objectMapper.writeValueAsString(
            mapOf("categories" to categories.map { it.name }.sorted()),
        )
        val result = workflowEngine.startWorkflow(
            definition = dispatchWorkflow,
            idempotencyKey = "dispatch-$keyCats-$batchToken",
            initialItem = payload,
        )
        log.info(
            "Dispatch trigger: categories={}, batchToken={}, result={}",
            keyCats,
            batchToken,
            result,
        )
    }
}
