package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDateTime

@ApplicationScoped
class DispatchScatterHandler(
    private val configRepo: DispatchConfigRepository,
    private val resultStore: SimulationResultStore,
    private val objectMapper: ObjectMapper,
    private val batchTokenProvider: () -> String = { currentBatchToken() },
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val itemNode = input.item?.let { objectMapper.readTree(it) }
        val providedToken = itemNode?.get("batchToken")?.takeIf { !it.isNull }?.asText()
        val configIdsNode = itemNode?.get("configIds")?.takeIf { it.isArray }

        return if (providedToken != null && configIdsNode != null) {
            // Path A — dry-run: batch already created by endpoint, configs supplied explicitly
            val configs = configIdsNode.map { configRepo.findById(it.asText()) }
            val items = configs.map { mapOf("configId" to it.id, "batchToken" to providedToken) }
            HandlerResult.Completed(objectMapper.writeValueAsString(items))
        } else {
            // Path B — cron: generate token, create batch, query all active configs
            val now = LocalDateTime.now()
            val batchToken = batchTokenProvider()
            val configs = configRepo.findActiveConfigs(now)
            resultStore.createBatch(batchToken, BatchStatus.NORMAL, configs.size)
            val items = configs.map { mapOf("configId" to it.id, "batchToken" to batchToken) }
            HandlerResult.Completed(objectMapper.writeValueAsString(items))
        }
    }
}
