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
) : TransitionHandler {
    private var batchTokenProvider: () -> String = { currentBatchToken() }

    internal fun setBatchTokenProvider(provider: () -> String) {
        this.batchTokenProvider = provider
    }

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val itemNode = input.item?.let { objectMapper.readTree(it) }
        val providedToken = itemNode?.get("batchToken")?.takeIf { !it.isNull }?.asText()
        val configIdsNode = itemNode?.get("configIds")?.takeIf { it.isArray }

        val (items, token) =
            if (providedToken != null && configIdsNode != null) {
                // Path A — dry-run: batch already created by endpoint, configs supplied explicitly
                val configs = configIdsNode.map { configRepo.findById(it.asText()) }
                configs.map { mapOf("configId" to it.id, "batchToken" to providedToken) } to providedToken
            } else {
                // Path B — cron: generate token, create batch, query all active configs
                val now = LocalDateTime.now()
                val batchToken = batchTokenProvider()
                val configs = configRepo.findActiveConfigs(now)
                resultStore.createBatch(batchToken, BatchStatus.NORMAL, configs.size)
                configs.map { mapOf("configId" to it.id, "batchToken" to batchToken) } to batchToken
            }
        return HandlerResult.Completed(
            result = objectMapper.writeValueAsString(mapOf("batchToken" to token)),
            items = objectMapper.writeValueAsString(items),
        )
    }
}
