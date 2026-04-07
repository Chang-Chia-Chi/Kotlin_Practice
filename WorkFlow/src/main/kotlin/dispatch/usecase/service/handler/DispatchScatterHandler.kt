package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchConfig
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
    private val clock: BatchTokenClock = SystemBatchTokenClock(),
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val itemNode = input.item?.let { objectMapper.readTree(it) }
        val providedToken = itemNode?.get("batchToken")?.takeIf { !it.isNull }?.asText()
        val configIdsNode = itemNode?.get("configIds")?.takeIf { it.isArray }

        val (items, token) = if (providedToken != null && configIdsNode != null) {
            handleDryRun(configIdsNode, providedToken)
        } else {
            handleCronTrigger()
        }
        return HandlerResult.Completed(
            result = objectMapper.writeValueAsString(mapOf("batchToken" to token)),
            items = items,
        )
    }

    // Path A — dry-run: batch already created by endpoint, configs supplied explicitly
    private suspend fun handleDryRun(
        configIdsNode: JsonNode,
        token: String,
    ): Pair<List<String>, String> {
        val configs = configIdsNode.map { configRepo.findById(it.asText()) }
        return toItems(configs) to token
    }

    // Path B — cron: generate token, create batch, query all active configs
    private suspend fun handleCronTrigger(): Pair<List<String>, String> {
        val token = clock.generate()
        val configs = configRepo.findActiveConfigs(LocalDateTime.now())
        resultStore.createBatch(token, BatchStatus.NORMAL, configs.size)
        return toItems(configs) to token
    }

    private fun toItems(configs: List<DispatchConfig>): List<String> =
        configs.map { objectMapper.writeValueAsString(mapOf("configId" to it.id)) }
}
