package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchCategory
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
    private val clock: BatchTokenClock,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val itemNode = input.taskPayload?.let { objectMapper.readTree(it) }
        val providedToken = itemNode?.get("batchToken")?.takeIf { !it.isNull }?.asText()
        val configIdsNode = itemNode?.get("configIds")?.takeIf { it.isArray }

        val (items, token) = if (providedToken != null && configIdsNode != null) {
            handleDryRun(configIdsNode, providedToken)
        } else {
            val categories = itemNode?.get("categories")
                ?.takeIf { it.isArray }
                ?.map { DispatchCategory.valueOf(it.asText()) }
                ?.toSet()
                ?: emptySet()
            handleCronTrigger(categories)
        }
        return HandlerResult(
            result = objectMapper.writeValueAsString(mapOf("batchToken" to token)),
            fanOutPayloads = items,
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

    // Path B — cron: generate token, create batch, query active configs (optionally filtered)
    private suspend fun handleCronTrigger(
        categories: Set<DispatchCategory>,
    ): Pair<List<String>, String> {
        val token = clock.generate()
        val configs = configRepo.findActiveConfigs(LocalDateTime.now(), categories)
        resultStore.createBatch(token, BatchStatus.NORMAL, configs.size)
        return toItems(configs) to token
    }

    private fun toItems(configs: List<DispatchConfig>): List<String> =
        configs.map { objectMapper.writeValueAsString(mapOf("configId" to it.id)) }
}
