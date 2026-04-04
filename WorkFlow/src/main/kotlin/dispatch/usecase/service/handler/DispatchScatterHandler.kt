package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDateTime

@ApplicationScoped
class DispatchScatterHandler(
    private val configRepo: DispatchConfigRepository,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val now = LocalDateTime.now()
        val batchToken = currentBatchToken(now)

        val configs = configRepo.findActiveConfigs(now)
        val items = configs.map { mapOf("configId" to it.id, "batchToken" to batchToken) }

        return HandlerOutput(objectMapper.writeValueAsString(items))
    }
}
