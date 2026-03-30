package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.port.DispatchConfigRepository
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

@ApplicationScoped
class DispatchScatterHandler(
    private val configRepo: DispatchConfigRepository,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val now = LocalDateTime.now()
        val batchToken = now.truncatedTo(ChronoUnit.HOURS)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        val configs = configRepo.findActiveConfigs(now)
        val items = configs.map { mapOf("configId" to it.id, "batchToken" to batchToken) }

        return HandlerOutput(objectMapper.writeValueAsString(items))
    }
}
