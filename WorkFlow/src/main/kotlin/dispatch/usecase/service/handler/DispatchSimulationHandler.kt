package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.CsvFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import com.workflow.dispatch.usecase.service.simulation.SimulationEngine
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.nio.file.Files
import java.util.zip.GZIPOutputStream

@ApplicationScoped
class DispatchSimulationHandler(
    private val configRepo: DispatchConfigRepository,
    private val candidateQuery: CandidateRepository,
    private val baselineProvider: BaselineProvider,
    private val simulationEngine: SimulationEngine,
    private val resultStore: SimulationResultStore,
    private val storage: StorageGateway,
    private val csvFormatter: CsvFormatter,
    private val pathBuilder: DispatchPathBuilder,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerResult {
        val payload = objectMapper.readTree(
            requireNotNull(input.taskPayload) { "DispatchSimulationHandler requires a fan-out taskPayload; check workflow definition" }
        )
        val configId = payload["configId"].asText()
        val batchToken = payload["batchToken"].asText()

        val config = configRepo.findById(configId)

        val result =
            simulationEngine.simulate(
                config = config,
                candidates = candidateQuery.queryCandidates(config),
                baseline = baselineProvider.loadBaseline(config),
            )

        resultStore.saveDecisions(batchToken, configId, result.decisions)

        val batchStatus = resultStore.findBatchStatus(batchToken)
        val csvPath = pathBuilder.csvPath(batchStatus, batchToken, configId)

        val csv = csvFormatter.format(batchToken, configId, result.decisions)
        val tmpFile = Files.createTempFile("dispatch-$configId-", ".csv.gz").toFile()
        try {
            GZIPOutputStream(tmpFile.outputStream()).use { it.write(csv) }
            storage.uploadCsv(csvPath, tmpFile)
        } finally {
            tmpFile.delete()
        }

        return HandlerResult.Completed(null)
    }
}
