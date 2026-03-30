package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.port.BaselineProvider
import com.workflow.dispatch.port.CandidateQueryPort
import com.workflow.dispatch.port.CsvFormatter
import com.workflow.dispatch.port.DispatchConfigRepository
import com.workflow.dispatch.port.SimulationResultStore
import com.workflow.dispatch.port.StoragePort
import com.workflow.dispatch.simulation.SimulationEngine
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DispatchSimulationHandler(
    private val configRepo: DispatchConfigRepository,
    private val candidateQuery: CandidateQueryPort,
    private val baselineProvider: BaselineProvider,
    private val simulationEngine: SimulationEngine,
    private val resultStore: SimulationResultStore,
    private val storage: StoragePort,
    private val csvFormatter: CsvFormatter,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val item = objectMapper.readTree(input.item!!)
        val configId = item["configId"].asText()
        val batchToken = item["batchToken"].asText()

        val config = configRepo.findById(configId)

        val result = simulationEngine.simulate(
            config = config,
            candidates = candidateQuery.queryCandidates(config),
            baseline = baselineProvider.loadBaseline(config),
        )

        resultStore.saveDecisions(batchToken, configId, result.decisions)

        val csv = csvFormatter.format(batchToken, configId, result.decisions)
        storage.uploadCsv("dispatch/$batchToken/simulation/$configId.csv", csv)

        return HandlerOutput(
            objectMapper.writeValueAsString(
                mapOf("configId" to configId, "batchToken" to batchToken),
            ),
        )
    }
}
