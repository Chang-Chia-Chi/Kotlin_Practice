package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.port.ParquetFormatter
import com.workflow.dispatch.port.SimulationResultStore
import com.workflow.dispatch.port.StoragePort
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DispatchJoinHandler(
    private val resultStore: SimulationResultStore,
    private val storage: StoragePort,
    private val parquetFormatter: ParquetFormatter,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val inputsNode = objectMapper.readTree(input.inputs!!)
        val batchTokenNode = inputsNode["batchToken"]
        // InputResolver aggregates parallel task outputs into an array
        val batchToken = if (batchTokenNode.isArray) {
            batchTokenNode[0].asText()
        } else {
            batchTokenNode.asText()
        }

        val allDecisions = resultStore.findByBatchToken(batchToken)
        val parquet = parquetFormatter.format(allDecisions)
        storage.uploadParquet("dispatch/$batchToken/result.parquet", parquet)

        return HandlerOutput(null)
    }
}
