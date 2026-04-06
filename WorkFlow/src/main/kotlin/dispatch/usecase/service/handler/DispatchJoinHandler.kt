package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import com.workflow.worker.usecase.port.inbound.trigger.deferK8sJob
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class DispatchJoinHandler(
    private val resultStore: SimulationResultStore,
    private val storage: StorageGateway,
    private val parquetFormatter: ParquetFormatter,
    private val pathBuilder: DispatchPathBuilder,
    @ConfigProperty(name = "dispatch.env", defaultValue = "prod") private val env: String,
    @ConfigProperty(name = "dispatch.k8s.namespace", defaultValue = "default") private val namespace: String,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerResult {
        val inputsNode = objectMapper.readTree(input.inputs!!)
        val batchTokenNode = inputsNode["batchToken"]
        val batchToken =
            when {
                batchTokenNode.isArray -> batchTokenNode[0].asText()
                else -> batchTokenNode.asText()
            }
        val batchStatus = resultStore.findBatchStatus(batchToken)

        if (env == "prod" && batchStatus == BatchStatus.NORMAL) {
            val allDecisions = resultStore.findByBatchToken(batchToken)
            val parquet = parquetFormatter.format(allDecisions)
            storage.uploadParquet(pathBuilder.prodParquetPath(), parquet)
        }

        return deferK8sJob("dispatch-join-$batchToken", namespace)
    }
}
