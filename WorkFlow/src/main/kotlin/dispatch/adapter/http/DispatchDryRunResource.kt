package com.workflow.dispatch.adapter.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.workflow.usecase.port.inbound.orchestration.WorkflowLifecycle
import io.quarkus.arc.profile.IfBuildProfile
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import java.time.LocalDateTime
import java.util.UUID

data class DryRunRequest(val configIds: List<String>? = null)
data class DryRunResponse(val batchToken: String, val status: String)

@Path("/dispatch")
@ApplicationScoped
@IfBuildProfile("prod")
class DispatchDryRunResource(
    private val resultStore: SimulationResultStore,
    private val workflowEngine: WorkflowLifecycle,
    private val configRepo: DispatchConfigRepository,
    private val objectMapper: ObjectMapper,
) {

    @POST
    @Path("/dryrun")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    suspend fun dryRun(request: DryRunRequest): DryRunResponse {
        val batchToken = UUID.randomUUID().toString()

        val configIds = request.configIds
            ?: configRepo.findActiveConfigs(LocalDateTime.now()).map { it.id }

        if (configIds.isEmpty()) throw BadRequestException("No dispatch configs to dry-run")

        resultStore.createBatch(batchToken, BatchStatus.DRYRUN, configIds.size)

        val initialItem = objectMapper.writeValueAsString(
            mapOf("batchToken" to batchToken, "configIds" to configIds),
        )

        workflowEngine.startWorkflow(
            definition = dispatchWorkflow,
            idempotencyKey = "dispatch-dryrun-$batchToken",
            initialItem = initialItem,
        )

        return DryRunResponse(batchToken = batchToken, status = "DRYRUN")
    }
}
