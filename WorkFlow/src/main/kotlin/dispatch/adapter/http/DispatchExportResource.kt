package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import io.quarkus.arc.profile.IfBuildProfile
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType

data class ExportRequest(val batchToken: String, val configIds: List<String>? = null)
data class ExportResponse(val batchToken: String, val exportedConfigs: List<String>, val path: String)

/**
 * Stg-only endpoint that re-exports existing batch simulation results as Parquet.
 *
 * Reads decisions from the stg result store, formats via [ParquetFormatter],
 * and uploads to MinIO via [StorageGateway].
 */
@Path("/dispatch")
@ApplicationScoped
@IfBuildProfile("stg")
class DispatchExportResource(
    private val resultStore: SimulationResultStore,
    private val parquetFormatter: ParquetFormatter,
    private val storage: StorageGateway,
    private val pathBuilder: DispatchPathBuilder,
) {

    @POST
    @Path("/export")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    suspend fun export(request: ExportRequest): ExportResponse {
        val decisions = if (request.configIds != null) {
            resultStore.findByBatchTokenAndConfigs(request.batchToken, request.configIds)
        } else {
            resultStore.findByBatchToken(request.batchToken)
        }

        val parquet = parquetFormatter.format(decisions)
        val path = pathBuilder.batchParquetPath(request.batchToken)
        storage.uploadParquet(path, parquet)

        return ExportResponse(
            batchToken = request.batchToken,
            exportedConfigs = request.configIds ?: listOf("all"),
            path = path,
        )
    }
}
