package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.persistence.SyncRepository
import io.quarkus.arc.profile.IfBuildProfile
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType

data class SyncRequest(val configIds: List<String>)
data class SyncResponse(val syncedConfigs: List<String>, val batchesCopied: Int, val eventsCopied: Int)

/**
 * REST endpoint for synchronizing dispatch data from prod tables into stg tables.
 *
 * Gated to the `stg` build profile — not available in production deployments.
 * Delegates to [SyncRepository.syncFromProd] which performs the full sync in a
 * single transaction.
 */
@Path("/dispatch")
@ApplicationScoped
@IfBuildProfile("stg")
class DispatchSyncResource(
    private val syncRepository: SyncRepository,
) {

    @POST
    @Path("/sync")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    suspend fun sync(request: SyncRequest): SyncResponse {
        val result = syncRepository.syncFromProd(request.configIds)
        return SyncResponse(
            syncedConfigs = result.syncedConfigs,
            batchesCopied = result.batchesCopied,
            eventsCopied = result.eventsCopied,
        )
    }
}
