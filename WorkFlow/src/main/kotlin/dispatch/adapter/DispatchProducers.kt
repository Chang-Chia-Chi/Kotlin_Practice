package com.workflow.dispatch.adapter

import com.workflow.dispatch.adapter.persistence.JdbiSimulationResultStore
import com.workflow.dispatch.adapter.persistence.SyncRepository
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import io.quarkus.arc.profile.IfBuildProfile
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jdbi.v3.core.Jdbi

/**
 * CDI producers for dispatch-layer beans.
 *
 * Reads `dispatch.env` (default `prod`) to wire environment-specific implementations:
 * - [simulationResultStore]: selects `prod` or `stg` dispatch tables
 * - [dispatchPathBuilder]: constructs MinIO/S3 paths scoped to the active environment
 */
@ApplicationScoped
class DispatchProducers {

    @Produces
    @ApplicationScoped
    fun simulationResultStore(
        @ConfigProperty(name = "dispatch.env", defaultValue = "prod") env: String,
        jdbi: Jdbi,
    ): SimulationResultStore {
        val (batchTable, eventTable) = when (env) {
            "prod" -> "dispatch_batch" to "dispatch_event"
            "stg" -> "dispatch_batch_stg" to "dispatch_event_stg"
            else -> throw IllegalArgumentException("Unknown dispatch.env: $env")
        }
        return JdbiSimulationResultStore(jdbi, batchTable, eventTable)
    }

    @Produces
    @ApplicationScoped
    fun dispatchPathBuilder(
        @ConfigProperty(name = "dispatch.env", defaultValue = "prod") env: String,
    ): DispatchPathBuilder = DispatchPathBuilder(env)

    @Produces
    @ApplicationScoped
    @IfBuildProfile("stg")
    fun syncRepository(jdbi: Jdbi): SyncRepository = SyncRepository(jdbi)
}
