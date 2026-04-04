package com.workflow.dispatch.adapter.persistence

import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jdbi.v3.core.Jdbi

/**
 * CDI producer for [SimulationResultStore].
 *
 * Reads `dispatch.env` (default `prod`) and wires [JdbiSimulationResultStore]
 * with the correct table names:
 * - `prod` → `dispatch_batch` / `dispatch_event`
 * - `stg`  → `dispatch_batch_stg` / `dispatch_event_stg`
 */
@ApplicationScoped
class DispatchPersistenceProducer {

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
}
