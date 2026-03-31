package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.DispatchDecision

interface SimulationResultStore {
    suspend fun saveDecisions(batchToken: String, configId: String, decisions: List<DispatchDecision>)
    suspend fun findByBatchToken(batchToken: String): List<DispatchDecision>
}
