package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchDecision

interface SimulationResultStore {
    suspend fun createBatch(batchToken: String, status: BatchStatus, configCount: Int)
    suspend fun findBatchStatus(batchToken: String): BatchStatus
    suspend fun saveDecisions(batchToken: String, configId: String, decisions: List<DispatchDecision>)
    suspend fun findByBatchToken(batchToken: String): List<DispatchDecision>
    suspend fun findByBatchTokenAndConfigs(batchToken: String, configIds: List<String>): List<DispatchDecision>
}
