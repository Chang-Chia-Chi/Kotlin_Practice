package com.workflow.dispatch.port

import com.workflow.dispatch.model.CandidateProduct
import com.workflow.dispatch.model.Baseline
import com.workflow.dispatch.model.DispatchConfig
import com.workflow.dispatch.model.DispatchDecision
import java.time.LocalDateTime

interface DispatchConfigRepository {
    suspend fun findActiveConfigs(asOf: LocalDateTime): List<DispatchConfig>
    suspend fun findById(configId: String): DispatchConfig
}

interface CandidateQueryPort {
    suspend fun queryCandidates(config: DispatchConfig): List<CandidateProduct>
}

interface BaselineProvider {
    suspend fun loadBaseline(config: DispatchConfig): Baseline
}

interface SimulationResultStore {
    suspend fun saveDecisions(batchToken: String, configId: String, decisions: List<DispatchDecision>)
    suspend fun findByBatchToken(batchToken: String): List<DispatchDecision>
}

interface StoragePort {
    suspend fun uploadCsv(path: String, content: ByteArray)
    suspend fun uploadParquet(path: String, content: ByteArray)
}

interface CsvFormatter {
    fun format(batchToken: String, configId: String, decisions: List<DispatchDecision>): ByteArray
}

interface ParquetFormatter {
    fun format(decisions: List<DispatchDecision>): ByteArray
}
