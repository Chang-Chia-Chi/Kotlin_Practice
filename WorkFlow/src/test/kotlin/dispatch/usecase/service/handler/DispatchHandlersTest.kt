package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.*
import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.CsvFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import com.workflow.dispatch.usecase.service.handler.DispatchJoinHandler
import com.workflow.dispatch.usecase.service.handler.DispatchScatterHandler
import com.workflow.dispatch.usecase.service.handler.DispatchSimulationHandler
import com.workflow.dispatch.usecase.service.simulation.SimulationEngine
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.math.BigDecimal
import java.time.LocalDateTime
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DispatchHandlersTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    @Test
    fun `scatter handler returns JSON array of config items`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

        val handler = DispatchScatterHandler(configRepo, objectMapper)
        val output = handler.execute(
            HandlerInput("t1", "w1", 1, null, null),
        )

        assertNotNull(output.result)
        val arr = objectMapper.readTree(output.result)
        assertTrue(arr.isArray)
        assertTrue(arr[0].has("configId"))
        assertTrue(arr[0].has("batchToken"))
    }

    @Test
    fun `simulation handler calls engine and uploads CSV`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val candidateQuery = mock<CandidateRepository>()
        val baselineProvider = mock<BaselineProvider>()
        val simulationEngine = mock<SimulationEngine>()
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StorageGateway>()
        val csvFormatter = mock<CsvFormatter>()

        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findById("cfg1")).thenReturn(config)
        whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
        whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
        whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
            SimulationResult(emptyList(), emptyMap(), emptyMap()),
        )
        whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())

        val handler = DispatchSimulationHandler(
            configRepo, candidateQuery, baselineProvider, simulationEngine,
            resultStore, storage, csvFormatter, objectMapper,
        )

        val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "20260329060000"))
        val output = handler.execute(
            HandlerInput("t1", "w1", 2, null, item),
        )

        verify(resultStore).saveDecisions(eq("20260329060000"), eq("cfg1"), any())
        verify(storage).uploadCsv(eq("dispatch/20260329060000/simulation/cfg1.csv.gz"), any())
        assertNotNull(output.result)
    }

    @Test
    fun `join handler uploads parquet with merged results`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StorageGateway>()
        val parquetFormatter = mock<ParquetFormatter>()

        whenever(resultStore.findByBatchToken("20260329060000")).thenReturn(emptyList())
        whenever(parquetFormatter.format(any())).thenReturn(byteArrayOf())

        val handler = DispatchJoinHandler(resultStore, storage, parquetFormatter, objectMapper)

        // Simulate aggregated input from parallel simulate tasks
        val inputs = objectMapper.writeValueAsString(
            mapOf("batchToken" to listOf("20260329060000", "20260329060000")),
        )
        handler.execute(
            HandlerInput("t1", "w1", 3, inputs, null),
        )

        verify(storage).uploadParquet(eq("dispatch/20260329060000/result.parquet"), any())
    }
}
