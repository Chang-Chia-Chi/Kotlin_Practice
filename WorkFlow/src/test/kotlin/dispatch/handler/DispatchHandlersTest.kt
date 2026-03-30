package com.workflow.dispatch.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.*
import com.workflow.dispatch.port.*
import com.workflow.dispatch.simulation.SimulationEngine
import com.workflow.worker.HandlerInput
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
        val candidateQuery = mock<CandidateQueryPort>()
        val baselineProvider = mock<BaselineProvider>()
        val simulationEngine = mock<SimulationEngine>()
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StoragePort>()
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

        val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "2026-03-29T06:00:00"))
        val output = handler.execute(
            HandlerInput("t1", "w1", 2, null, item),
        )

        verify(resultStore).saveDecisions(eq("2026-03-29T06:00:00"), eq("cfg1"), any())
        verify(storage).uploadCsv(eq("dispatch/2026-03-29T06:00:00/simulation/cfg1.csv"), any())
        assertNotNull(output.result)
    }

    @Test
    fun `join handler uploads parquet with merged results`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StoragePort>()
        val parquetFormatter = mock<ParquetFormatter>()

        whenever(resultStore.findByBatchToken("2026-03-29T06:00:00")).thenReturn(emptyList())
        whenever(parquetFormatter.format(any())).thenReturn(byteArrayOf())

        val handler = DispatchJoinHandler(resultStore, storage, parquetFormatter, objectMapper)

        // Simulate aggregated input from parallel simulate tasks
        val inputs = objectMapper.writeValueAsString(
            mapOf("batchToken" to listOf("2026-03-29T06:00:00", "2026-03-29T06:00:00")),
        )
        handler.execute(
            HandlerInput("t1", "w1", 3, inputs, null),
        )

        verify(storage).uploadParquet(eq("dispatch/2026-03-29T06:00:00/result.parquet"), any())
    }
}
