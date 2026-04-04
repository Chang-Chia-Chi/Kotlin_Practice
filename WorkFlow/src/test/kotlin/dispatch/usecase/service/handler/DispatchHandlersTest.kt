package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.model.*
import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.CsvFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import com.workflow.dispatch.usecase.service.simulation.SimulationEngine
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.math.BigDecimal
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DispatchHandlersTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    @Test
    fun `scatter handler returns JSON array of config items`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val resultStore = mock<SimulationResultStore>()
        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

        val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper)
        val output = handler.execute(
            HandlerInput("t1", "w1", 1, null, null),
        )

        assertNotNull(output.result)
        val arr = objectMapper.readTree(output.result)
        assertTrue(arr.isArray)
        assertTrue(arr[0].has("configId"))
        assertTrue(arr[0].has("batchToken"))

        verify(resultStore).createBatch(any(), eq(BatchStatus.NORMAL), eq(1))
        verify(configRepo, never()).findById(any())
    }

    @Test
    fun `scatter handler uses provided configIds and batchToken without creating batch`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val resultStore = mock<SimulationResultStore>()
        val config1 = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        val config2 = DispatchConfig("cfg2", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("B", BigDecimal("200"))), null)
        whenever(configRepo.findById("cfg1")).thenReturn(config1)
        whenever(configRepo.findById("cfg2")).thenReturn(config2)

        val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper)
        val item = objectMapper.writeValueAsString(
            mapOf("batchToken" to "custom-token", "configIds" to listOf("cfg1", "cfg2"))
        )
        val output = handler.execute(
            HandlerInput("t1", "w1", 1, null, item),
        )

        val arr = objectMapper.readTree(output.result)
        assertTrue(arr.isArray)
        assertEquals(2, arr.size())
        assertEquals("cfg1", arr[0]["configId"].asText())
        assertEquals("custom-token", arr[0]["batchToken"].asText())
        assertEquals("cfg2", arr[1]["configId"].asText())
        assertEquals("custom-token", arr[1]["batchToken"].asText())

        verify(configRepo, never()).findActiveConfigs(any())
        verify(resultStore, never()).createBatch(any(), any(), any())
    }

    @Test
    fun `scatter handler creates NORMAL batch and uses all active configs when no item`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val resultStore = mock<SimulationResultStore>()
        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

        val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper,
            batchTokenProvider = { "20260404140000" })
        val output = handler.execute(
            HandlerInput("t1", "w1", 1, null, null),
        )

        val arr = objectMapper.readTree(output.result)
        assertTrue(arr.isArray)
        assertEquals(1, arr.size())
        assertEquals("cfg1", arr[0]["configId"].asText())
        assertEquals("20260404140000", arr[0]["batchToken"].asText())

        verify(configRepo).findActiveConfigs(any())
        verify(configRepo, never()).findById(any())
        verify(resultStore).createBatch(eq("20260404140000"), eq(BatchStatus.NORMAL), eq(1))
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
    fun `simulation handler uploads CSV to env-aware path`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val candidateQuery = mock<CandidateRepository>()
        val baselineProvider = mock<BaselineProvider>()
        val simulationEngine = mock<SimulationEngine>()
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StorageGateway>()
        val csvFormatter = mock<CsvFormatter>()
        val pathBuilder = DispatchPathBuilder("prod")

        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findById("cfg1")).thenReturn(config)
        whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
        whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
        whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
            SimulationResult(emptyList(), emptyMap(), emptyMap()),
        )
        whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())
        whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)

        val handler = DispatchSimulationHandler(
            configRepo, candidateQuery, baselineProvider, simulationEngine,
            resultStore, storage, csvFormatter, pathBuilder, objectMapper,
        )

        val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "20260329060000"))
        handler.execute(HandlerInput("t1", "w1", 2, null, item))

        verify(storage).uploadCsv(eq("env=prod/mode=normal/dispatch/20260329060000/simulation/cfg1.csv.gz"), any())
    }

    @Test
    fun `simulation handler uses dryrun mode path for dryrun batch`() = runTest {
        val configRepo = mock<DispatchConfigRepository>()
        val candidateQuery = mock<CandidateRepository>()
        val baselineProvider = mock<BaselineProvider>()
        val simulationEngine = mock<SimulationEngine>()
        val resultStore = mock<SimulationResultStore>()
        val storage = mock<StorageGateway>()
        val csvFormatter = mock<CsvFormatter>()
        val pathBuilder = DispatchPathBuilder("prod")

        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findById("cfg1")).thenReturn(config)
        whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
        whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
        whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
            SimulationResult(emptyList(), emptyMap(), emptyMap()),
        )
        whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())
        whenever(resultStore.findBatchStatus("dryrun-abc")).thenReturn(BatchStatus.DRYRUN)

        val handler = DispatchSimulationHandler(
            configRepo, candidateQuery, baselineProvider, simulationEngine,
            resultStore, storage, csvFormatter, pathBuilder, objectMapper,
        )

        val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "dryrun-abc"))
        handler.execute(HandlerInput("t1", "w1", 2, null, item))

        verify(storage).uploadCsv(eq("env=prod/mode=dryrun/dispatch/dryrun-abc/simulation/cfg1.csv.gz"), any())
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
