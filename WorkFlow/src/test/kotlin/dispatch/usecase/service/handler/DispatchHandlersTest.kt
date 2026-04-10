package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.usecase.service.handler.BatchTokenClock
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
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DispatchHandlersTest {
    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    @Test
    fun `scatter handler returns JSON array of config items`() =
        runTest {
            val configRepo = mock<DispatchConfigRepository>()
            val resultStore = mock<SimulationResultStore>()
            val config =
                DispatchConfig(
                    "cfg1",
                    DispatchMode.QTY,
                    "default",
                    "bom",
                    listOf(SiteTarget("A", BigDecimal("100"))),
                    null,
                )
            whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

            val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper, SystemBatchTokenClock())
            val output =
                handler.execute(
                    HandlerInput("t1", "w1", 1, null, null),
                ) as HandlerResult.Completed

            assertNotNull(output.fanOutPayloads)
            assertEquals(listOf("""{"configId":"cfg1"}"""), output.fanOutPayloads)

            assertNotNull(output.result)
            assertNotNull(objectMapper.readTree(output.result)["batchToken"].asText())

            verify(resultStore).createBatch(any(), eq(BatchStatus.NORMAL), eq(1))
            verify(configRepo, never()).findById(any())
        }

    @Test
    fun `scatter handler uses provided configIds and batchToken without creating batch`() =
        runTest {
            val configRepo = mock<DispatchConfigRepository>()
            val resultStore = mock<SimulationResultStore>()
            val config1 =
                DispatchConfig(
                    "cfg1",
                    DispatchMode.QTY,
                    "default",
                    "bom",
                    listOf(SiteTarget("A", BigDecimal("100"))),
                    null,
                )
            val config2 =
                DispatchConfig(
                    "cfg2",
                    DispatchMode.QTY,
                    "default",
                    "bom",
                    listOf(SiteTarget("B", BigDecimal("200"))),
                    null,
                )
            whenever(configRepo.findById("cfg1")).thenReturn(config1)
            whenever(configRepo.findById("cfg2")).thenReturn(config2)

            val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper, SystemBatchTokenClock())
            val item =
                objectMapper.writeValueAsString(
                    mapOf("batchToken" to "custom-token", "configIds" to listOf("cfg1", "cfg2")),
                )
            val output =
                handler.execute(
                    HandlerInput("t1", "w1", 1, null, item),
                ) as HandlerResult.Completed

            assertNotNull(output.fanOutPayloads)
            assertEquals(listOf("""{"configId":"cfg1"}""", """{"configId":"cfg2"}"""), output.fanOutPayloads)

            assertNotNull(output.result)
            assertEquals("custom-token", objectMapper.readTree(output.result)["batchToken"].asText())

            verify(configRepo, never()).findActiveConfigs(any())
            verify(resultStore, never()).createBatch(any(), any(), any())
        }

    @Test
    fun `scatter handler creates NORMAL batch and uses all active configs when no item`() =
        runTest {
            val configRepo = mock<DispatchConfigRepository>()
            val resultStore = mock<SimulationResultStore>()
            val config =
                DispatchConfig(
                    "cfg1",
                    DispatchMode.QTY,
                    "default",
                    "bom",
                    listOf(SiteTarget("A", BigDecimal("100"))),
                    null,
                )
            whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

            val handler =
                DispatchScatterHandler(configRepo, resultStore, objectMapper, BatchTokenClock { "20260404140000" })
            val output =
                handler.execute(
                    HandlerInput("t1", "w1", 1, null, null),
                ) as HandlerResult.Completed

            assertNotNull(output.fanOutPayloads)
            assertEquals(listOf("""{"configId":"cfg1"}"""), output.fanOutPayloads)

            assertNotNull(output.result)
            assertEquals("20260404140000", objectMapper.readTree(output.result)["batchToken"].asText())

            verify(configRepo).findActiveConfigs(any())
            verify(configRepo, never()).findById(any())
            verify(resultStore).createBatch(eq("20260404140000"), eq(BatchStatus.NORMAL), eq(1))
        }

    @Test
    fun `simulation handler throws descriptive error when item is null`() =
        runTest {
            val handler =
                DispatchSimulationHandler(
                    mock(), mock(), mock(), mock(), mock(), mock(), mock(),
                    DispatchPathBuilder("prod"),
                    objectMapper,
                )
            val ex = assertFailsWith<IllegalArgumentException> {
                handler.execute(HandlerInput("t1", "w1", 2, null, null))
            }
            assertTrue(ex.message!!.contains("DispatchSimulationHandler"))
        }

    @Test
    fun `simulation handler calls engine and uploads CSV`() =
        runTest {
            val configRepo = mock<DispatchConfigRepository>()
            val candidateQuery = mock<CandidateRepository>()
            val baselineProvider = mock<BaselineProvider>()
            val simulationEngine = mock<SimulationEngine>()
            val resultStore = mock<SimulationResultStore>()
            val storage = mock<StorageGateway>()
            val csvFormatter = mock<CsvFormatter>()
            val pathBuilder = DispatchPathBuilder("prod")

            val config =
                DispatchConfig(
                    "cfg1",
                    DispatchMode.QTY,
                    "default",
                    "bom",
                    listOf(SiteTarget("A", BigDecimal("100"))),
                    null,
                )
            whenever(configRepo.findById("cfg1")).thenReturn(config)
            whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
            whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
            whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
                SimulationResult(emptyList(), emptyMap(), emptyMap()),
            )
            whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())
            whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)

            val handler =
                DispatchSimulationHandler(
                    configRepo,
                    candidateQuery,
                    baselineProvider,
                    simulationEngine,
                    resultStore,
                    storage,
                    csvFormatter,
                    pathBuilder,
                    objectMapper,
                )

            val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "20260329060000"))
            val output =
                handler.execute(
                    HandlerInput("t1", "w1", 2, null, item),
                ) as HandlerResult.Completed

            val order = inOrder(resultStore)
            order.verify(resultStore).saveDecisions(eq("20260329060000"), eq("cfg1"), any())
            order.verify(resultStore).findBatchStatus(eq("20260329060000"))
            verify(storage).uploadCsv(eq("env=prod/mode=normal/dispatch/20260329060000/simulation/cfg1.csv.gz"), any())
            assertNull(output.result)
        }

    @Test
    fun `simulation handler uses env from pathBuilder in CSV path`() =
        runTest {
            val configRepo = mock<DispatchConfigRepository>()
            val candidateQuery = mock<CandidateRepository>()
            val baselineProvider = mock<BaselineProvider>()
            val simulationEngine = mock<SimulationEngine>()
            val resultStore = mock<SimulationResultStore>()
            val storage = mock<StorageGateway>()
            val csvFormatter = mock<CsvFormatter>()
            val pathBuilder = DispatchPathBuilder("staging")

            val config =
                DispatchConfig(
                    "cfg1",
                    DispatchMode.QTY,
                    "default",
                    "bom",
                    listOf(SiteTarget("A", BigDecimal("100"))),
                    null,
                )
            whenever(configRepo.findById("cfg1")).thenReturn(config)
            whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
            whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
            whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
                SimulationResult(emptyList(), emptyMap(), emptyMap()),
            )
            whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())
            whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)

            val handler =
                DispatchSimulationHandler(
                    configRepo,
                    candidateQuery,
                    baselineProvider,
                    simulationEngine,
                    resultStore,
                    storage,
                    csvFormatter,
                    pathBuilder,
                    objectMapper,
                )

            val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "20260329060000"))
            handler.execute(HandlerInput("t1", "w1", 2, null, item))

            verify(storage).uploadCsv(eq("env=staging/mode=normal/dispatch/20260329060000/simulation/cfg1.csv.gz"), any())
        }

    @Test
    fun `simulation handler uses dryrun mode path for dryrun batch`() =
        runTest {
            val configRepo = mock<DispatchConfigRepository>()
            val candidateQuery = mock<CandidateRepository>()
            val baselineProvider = mock<BaselineProvider>()
            val simulationEngine = mock<SimulationEngine>()
            val resultStore = mock<SimulationResultStore>()
            val storage = mock<StorageGateway>()
            val csvFormatter = mock<CsvFormatter>()
            val pathBuilder = DispatchPathBuilder("prod")

            val config =
                DispatchConfig(
                    "cfg1",
                    DispatchMode.QTY,
                    "default",
                    "bom",
                    listOf(SiteTarget("A", BigDecimal("100"))),
                    null,
                )
            whenever(configRepo.findById("cfg1")).thenReturn(config)
            whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
            whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
            whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
                SimulationResult(emptyList(), emptyMap(), emptyMap()),
            )
            whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())
            whenever(resultStore.findBatchStatus("dryrun-abc")).thenReturn(BatchStatus.DRYRUN)

            val handler =
                DispatchSimulationHandler(
                    configRepo,
                    candidateQuery,
                    baselineProvider,
                    simulationEngine,
                    resultStore,
                    storage,
                    csvFormatter,
                    pathBuilder,
                    objectMapper,
                )

            val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "dryrun-abc"))
            handler.execute(HandlerInput("t1", "w1", 2, null, item))

            verify(storage).uploadCsv(eq("env=prod/mode=dryrun/dispatch/dryrun-abc/simulation/cfg1.csv.gz"), any())
        }

    @Test
    fun `join handler uploads parquet with merged results`() =
        runTest {
            val resultStore = mock<SimulationResultStore>()
            val storage = mock<StorageGateway>()
            val parquetFormatter = mock<ParquetFormatter>()
            val pathBuilder = DispatchPathBuilder("prod")

            whenever(resultStore.findByBatchToken("20260329060000")).thenReturn(emptyList())
            whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)
            whenever(parquetFormatter.format(any())).thenReturn(byteArrayOf())

            val handler =
                DispatchJoinHandler(
                    resultStore,
                    storage,
                    parquetFormatter,
                    pathBuilder,
                    "prod",
                    objectMapper,
                )

            val inputs =
                objectMapper.writeValueAsString(
                    mapOf("batchToken" to listOf("20260329060000", "20260329060000")),
                )
            val result =
                handler.execute(
                    HandlerInput("t1", "w1", 3, inputs, null),
                )

            verify(storage).uploadParquet(eq("env=prod/dispatch/result.parquet"), any())
            assertTrue(result is HandlerResult.Completed)
            assertNull((result as HandlerResult.Completed).result)
        }

    @Test
    fun `join handler skips parquet for prod dryrun batch`() =
        runTest {
            val resultStore = mock<SimulationResultStore>()
            val storage = mock<StorageGateway>()
            val parquetFormatter = mock<ParquetFormatter>()
            val pathBuilder = DispatchPathBuilder("prod")

            whenever(resultStore.findBatchStatus("dryrun-abc")).thenReturn(BatchStatus.DRYRUN)

            val handler =
                DispatchJoinHandler(
                    resultStore,
                    storage,
                    parquetFormatter,
                    pathBuilder,
                    "prod",
                    objectMapper,
                )

            val inputs =
                objectMapper.writeValueAsString(
                    mapOf("batchToken" to listOf("dryrun-abc", "dryrun-abc")),
                )
            val result = handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

            verify(storage, never()).uploadParquet(any(), any())
            verify(parquetFormatter, never()).format(any())
            assertTrue(result is HandlerResult.Completed)
            assertNull((result as HandlerResult.Completed).result)
        }

    @Test
    fun `join handler skips parquet for stg env`() =
        runTest {
            val resultStore = mock<SimulationResultStore>()
            val storage = mock<StorageGateway>()
            val parquetFormatter = mock<ParquetFormatter>()
            val pathBuilder = DispatchPathBuilder("stg")

            whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)

            val handler =
                DispatchJoinHandler(
                    resultStore,
                    storage,
                    parquetFormatter,
                    pathBuilder,
                    "stg",
                    objectMapper,
                )

            val inputs =
                objectMapper.writeValueAsString(
                    mapOf("batchToken" to listOf("20260329060000", "20260329060000")),
                )
            val result = handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

            verify(resultStore).findBatchStatus("20260329060000")
            verify(storage, never()).uploadParquet(any(), any())
            verify(parquetFormatter, never()).format(any())
            assertTrue(result is HandlerResult.Completed)
            assertNull((result as HandlerResult.Completed).result)
        }
}
