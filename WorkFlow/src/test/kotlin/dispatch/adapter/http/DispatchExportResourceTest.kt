package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.math.BigDecimal
import kotlin.test.assertEquals

class DispatchExportResourceTest {

    private val resultStore = mock<SimulationResultStore>()
    private val parquetFormatter = mock<ParquetFormatter>()
    private val storage = mock<StorageGateway>()
    private val pathBuilder = DispatchPathBuilder("stg")
    private val resource = DispatchExportResource(resultStore, parquetFormatter, storage, pathBuilder)

    private val sampleDecisions = listOf(
        DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
    )

    // ── Happy path: null configIds → whole batch ─────────────────────────

    @Test
    fun `export uploads parquet for whole batch when no configIds`() = runTest {
        whenever(resultStore.findByBatchToken("batch1")).thenReturn(sampleDecisions)
        whenever(parquetFormatter.format(sampleDecisions)).thenReturn(byteArrayOf(1, 2, 3))

        val response = resource.export(ExportRequest(batchToken = "batch1", configIds = null))

        verify(resultStore).findByBatchToken("batch1")
        verify(resultStore, never()).findByBatchTokenAndConfigs(any(), any())
        verify(storage).uploadParquet(eq("env=stg/dispatch/batch1/result.parquet"), any())
        assertEquals("batch1", response.batchToken)
        assertEquals("env=stg/dispatch/batch1/result.parquet", response.path)
    }

    // ── Happy path: specific configIds ───────────────────────────────────

    @Test
    fun `export uploads parquet for specified configs only`() = runTest {
        whenever(resultStore.findByBatchTokenAndConfigs("batch1", listOf("cfg1"))).thenReturn(sampleDecisions)
        whenever(parquetFormatter.format(sampleDecisions)).thenReturn(byteArrayOf(4, 5))

        val response = resource.export(ExportRequest(batchToken = "batch1", configIds = listOf("cfg1")))

        verify(resultStore).findByBatchTokenAndConfigs("batch1", listOf("cfg1"))
        verify(resultStore, never()).findByBatchToken(any())
        verify(storage).uploadParquet(eq("env=stg/dispatch/batch1/result.parquet"), any())
        assertEquals("batch1", response.batchToken)
    }

    // ── Data flow: formatter output → storage ────────────────────────────

    @Test
    fun `export passes formatter output to storage`() = runTest {
        val parquetBytes = byteArrayOf(10, 20, 30, 40)
        whenever(resultStore.findByBatchToken("batch2")).thenReturn(sampleDecisions)
        whenever(parquetFormatter.format(sampleDecisions)).thenReturn(parquetBytes)

        resource.export(ExportRequest(batchToken = "batch2", configIds = null))

        verify(storage).uploadParquet(
            eq("env=stg/dispatch/batch2/result.parquet"),
            eq(parquetBytes),
        )
    }

    // ── Response fields ──────────────────────────────────────────────────

    @Test
    fun `export response contains correct path and batch token`() = runTest {
        whenever(resultStore.findByBatchTokenAndConfigs("b3", listOf("c1", "c2"))).thenReturn(sampleDecisions)
        whenever(parquetFormatter.format(sampleDecisions)).thenReturn(byteArrayOf(1))

        val response = resource.export(ExportRequest(batchToken = "b3", configIds = listOf("c1", "c2")))

        assertEquals("b3", response.batchToken)
        assertEquals("env=stg/dispatch/b3/result.parquet", response.path)
        assertEquals(listOf("c1", "c2"), response.exportedConfigs)
    }
}
