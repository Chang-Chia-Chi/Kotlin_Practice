package com.workflow.dispatch.adapter.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchCategory
import com.workflow.dispatch.model.DispatchConfig
import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.workflow.model.StartResult
import com.workflow.workflow.usecase.port.inbound.orchestration.WorkflowLifecycle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.math.BigDecimal
import jakarta.ws.rs.BadRequestException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DispatchDryRunResourceTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    // -------------------------------------------------------------------------
    // Happy path: explicit configIds provided
    // -------------------------------------------------------------------------

    @Test
    fun `dryrun with explicit configIds creates DRYRUN batch and starts workflow`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        whenever(workflowEngine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val response = resource.dryRun(DryRunRequest(configIds = listOf("cfg1", "cfg2")))

        // batch token must be a 36-char UUID string
        assertEquals(36, response.batchToken.length)
        assertEquals("DRYRUN", response.status)

        verify(resultStore).createBatch(
            eq(response.batchToken),
            eq(BatchStatus.DRYRUN),
            eq(2),
        )
        verify(workflowEngine).startWorkflow(
            definition = any(),
            idempotencyKey = argThat { startsWith("dispatch-dryrun-") },
            initialItem = argThat { contains("cfg1") && contains("cfg2") },
        )
        // configRepo must NOT be queried when configIds is provided
        verify(configRepo, never()).findActiveConfigs(any())
    }

    @Test
    fun `dryrun idempotencyKey embeds the batchToken`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        whenever(workflowEngine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val response = resource.dryRun(DryRunRequest(configIds = listOf("cfg1")))

        val expectedKey = "dispatch-dryrun-${response.batchToken}"
        verify(workflowEngine).startWorkflow(
            definition = any(),
            idempotencyKey = eq(expectedKey),
            initialItem = any(),
        )
    }

    @Test
    fun `dryrun initialItem JSON contains batchToken and configIds`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        whenever(workflowEngine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val response = resource.dryRun(DryRunRequest(configIds = listOf("cfgA", "cfgB")))

        verify(workflowEngine).startWorkflow(
            definition = any(),
            idempotencyKey = any(),
            initialItem = argThat {
                val node = objectMapper.readTree(this)
                node.has("batchToken") &&
                    node.get("batchToken").asText() == response.batchToken &&
                    node.has("configIds") &&
                    node.get("configIds").map { it.asText() }.containsAll(listOf("cfgA", "cfgB"))
            },
        )
    }

    // -------------------------------------------------------------------------
    // Edge case: null configIds → fall back to configRepo
    // -------------------------------------------------------------------------

    @Test
    fun `dryrun with null configIds queries active configs from repo`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        val config = DispatchConfig(
            id = "cfg1",
            category = DispatchCategory.NORMAL,
            mode = DispatchMode.QTY,
            algorithmId = "default",
            sourceBomPrefix = "bom",
            siteTargets = listOf(SiteTarget("A", BigDecimal("100"))),
            bomMappings = null,
        )
        whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))
        whenever(workflowEngine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val response = resource.dryRun(DryRunRequest(configIds = null))

        verify(configRepo).findActiveConfigs(any())
        verify(resultStore).createBatch(any(), eq(BatchStatus.DRYRUN), eq(1))
        verify(workflowEngine).startWorkflow(
            definition = any(),
            idempotencyKey = any(),
            initialItem = argThat { contains("cfg1") },
        )
        assertTrue(response.batchToken.isNotEmpty())
        assertEquals("DRYRUN", response.status)
    }

    @Test
    fun `dryrun with null configIds passes mapped IDs as initialItem`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        val configs = listOf(
            DispatchConfig(
                id = "id1",
                category = DispatchCategory.NORMAL,
                mode = DispatchMode.QTY,
                algorithmId = "default",
                sourceBomPrefix = "bom",
                siteTargets = listOf(SiteTarget("A", BigDecimal("10"))),
                bomMappings = null,
            ),
            DispatchConfig(
                id = "id2",
                category = DispatchCategory.NORMAL,
                mode = DispatchMode.RATIO,
                algorithmId = "default",
                sourceBomPrefix = "bom",
                siteTargets = listOf(SiteTarget("B", BigDecimal("50"))),
                bomMappings = null,
            ),
        )
        whenever(configRepo.findActiveConfigs(any())).thenReturn(configs)
        whenever(workflowEngine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w2"))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        resource.dryRun(DryRunRequest(configIds = null))

        verify(resultStore).createBatch(any(), eq(BatchStatus.DRYRUN), eq(2))
        verify(workflowEngine).startWorkflow(
            definition = any(),
            idempotencyKey = any(),
            initialItem = argThat {
                val node = objectMapper.readTree(this)
                val ids = node.get("configIds").map { it.asText() }
                ids.containsAll(listOf("id1", "id2"))
            },
        )
    }

    // -------------------------------------------------------------------------
    // Edge case: empty active configs list
    // -------------------------------------------------------------------------

    @Test
    fun `dryrun with null configIds and empty active configs raises BadRequestException`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        whenever(configRepo.findActiveConfigs(any())).thenReturn(emptyList())

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        assertFailsWith<BadRequestException> {
            resource.dryRun(DryRunRequest(configIds = null))
        }
        verify(resultStore, never()).createBatch(any(), any(), any())
        verify(workflowEngine, never()).startWorkflow(any(), any(), any())
    }

    // -------------------------------------------------------------------------
    // Edge case: empty explicit configIds list
    // -------------------------------------------------------------------------

    @Test
    fun `dryrun with explicit empty configIds raises BadRequestException`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        assertFailsWith<BadRequestException> {
            resource.dryRun(DryRunRequest(configIds = emptyList()))
        }
        verify(configRepo, never()).findActiveConfigs(any())
        verify(resultStore, never()).createBatch(any(), any(), any())
        verify(workflowEngine, never()).startWorkflow(any(), any(), any())
    }

    // -------------------------------------------------------------------------
    // Error: workflowEngine returns AlreadyExists (idempotent duplicate)
    // -------------------------------------------------------------------------

    @Test
    fun `dryrun still returns DRYRUN response when workflow already exists`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        whenever(workflowEngine.startWorkflow(any(), any(), any()))
            .thenReturn(StartResult.AlreadyExists("existing-w1"))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val response = resource.dryRun(DryRunRequest(configIds = listOf("cfg1")))

        assertNotNull(response.batchToken)
        assertEquals("DRYRUN", response.status)
        verify(resultStore).createBatch(any(), eq(BatchStatus.DRYRUN), eq(1))
    }

    // -------------------------------------------------------------------------
    // Each call produces a distinct batchToken (no shared state)
    // -------------------------------------------------------------------------

    @Test
    fun `each dryrun call produces a distinct batchToken`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowLifecycle>()
        val configRepo = mock<DispatchConfigRepository>()
        whenever(workflowEngine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w"))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val t1 = resource.dryRun(DryRunRequest(configIds = listOf("cfg1"))).batchToken
        val t2 = resource.dryRun(DryRunRequest(configIds = listOf("cfg1"))).batchToken

        assertTrue(t1 != t2, "Each invocation must generate a unique batchToken")
    }
}
