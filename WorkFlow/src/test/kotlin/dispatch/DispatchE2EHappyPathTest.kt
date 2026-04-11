package com.workflow.dispatch

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.DeleteObjectRequest
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.smithy.kotlin.runtime.content.toByteArray
import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.infrastructure.persistence.OracleTestResource
import com.workflow.infrastructure.storage.MinioTestContainer
import com.workflow.infrastructure.storage.MinioTestResource
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.usecase.port.inbound.orchestration.WorkflowLifecycle
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import io.quarkus.test.InjectMock
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import java.sql.DriverManager
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@QuarkusTest
@TestProfile(E2ETestProfile::class)
@QuarkusTestResource(OracleTestResource::class)
@QuarkusTestResource(MinioTestResource::class)
class DispatchE2EHappyPathTest {

    @Inject
    lateinit var engine: WorkflowLifecycle

    @Inject
    lateinit var workflowRepo: WorkflowRepository

    @Inject
    lateinit var objectMapper: ObjectMapper

    @Inject
    lateinit var jdbi: Jdbi

    @InjectMock
    lateinit var configRepo: DispatchConfigRepository

    @InjectMock
    lateinit var candidateRepo: CandidateRepository

    @InjectMock
    lateinit var baselineProvider: BaselineProvider

    private val fixture = DispatchE2EFixture
    private val s3Client: S3Client get() = MinioTestContainer.s3Client
    private val pathBuilder = DispatchPathBuilder("prod")

    @BeforeEach
    fun setup() {
        runBlocking {
            cleanOracleTables()
            cleanMinioBucket()
            setupMocks()
        }
    }

    @Test
    fun `full dispatch pipeline completes with CSV and Parquet artifacts`() {
        // Step 1: Create workflow
        val workflowId = runBlocking {
            engine.startWorkflow(dispatchWorkflow).workflowId
        }

        // Step 2-3: Await scatter + simulation tasks completed
        await().atMost(30, TimeUnit.SECONDS).untilAsserted {
            val tasks = findTasksByWorkflowId(workflowId)
            val simulationTasks = tasks.filter { it["HANDLER_KEY"] == "DispatchSimulationHandler" }
            assertTrue(simulationTasks.isNotEmpty(), "Simulation tasks should exist")
            assertTrue(
                simulationTasks.all { it["STATUS"] == "COMPLETED" },
                "All simulation tasks should be COMPLETED",
            )
        }

        // Step 4: Await join task COMPLETED
        await().atMost(15, TimeUnit.SECONDS).untilAsserted {
            val tasks = findTasksByWorkflowId(workflowId)
            val joinTask = tasks.find { it["HANDLER_KEY"] == "DispatchJoinHandler" }
            assertEquals("COMPLETED", joinTask?.get("STATUS"), "Join task should be COMPLETED")
        }

        // Step 5: Await workflow COMPLETED
        await().atMost(30, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val wf = workflowRepo.findById(workflowId)
                assertEquals(WorkflowStatus.COMPLETED, wf?.status, "Workflow should be COMPLETED")
            }
        }

        // Assertions
        runBlocking {
            assertOracleState(workflowId)
            assertMinIOCsvArtifacts()
            assertMinIOParquetArtifact()
        }
    }

    private suspend fun setupMocks() {
        val configs = fixture.configs()

        whenever(configRepo.findActiveConfigs(any<LocalDateTime>(), any())).thenReturn(configs)
        for (config in configs) {
            whenever(configRepo.findById(config.id)).thenReturn(config)
            whenever(candidateRepo.queryCandidates(config)).thenReturn(fixture.candidates(config.id))
            whenever(baselineProvider.loadBaseline(config)).thenReturn(fixture.baseline(config.id))
        }
    }

    private suspend fun assertOracleState(workflowId: String) {
        val tasks = findTasksByWorkflowId(workflowId)
        assertTrue(
            tasks.all { it["STATUS"] == "COMPLETED" },
            "All tasks should be COMPLETED, got: ${tasks.map { it["HANDLER_KEY"] to it["STATUS"] }}",
        )

        val wf = workflowRepo.findById(workflowId)
        assertEquals(WorkflowStatus.COMPLETED, wf?.status)
    }

    private suspend fun assertMinIOCsvArtifacts() {
        val configIds = fixture.configIds()
        val response = s3Client.listObjectsV2(ListObjectsV2Request {
            bucket = MinioTestContainer.BUCKET
            prefix = "env=prod/"
        })
        val csvKeys = response.contents
            ?.map { it.key!! }
            ?.filter { it.endsWith(".csv.gz") }
            ?: emptyList()

        assertEquals(configIds.size, csvKeys.size, "Should have one CSV per config")

        for (key in csvKeys) {
            val obj = s3Client.getObject(
                GetObjectRequest {
                    bucket = MinioTestContainer.BUCKET
                    this.key = key
                },
            ) { resp ->
                resp.body?.toByteArray() ?: ByteArray(0)
            }
            val decompressed = GZIPInputStream(obj.inputStream()).bufferedReader().readText()
            assertTrue(decompressed.contains("dispatch_order"), "CSV should have header row")
            val lines = decompressed.trim().lines()
            assertTrue(lines.size > 1, "CSV should have data rows beyond header")
        }
    }

    private suspend fun assertMinIOParquetArtifact() {
        val parquetPath = pathBuilder.prodParquetPath()
        val parquetBytes = s3Client.getObject(
            GetObjectRequest {
                bucket = MinioTestContainer.BUCKET
                key = parquetPath
            },
        ) { resp ->
            resp.body?.toByteArray() ?: ByteArray(0)
        }
        assertTrue(parquetBytes.isNotEmpty(), "Parquet file should not be empty")

        // Read back via DuckDB to verify content
        val tmpFile = kotlin.io.path.createTempFile(prefix = "e2e-parquet-", suffix = ".parquet")
        try {
            tmpFile.toFile().writeBytes(parquetBytes)
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    val rs = stmt.executeQuery(
                        "SELECT COUNT(*) as cnt FROM read_parquet('${tmpFile.toString().replace("\\", "/")}')",
                    )
                    rs.next()
                    val count = rs.getInt("cnt")
                    assertTrue(
                        count >= fixture.configIds().size,
                        "Parquet should have >= 1 row per config (got $count)",
                    )
                }
            }
        } finally {
            tmpFile.toFile().delete()
        }
    }

    /**
     * Queries tasks for a workflow using JDBI directly, returning raw maps.
     * Same pattern as WorkflowIntegrationTest.readTasksDirect().
     */
    private fun findTasksByWorkflowId(workflowId: String): List<Map<String, Any?>> =
        jdbi.withHandle<List<Map<String, Any?>>, Exception> { handle ->
            handle.createQuery("SELECT * FROM task WHERE workflow_id = :wfId")
                .bind("wfId", workflowId)
                .mapToMap()
                .list()
                .map { raw ->
                    val ci = java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
                    raw.forEach { (k, v) ->
                        ci[k] = if (v is java.sql.Clob) v.characterStream.readText() else v
                    }
                    ci
                }
        }

    private fun cleanOracleTables() {
        jdbi.useHandle<Exception> { handle ->
            // Order matters because of FK constraints (event -> batch).
            handle.execute("DELETE FROM dispatch_event")
            handle.execute("DELETE FROM dispatch_event_stg")
            handle.execute("DELETE FROM dispatch_batch")
            handle.execute("DELETE FROM dispatch_batch_stg")
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    private suspend fun cleanMinioBucket() {
        var continuation: String? = null
        do {
            val token = continuation
            val response = s3Client.listObjectsV2(
                ListObjectsV2Request {
                    bucket = MinioTestContainer.BUCKET
                    continuationToken = token
                },
            )
            response.contents?.forEach { obj ->
                s3Client.deleteObject(
                    DeleteObjectRequest {
                        bucket = MinioTestContainer.BUCKET
                        key = obj.key
                    },
                )
            }
            continuation = if (response.isTruncated == true) response.nextContinuationToken else null
        } while (continuation != null)
    }
}
