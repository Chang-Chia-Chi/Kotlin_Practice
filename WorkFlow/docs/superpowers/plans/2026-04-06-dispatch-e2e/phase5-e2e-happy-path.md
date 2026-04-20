# Phase 5: Dispatch E2E Happy Path Integration Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Full end-to-end `@QuarkusTest` that exercises: workflow creation → scatter → parallel simulation (CSV upload) → join (DuckDB Parquet upload + K8s defer) → trigger loop settles → workflow COMPLETED.

**Architecture:** `@QuarkusTest` with real CDI wiring. Uses `OracleTestContainer` for persistence, `MinioTestContainer` for S3 storage, Fabric8 `KubernetesMockServer` for K8s trigger path. Dispatch repositories (`DispatchConfigRepository`, `CandidateRepository`, `BaselineProvider`) are mocked via `@InjectMock` to return fixture data — they are backed by external systems in production, not the Oracle test container.

**Tech Stack:** Quarkus Test, Awaitility, Fabric8 KubernetesMockServer, MinIO Testcontainer, DuckDB

---

### Task 1: Add test application.properties entries

**Files:**
- Modify: `src/test/resources/application.properties`

- [ ] **Step 1: Add storage and dispatch config for tests**

Append the following to `src/test/resources/application.properties`:

```properties
# Storage (MinIO via Testcontainer — endpoint overridden by QuarkusTestResource)
storage.endpoint=http://localhost:9000
storage.region=us-east-1
storage.bucket=dispatch-test
storage.access-key=minioadmin
storage.secret-key=minioadmin

# Dispatch
dispatch.env=prod
dispatch.k8s.namespace=test-ns

# Faster polling for E2E tests
framework.worker.poll-interval=PT0.2S
framework.worker.fallback-poll-interval=PT0.5S
framework.worker.concurrency=4
framework.worker.max-batch-size=16
```

- [ ] **Step 2: Commit**

```bash
git add src/test/resources/application.properties
git commit -m "test: add storage and dispatch config for E2E tests"
```

---

### Task 2: Create QuarkusTestResource for MinIO endpoint

**Files:**
- Create: `src/test/kotlin/infrastructure/storage/MinioTestResource.kt`

This `QuarkusTestResourceLifecycleManager` starts the MinIO container and injects the
dynamic endpoint into Quarkus config.

- [ ] **Step 1: Create the test resource**

```kotlin
package com.workflow.infrastructure.storage

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager

class MinioTestResource : QuarkusTestResourceLifecycleManager {

    override fun start(): Map<String, String> {
        // Accessing the lazy s3Client triggers container start + bucket creation
        MinioTestContainer.s3Client
        return mapOf(
            "storage.endpoint" to MinioTestContainer.endpoint,
        )
    }

    override fun stop() {
        // Container lifecycle managed by singleton — no explicit stop
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add src/test/kotlin/infrastructure/storage/MinioTestResource.kt
git commit -m "test: add MinioTestResource for QuarkusTest endpoint injection"
```

---

### Task 3: Create the Happy Path Test Class

**Files:**
- Create: `src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt`

- [ ] **Step 1: Write the full happy path test**

```kotlin
package com.workflow.dispatch

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.smithy.kotlin.runtime.content.toByteArray
import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.infrastructure.storage.MinioTestContainer
import com.workflow.infrastructure.storage.MinioTestResource
import com.workflow.workflow.model.StartResult
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import com.workflow.workflow.usecase.service.orchestration.WorkflowLifecycle
import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobConditionBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.quarkus.test.InjectMock
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
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
@QuarkusTestResource(MinioTestResource::class)
class DispatchE2EHappyPathTest {

    @Inject
    lateinit var engine: WorkflowLifecycle

    @Inject
    lateinit var workflowRepo: WorkflowRepository

    @Inject
    lateinit var objectMapper: ObjectMapper

    @Inject
    lateinit var jdbi: org.jdbi.v3.core.Jdbi

    @InjectMock
    lateinit var configRepo: DispatchConfigRepository

    @InjectMock
    lateinit var candidateRepo: CandidateRepository

    @InjectMock
    lateinit var baselineProvider: BaselineProvider

    @Inject
    lateinit var k8sClient: KubernetesClient

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

        // Step 4: Await join task DEFERRED
        await().atMost(15, TimeUnit.SECONDS).untilAsserted {
            val tasks = findTasksByWorkflowId(workflowId)
            val joinTask = tasks.find { it["HANDLER_KEY"] == "DispatchJoinHandler" }
            assertEquals("DEFERRED", joinTask?.get("STATUS"), "Join task should be DEFERRED")
        }

        // Step 5: Push K8s Job completion on mock server
        val joinTask = findTasksByWorkflowId(workflowId)
            .first { it["HANDLER_KEY"] == "DispatchJoinHandler" }
        val triggerMeta = objectMapper.readTree(joinTask["TRIGGER_META"] as String)
        val jobName = triggerMeta["jobName"].asText()
        val namespace = triggerMeta["namespace"].asText()

        // Create the Job resource on mock server with Complete condition
        k8sClient.batch().v1().jobs().inNamespace(namespace)
            .resource(
                JobBuilder()
                    .withNewMetadata().withName(jobName).withNamespace(namespace).endMetadata()
                    .withStatus(
                        JobStatusBuilder()
                            .withConditions(
                                JobConditionBuilder()
                                    .withType("Complete")
                                    .withStatus("True")
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .create()

        // Create ConfigMap with result
        k8sClient.configMaps().inNamespace(namespace)
            .resource(
                ConfigMapBuilder()
                    .withNewMetadata().withName("$jobName-output").withNamespace(namespace).endMetadata()
                    .addToData("result", """{"status":"ok"}""")
                    .build(),
            )
            .create()

        // Step 6: Await workflow COMPLETED
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

        whenever(configRepo.findActiveConfigs(any<LocalDateTime>())).thenReturn(configs)
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
            val obj = s3Client.getObject(GetObjectRequest {
                bucket = MinioTestContainer.BUCKET
                this.key = key
            }) { resp ->
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
        val parquetBytes = s3Client.getObject(GetObjectRequest {
            bucket = MinioTestContainer.BUCKET
            key = parquetPath
        }) { resp ->
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
                    assertTrue(count > 0, "Parquet should contain dispatch decisions (got $count rows)")
                }
            }
        } finally {
            tmpFile.toFile().delete()
        }
    }

    /**
     * Queries tasks for a workflow using JDBI directly, returning raw maps.
     * Same pattern as WorkflowIntegrationTest.readTasksDirect().
     * TaskRepository doesn't expose a suspend findByWorkflowId, so we query directly.
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
            handle.execute("DELETE FROM dispatch_event")
            handle.execute("DELETE FROM dispatch_batch")
            handle.execute("DELETE FROM dispatch_event_stg")
            handle.execute("DELETE FROM dispatch_batch_stg")
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    private suspend fun cleanMinioBucket() {
        val response = s3Client.listObjectsV2(ListObjectsV2Request {
            bucket = MinioTestContainer.BUCKET
        })
        response.contents?.forEach { obj ->
            s3Client.deleteObject(aws.sdk.kotlin.services.s3.model.DeleteObjectRequest {
                bucket = MinioTestContainer.BUCKET
                key = obj.key
            })
        }
    }
}
```

**Important implementation notes:**
- The `cleanOracleTables()` method needs to be adapted to the actual JDBI access pattern.
  Read `WorkflowIntegrationTest.cleanTables()` (line 80+) for the existing pattern.
  The E2E test can inject `Jdbi` or `DataSource` directly.
- The `taskRepo.findByWorkflowId()` method needs to exist. Check the actual `TaskRepository`
  interface — if it doesn't have this method, use an equivalent query or add it.
- The K8s mock server interaction depends on whether the test uses a real
  `KubernetesMockServer` or the CDI-injected `KubernetesClient`. For `@QuarkusTest`,
  the client is injected by Quarkus's kubernetes-client extension. You may need to use
  `@InjectMock KubernetesClient` or configure the mock server URL via test properties.

- [ ] **Step 2: Run the test to see if it compiles and identify any missing APIs**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: Fix any compilation errors (missing imports, API mismatches).

- [ ] **Step 3: Run the test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DispatchE2EHappyPathTest"`
Expected: Test PASSES. If not, debug and fix issues (timing, API mismatches, mock wiring).

- [ ] **Step 4: Run the full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt
git commit -m "test: add dispatch E2E happy path integration test"
```
