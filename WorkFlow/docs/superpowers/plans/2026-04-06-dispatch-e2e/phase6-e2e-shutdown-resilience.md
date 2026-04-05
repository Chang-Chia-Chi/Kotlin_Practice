# Phase 6: Dispatch E2E Shutdown Resilience Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `@QuarkusTest` that verifies graceful shutdown preserves DEFERRED tasks and allows recovery. Triggers shutdown while the join task's K8s Job is still running, then recovers.

**Architecture:** Same infrastructure as happy path test. Fires `ShutdownEvent` programmatically to trigger the real Quarkus shutdown sequence. Verifies DEFERRED join task is preserved, then re-starts loops and completes the pipeline.

**Tech Stack:** Quarkus Test, Awaitility, Fabric8 KubernetesMockServer, CDI Event<ShutdownEvent>

---

### Task 1: Create the Shutdown Resilience Test Class

**Files:**
- Create: `src/test/kotlin/dispatch/DispatchE2EShutdownTest.kt`

- [ ] **Step 1: Write the shutdown resilience test**

```kotlin
package com.workflow.dispatch

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.infrastructure.storage.MinioTestContainer
import com.workflow.infrastructure.storage.MinioTestResource
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import com.workflow.workflow.usecase.service.orchestration.WorkflowLifecycle
import com.workflow.worker.usecase.service.execution.WorkerLoop
import com.workflow.worker.usecase.service.trigger.TriggerLoop
import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobConditionBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.test.InjectMock
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.enterprise.event.Event
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

@QuarkusTest
@QuarkusTestResource(MinioTestResource::class)
class DispatchE2EShutdownTest {

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

    @Inject
    lateinit var shutdownEvent: Event<ShutdownEvent>

    @Inject
    lateinit var workerLoop: WorkerLoop

    @Inject
    lateinit var triggerLoop: TriggerLoop

    private val fixture = DispatchE2EFixture
    private val s3Client: S3Client get() = MinioTestContainer.s3Client

    @BeforeEach
    fun setup() {
        runBlocking {
            cleanOracleTables()
            cleanMinioBucket()
            setupMocks()
        }
    }

    @Test
    fun `shutdown preserves DEFERRED join task and recovery completes pipeline`() {
        // Step 1: Create workflow
        val workflowId = runBlocking {
            engine.startWorkflow(dispatchWorkflow).workflowId
        }

        // Step 2-3: Await scatter + simulation tasks completed
        await().atMost(30, TimeUnit.SECONDS).untilAsserted {
            val tasks = findTasksByWorkflowId(workflowId)
            val simulationTasks = tasks.filter { it["HANDLER_KEY"] == "DispatchSimulationHandler" }
            assertTrue(simulationTasks.isNotEmpty())
            assertTrue(simulationTasks.all { it["STATUS"] == "COMPLETED" })
        }

        // Step 4: Await join task DEFERRED (handler did DuckDB work, uploaded Parquet, deferred to K8s)
        await().atMost(15, TimeUnit.SECONDS).untilAsserted {
            val tasks = findTasksByWorkflowId(workflowId)
            val joinTask = tasks.find { it["HANDLER_KEY"] == "DispatchJoinHandler" }
            assertEquals("DEFERRED", joinTask?.get("STATUS"))
        }

        // Step 5: Fire shutdown WITHOUT pushing K8s Job completion
        // The join task's K8s Job is still "Running" — trigger loop has not settled it
        shutdownEvent.fire(ShutdownEvent())

        // Step 6: Await shutdown completes
        await().atMost(15, TimeUnit.SECONDS).untilAsserted {
            // Verify loops have stopped accepting work
            // This depends on the WorkerLoop/TriggerLoop exposing their running state
        }

        // Assertions — Preserved State
        val tasks = findTasksByWorkflowId(workflowId)

        // a. Simulation tasks all COMPLETED, CSVs in MinIO
        val simTasks = tasks.filter { it["HANDLER_KEY"] == "DispatchSimulationHandler" }
        assertTrue(simTasks.all { it["STATUS"] == "COMPLETED" })

        runBlocking {
            val csvResponse = s3Client.listObjectsV2(ListObjectsV2Request {
                bucket = MinioTestContainer.BUCKET
                prefix = "env=prod/"
            })
            val csvKeys = csvResponse.contents?.filter { it.key!!.endsWith(".csv.gz") } ?: emptyList()
            assertEquals(fixture.configIds().size, csvKeys.size)
        }

        // b. Join task still DEFERRED (not lost)
        val joinTask = tasks.first { it["HANDLER_KEY"] == "DispatchJoinHandler" }
        assertEquals("DEFERRED", joinTask["STATUS"])

        // c. Parquet already uploaded (handler uploads before deferring)
        runBlocking {
            val parquetPath = DispatchPathBuilder("prod").prodParquetPath()
            val parquetResponse = s3Client.listObjectsV2(ListObjectsV2Request {
                bucket = MinioTestContainer.BUCKET
                prefix = parquetPath
            })
            assertTrue(parquetResponse.contents?.isNotEmpty() == true, "Parquet should exist")
        }

        // d. No orphaned PROCESSING tasks
        val processingTasks = tasks.filter { it["STATUS"] == "PROCESSING" }
        assertTrue(processingTasks.isEmpty(), "No tasks should be stuck in PROCESSING")

        // e. Workflow NOT completed (join not settled)
        runBlocking {
            val wf = workflowRepo.findById(workflowId)
            assertNotEquals(WorkflowStatus.COMPLETED, wf?.status)
        }

        // ---- Recovery Simulation ----

        // Step 7: Re-start WorkerLoop + TriggerLoop
        // Create new coroutine scopes for the loops
        runBlocking {
            workerLoop.start(kotlinx.coroutines.CoroutineScope(
                kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.IO,
            ))
            triggerLoop.start(kotlinx.coroutines.CoroutineScope(
                kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.IO,
            ))
        }

        // Step 8: Push K8s Job "Complete" on mock server
        val joinTaskForRecovery = findTasksByWorkflowId(workflowId)
            .first { it["HANDLER_KEY"] == "DispatchJoinHandler" }
        val triggerMeta = objectMapper.readTree(joinTaskForRecovery["TRIGGER_META"] as String)
        val jobName = triggerMeta["jobName"].asText()
        val namespace = triggerMeta["namespace"].asText()

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

        k8sClient.configMaps().inNamespace(namespace)
            .resource(
                ConfigMapBuilder()
                    .withNewMetadata().withName("$jobName-output").withNamespace(namespace).endMetadata()
                    .addToData("result", """{"status":"ok"}""")
                    .build(),
            )
            .create()

        // Step 9: Await join task settles → workflow COMPLETED
        await().atMost(30, TimeUnit.SECONDS).untilAsserted {
            runBlocking {
                val wf = workflowRepo.findById(workflowId)
                assertEquals(WorkflowStatus.COMPLETED, wf?.status, "Workflow should complete after recovery")
            }
        }

        // Step 10: Verify final state
        val finalTasks = findTasksByWorkflowId(workflowId)
        assertTrue(
            finalTasks.all { it["STATUS"] == "COMPLETED" },
            "All tasks should be COMPLETED after recovery",
        )
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

- **Shutdown event firing**: `shutdownEvent.fire(ShutdownEvent())` triggers the real
  `ShutdownCoordinator.onShutdown()` observer. This runs `runBlocking` internally, so
  it will block the calling thread until shutdown completes (or times out at 30s).

- **Recovery re-start**: The `workerLoop.start()` and `triggerLoop.start()` methods
  accept a `CoroutineScope` parameter. After shutdown, you need to reset their internal
  state (e.g., `_accepting` flag). Check if the loops have a `reset()` method or if
  you need to use a fresh bean instance. If the loops don't support re-start, the
  recovery simulation may need to create new loop instances manually.

- **K8s mock server state**: The mock server must survive across shutdown/restart.
  Since it's per-test-class lifecycle and not tied to Quarkus CDI, it should persist.

- **`cleanOracleTables()`**: Must be implemented by injecting `Jdbi` and executing
  DELETE statements. Follow the pattern in `WorkflowIntegrationTest.cleanTables()`.

- [ ] **Step 2: Verify it compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: Fix compilation errors.

- [ ] **Step 3: Run the test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DispatchE2EShutdownTest"`
Expected: Test PASSES.

- [ ] **Step 4: Run the full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/dispatch/DispatchE2EShutdownTest.kt
git commit -m "test: add dispatch E2E shutdown resilience integration test"
```
