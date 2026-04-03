# Dispatch Env P10: Export Endpoint (Stg Only)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /dispatch/export` REST endpoint gated to stg profile. It reads existing stg batch results, formats them as Parquet, and uploads to the stg MinIO path.

**Architecture:** A JAX-RS resource annotated with `@IfBuildProfile("stg")`. It reads events from `dispatch_event_stg` via `SimulationResultStore`, optionally filters by config IDs, formats via `ParquetFormatter`, and uploads via `StorageGateway` using `DispatchPathBuilder.batchParquetPath()`.

**Tech Stack:** Kotlin, Quarkus JAX-RS

**Depends on:** P3 (JdbiSimulationResultStore), P4 (DispatchPathBuilder)

---

### Task 1: Extend SimulationResultStore with config-filtered query

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/port/outbound/persistence/SimulationResultStore.kt`
- Modify: `src/main/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStore.kt`
- Modify: `src/test/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStoreTest.kt`

- [ ] **Step 1: Add port method**

Add to `SimulationResultStore`:

```kotlin
suspend fun findByBatchTokenAndConfigs(batchToken: String, configIds: List<String>): List<DispatchDecision>
```

- [ ] **Step 2: Write test for the new method**

In `JdbiSimulationResultStoreTest`, add:

```kotlin
@Test
fun `findByBatchTokenAndConfigs filters by config IDs`() = runTest {
    store.createBatch("batch1", BatchStatus.NORMAL, 2)

    store.saveDecisions("batch1", "cfg1", listOf(
        DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
    ))
    store.saveDecisions("batch1", "cfg2", listOf(
        DispatchDecision(1, "P2", "BOM-B", 8, "SITE-Y", null, BigDecimal("3.0"), null),
    ))

    val found = store.findByBatchTokenAndConfigs("batch1", listOf("cfg1"))
    assertEquals(1, found.size)
    assertEquals("P1", found[0].productId)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="JdbiSimulationResultStoreTest" -pl WorkFlow`
Expected: FAIL — method not implemented.

- [ ] **Step 4: Implement in JdbiSimulationResultStore**

Add to `JdbiSimulationResultStore`:

```kotlin
override suspend fun findByBatchTokenAndConfigs(
    batchToken: String,
    configIds: List<String>,
): List<DispatchDecision> {
    return jdbi.withHandleSuspend<List<DispatchDecision>, Exception> { h ->
        h.createQuery(
            """SELECT dispatch_order, product_id, source_bom_id, qty,
                      target_site_id, target_bom_id, site_gap, bom_gap
               FROM $eventTable
               WHERE batch_token = :token AND config_id IN (<configIds>)
               ORDER BY config_id, dispatch_order"""
        )
            .bind("token", batchToken)
            .bindList("configIds", configIds)
            .mapToMap()
            .list()
            .map { row ->
                DispatchDecision(
                    dispatchOrder = (row["dispatch_order"] as Number).toInt(),
                    productId = row["product_id"] as String,
                    sourceBomId = row["source_bom_id"] as String,
                    qty = (row["qty"] as Number).toInt(),
                    targetSiteId = row["target_site_id"] as String,
                    targetBomId = row["target_bom_id"] as String?,
                    siteGap = row["site_gap"] as BigDecimal,
                    bomGap = row["bom_gap"] as BigDecimal?,
                )
            }
    }
}
```

- [ ] **Step 5: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="JdbiSimulationResultStoreTest" -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/port/outbound/persistence/SimulationResultStore.kt src/main/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStore.kt src/test/kotlin/dispatch/adapter/persistence/JdbiSimulationResultStoreTest.kt
git commit -m "feat(dispatch): add findByBatchTokenAndConfigs to SimulationResultStore"
```

---

### Task 2: Implement export REST endpoint

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/http/DispatchExportResource.kt`
- Create: `src/test/kotlin/dispatch/adapter/http/DispatchExportResourceTest.kt`

- [ ] **Step 1: Write the test**

Create `src/test/kotlin/dispatch/adapter/http/DispatchExportResourceTest.kt`:

```kotlin
package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.math.BigDecimal
import kotlin.test.assertEquals

class DispatchExportResourceTest {

    @Test
    fun `export uploads parquet for whole batch when no configIds`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val parquetFormatter = mock<ParquetFormatter>()
        val storage = mock<StorageGateway>()
        val pathBuilder = DispatchPathBuilder("stg")

        val decisions = listOf(
            DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
        )
        whenever(resultStore.findByBatchToken("batch1")).thenReturn(decisions)
        whenever(parquetFormatter.format(decisions)).thenReturn(byteArrayOf(1, 2, 3))

        val resource = DispatchExportResource(resultStore, parquetFormatter, storage, pathBuilder)
        val response = resource.export(ExportRequest(batchToken = "batch1", configIds = null))

        verify(resultStore).findByBatchToken("batch1")
        verify(storage).uploadParquet(eq("env=stg/dispatch/batch1/result.parquet"), any())
        assertEquals("batch1", response.batchToken)
        assertEquals("env=stg/dispatch/batch1/result.parquet", response.path)
    }

    @Test
    fun `export uploads parquet for specified configs only`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val parquetFormatter = mock<ParquetFormatter>()
        val storage = mock<StorageGateway>()
        val pathBuilder = DispatchPathBuilder("stg")

        val decisions = listOf(
            DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
        )
        whenever(resultStore.findByBatchTokenAndConfigs("batch1", listOf("cfg1"))).thenReturn(decisions)
        whenever(parquetFormatter.format(decisions)).thenReturn(byteArrayOf(1, 2, 3))

        val resource = DispatchExportResource(resultStore, parquetFormatter, storage, pathBuilder)
        val response = resource.export(ExportRequest(batchToken = "batch1", configIds = listOf("cfg1")))

        verify(resultStore).findByBatchTokenAndConfigs("batch1", listOf("cfg1"))
        verify(resultStore, never()).findByBatchToken(any())
        verify(storage).uploadParquet(eq("env=stg/dispatch/batch1/result.parquet"), any())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchExportResourceTest" -pl WorkFlow`
Expected: FAIL — resource doesn't exist.

- [ ] **Step 3: Create the resource**

Create `src/main/kotlin/dispatch/adapter/http/DispatchExportResource.kt`:

```kotlin
package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import io.quarkus.arc.profile.IfBuildProfile
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import kotlinx.coroutines.runBlocking

data class ExportRequest(val batchToken: String, val configIds: List<String>? = null)
data class ExportResponse(val batchToken: String, val exportedConfigs: List<String>, val path: String)

@Path("/dispatch")
@ApplicationScoped
@IfBuildProfile("stg")
class DispatchExportResource(
    private val resultStore: SimulationResultStore,
    private val parquetFormatter: ParquetFormatter,
    private val storage: StorageGateway,
    private val pathBuilder: DispatchPathBuilder,
) {

    @POST
    @Path("/export")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun export(request: ExportRequest): ExportResponse = runBlocking {
        val decisions = if (request.configIds != null) {
            resultStore.findByBatchTokenAndConfigs(request.batchToken, request.configIds)
        } else {
            resultStore.findByBatchToken(request.batchToken)
        }

        val parquet = parquetFormatter.format(decisions)
        val path = pathBuilder.batchParquetPath(request.batchToken)
        storage.uploadParquet(path, parquet)

        ExportResponse(
            batchToken = request.batchToken,
            exportedConfigs = request.configIds ?: listOf("all"),
            path = path,
        )
    }
}
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchExportResourceTest" -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/http/DispatchExportResource.kt src/test/kotlin/dispatch/adapter/http/DispatchExportResourceTest.kt
git commit -m "feat(dispatch): add POST /dispatch/export endpoint for stg profile"
```
