# Dispatch Env P8: Dry-Run Endpoint (Prod Only)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /dispatch/dryrun` REST endpoint gated to prod profile. It creates a DRYRUN batch and starts the dispatch workflow with a random batch token and optional config IDs.

**Architecture:** A JAX-RS resource class annotated with `@IfBuildProfile("prod")`. It generates a UUID batch token, writes a `dispatch_batch` record with `DRYRUN` status via `SimulationResultStore.createBatch()`, and starts the existing `dispatchWorkflow` via `WorkflowEngine`, passing the batch token + optional config IDs as the initial item for the scatter handler.

**Tech Stack:** Kotlin, Quarkus JAX-RS, Quarkus CDI profiles

**Key dependency:** `DispatchScheduler.trigger()` currently creates the workflow with no initial item. The dry-run endpoint passes an item containing `batchToken` and `configIds`. The scatter handler (modified in P5) reads these from `input.item`.

---

### Task 1: Write test for dry-run endpoint

**Files:**
- Create: `src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt`

- [ ] **Step 1: Write the test class**

Create `src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt`:

```kotlin
package com.workflow.dispatch.adapter.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.model.DispatchConfig
import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.model.SiteTarget
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DispatchDryRunResourceTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    @Test
    fun `dryrun creates DRYRUN batch and starts workflow`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowEngine>()
        val configRepo = mock<DispatchConfigRepository>()

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val response = resource.dryRun(DryRunRequest(configIds = listOf("cfg1", "cfg2")))

        verify(resultStore).createBatch(
            argThat { length == 36 },  // UUID format
            eq(BatchStatus.DRYRUN),
            eq(2),
        )
        verify(workflowEngine).startWorkflow(
            definition = any(),
            idempotencyKey = argThat { startsWith("dispatch-dryrun-") },
            initialItem = argThat { contains("cfg1") && contains("cfg2") },
        )
        assertEquals("DRYRUN", response.status)
        assertNotNull(response.batchToken)
    }

    @Test
    fun `dryrun with null configIds queries all active configs`() = runTest {
        val resultStore = mock<SimulationResultStore>()
        val workflowEngine = mock<WorkflowEngine>()
        val configRepo = mock<DispatchConfigRepository>()

        val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
            listOf(SiteTarget("A", BigDecimal("100"))), null)
        whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

        val resource = DispatchDryRunResource(resultStore, workflowEngine, configRepo, objectMapper)

        val response = resource.dryRun(DryRunRequest(configIds = null))

        verify(configRepo).findActiveConfigs(any())
        verify(resultStore).createBatch(any(), eq(BatchStatus.DRYRUN), eq(1))
        assertTrue(response.batchToken.isNotEmpty())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchDryRunResourceTest" -pl WorkFlow`
Expected: FAIL — classes don't exist.

- [ ] **Step 3: Commit failing tests**

```bash
git add src/test/kotlin/dispatch/adapter/http/DispatchDryRunResourceTest.kt
git commit -m "test(dispatch): add dry-run endpoint tests"
```

---

### Task 2: Add initialItem to WorkflowLifecycle and WorkflowEngine

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/port/inbound/orchestration/WorkflowLifecycle.kt`
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowEngine.kt`

- [ ] **Step 1: Update WorkflowLifecycle interface**

Add `initialItem: String? = null` as a trailing parameter to `startWorkflow` in `WorkflowLifecycle.kt`:

```kotlin
interface WorkflowLifecycle {
    suspend fun startWorkflow(
        definition: WorkflowDefinition,
        idempotencyKey: String? = null,
        initialItem: String? = null,
    ): StartResult
    suspend fun cancelWorkflow(workflowId: String): Boolean
    suspend fun replayWorkflow(workflowId: String): Boolean
}
```

- [ ] **Step 2: Update WorkflowEngine.startWorkflow**

Add `initialItem: String? = null` to the `startWorkflow` override signature, and thread it through both `createTaskForActivity` call sites:

```kotlin
override suspend fun startWorkflow(
    definition: WorkflowDefinition,
    idempotencyKey: String?,
    initialItem: String? = null,
): StartResult {
    // ...
    // Non-idempotent path (no idempotencyKey):
    val task = createTaskForActivity(workflowId, startSeqInfo.activityName, startSeqInfo.sequenceNumber, startSeqInfo.activity, now, initialItem)

    // Idempotent path (with idempotencyKey, inside the transaction lambda):
    val task = createTaskForActivity(mId, startSeqInfo.activityName, startSeqInfo.sequenceNumber, startSeqInfo.activity, now, initialItem)
}
```

`DispatchScheduler` is the only existing caller and passes no `initialItem` — it continues to work with the default `null`.

- [ ] **Step 3: Run existing tests to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All existing tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/workflow/usecase/port/inbound/orchestration/WorkflowLifecycle.kt
git add src/main/kotlin/workflow/usecase/service/orchestration/WorkflowEngine.kt
git commit -m "feat(workflow): add optional initialItem parameter to startWorkflow"
```

---

### Task 3: Implement dry-run resource

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/http/DispatchDryRunResource.kt`

- [ ] **Step 1: Create request/response DTOs and resource class**

Create `src/main/kotlin/dispatch/adapter/http/DispatchDryRunResource.kt`:

```kotlin
package com.workflow.dispatch.adapter.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.quarkus.arc.profile.IfBuildProfile
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import java.time.LocalDateTime
import java.util.UUID

data class DryRunRequest(val configIds: List<String>? = null)
data class DryRunResponse(val batchToken: String, val status: String)

@Path("/dispatch")
@ApplicationScoped
@IfBuildProfile("prod")
class DispatchDryRunResource(
    private val resultStore: SimulationResultStore,
    private val workflowEngine: WorkflowEngine,
    private val configRepo: DispatchConfigRepository,
    private val objectMapper: ObjectMapper,
) {

    @POST
    @Path("/dryrun")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    suspend fun dryRun(request: DryRunRequest): DryRunResponse {
        val batchToken = UUID.randomUUID().toString()

        val configIds = request.configIds
            ?: configRepo.findActiveConfigs(LocalDateTime.now()).map { it.id }

        resultStore.createBatch(batchToken, BatchStatus.DRYRUN, configIds.size)

        val initialItem = objectMapper.writeValueAsString(
            mapOf("batchToken" to batchToken, "configIds" to configIds),
        )

        workflowEngine.startWorkflow(
            definition = dispatchWorkflow,
            idempotencyKey = "dispatch-dryrun-$batchToken",
            initialItem = initialItem,
        )

        return DryRunResponse(batchToken = batchToken, status = "DRYRUN")
    }
}
```

- [ ] **Step 2: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchDryRunResourceTest" -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/http/DispatchDryRunResource.kt
git commit -m "feat(dispatch): add POST /dispatch/dryrun endpoint for prod profile"
```
