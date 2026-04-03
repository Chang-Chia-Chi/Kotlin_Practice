# Dispatch Env P7: JoinHandler Conditional Parquet Export

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modify `DispatchJoinHandler` to only export Parquet when running in prod with a NORMAL batch. Use `DispatchPathBuilder` for the export path.

**Architecture:** Handler gets `dispatch.env` config and `DispatchPathBuilder` injected. After aggregating decisions, it checks: if `env == "prod"` AND `batchStatus == NORMAL`, export Parquet to the fixed prod path. Otherwise, skip Parquet export entirely.

**Tech Stack:** Kotlin

**Current behavior:** Always exports Parquet to `dispatch/$batchToken/result.parquet`.
**New behavior:** Only exports when prod + NORMAL. Path becomes `env=prod/dispatch/result.parquet`.

---

### Task 1: Write tests for conditional Parquet export

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

- [ ] **Step 1: Add test for prod NORMAL — exports Parquet**

In `DispatchHandlersTest`, add:

```kotlin
@Test
fun `join handler exports parquet for prod normal batch`() = runTest {
    val resultStore = mock<SimulationResultStore>()
    val storage = mock<StorageGateway>()
    val parquetFormatter = mock<ParquetFormatter>()
    val pathBuilder = DispatchPathBuilder("prod")

    whenever(resultStore.findByBatchToken("20260329060000")).thenReturn(emptyList())
    whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)
    whenever(parquetFormatter.format(any())).thenReturn(byteArrayOf())

    val handler = DispatchJoinHandler(resultStore, storage, parquetFormatter, pathBuilder, "prod", objectMapper)

    val inputs = objectMapper.writeValueAsString(
        mapOf("batchToken" to listOf("20260329060000", "20260329060000")),
    )
    handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

    verify(storage).uploadParquet(eq("env=prod/dispatch/result.parquet"), any())
}
```

- [ ] **Step 2: Add test for prod DRYRUN — skips Parquet**

```kotlin
@Test
fun `join handler skips parquet for prod dryrun batch`() = runTest {
    val resultStore = mock<SimulationResultStore>()
    val storage = mock<StorageGateway>()
    val parquetFormatter = mock<ParquetFormatter>()
    val pathBuilder = DispatchPathBuilder("prod")

    whenever(resultStore.findByBatchToken("dryrun-abc")).thenReturn(emptyList())
    whenever(resultStore.findBatchStatus("dryrun-abc")).thenReturn(BatchStatus.DRYRUN)

    val handler = DispatchJoinHandler(resultStore, storage, parquetFormatter, pathBuilder, "prod", objectMapper)

    val inputs = objectMapper.writeValueAsString(
        mapOf("batchToken" to listOf("dryrun-abc", "dryrun-abc")),
    )
    handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

    verify(storage, never()).uploadParquet(any(), any())
    verify(parquetFormatter, never()).format(any())
}
```

- [ ] **Step 3: Add test for stg — skips Parquet**

```kotlin
@Test
fun `join handler skips parquet for stg env`() = runTest {
    val resultStore = mock<SimulationResultStore>()
    val storage = mock<StorageGateway>()
    val parquetFormatter = mock<ParquetFormatter>()
    val pathBuilder = DispatchPathBuilder("stg")

    whenever(resultStore.findByBatchToken("20260329060000")).thenReturn(emptyList())
    whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)

    val handler = DispatchJoinHandler(resultStore, storage, parquetFormatter, pathBuilder, "stg", objectMapper)

    val inputs = objectMapper.writeValueAsString(
        mapOf("batchToken" to listOf("20260329060000", "20260329060000")),
    )
    handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

    verify(storage, never()).uploadParquet(any(), any())
}
```

Add imports: `com.workflow.dispatch.adapter.storage.DispatchPathBuilder`, `com.workflow.dispatch.model.BatchStatus`, `org.mockito.kotlin.never`.

- [ ] **Step 4: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: New tests FAIL — constructor signature doesn't match.

- [ ] **Step 5: Commit failing tests**

```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "test(dispatch): add join handler tests for conditional parquet export"
```

---

### Task 2: Update JoinHandler for conditional Parquet

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchJoinHandler.kt`

- [ ] **Step 1: Update the handler**

Replace `DispatchJoinHandler`:

```kotlin
package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class DispatchJoinHandler(
    private val resultStore: SimulationResultStore,
    private val storage: StorageGateway,
    private val parquetFormatter: ParquetFormatter,
    private val pathBuilder: DispatchPathBuilder,
    @ConfigProperty(name = "dispatch.env", defaultValue = "prod") private val env: String,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val inputsNode = objectMapper.readTree(input.inputs!!)
        val batchTokenNode = inputsNode["batchToken"]
        val batchToken = if (batchTokenNode.isArray) {
            batchTokenNode[0].asText()
        } else {
            batchTokenNode.asText()
        }

        val batchStatus = resultStore.findBatchStatus(batchToken)

        if (env == "prod" && batchStatus == BatchStatus.NORMAL) {
            val allDecisions = resultStore.findByBatchToken(batchToken)
            val parquet = parquetFormatter.format(allDecisions)
            storage.uploadParquet(pathBuilder.prodParquetPath(), parquet)
        }

        return HandlerOutput(null)
    }
}
```

- [ ] **Step 2: Update the existing join handler test**

Update the existing `join handler uploads parquet with merged results` test to use the new constructor signature:

```kotlin
@Test
fun `join handler uploads parquet with merged results`() = runTest {
    val resultStore = mock<SimulationResultStore>()
    val storage = mock<StorageGateway>()
    val parquetFormatter = mock<ParquetFormatter>()
    val pathBuilder = DispatchPathBuilder("prod")

    whenever(resultStore.findByBatchToken("20260329060000")).thenReturn(emptyList())
    whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)
    whenever(parquetFormatter.format(any())).thenReturn(byteArrayOf())

    val handler = DispatchJoinHandler(resultStore, storage, parquetFormatter, pathBuilder, "prod", objectMapper)

    val inputs = objectMapper.writeValueAsString(
        mapOf("batchToken" to listOf("20260329060000", "20260329060000")),
    )
    handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

    verify(storage).uploadParquet(eq("env=prod/dispatch/result.parquet"), any())
}
```

- [ ] **Step 3: Run all handler tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/service/handler/DispatchJoinHandler.kt src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "feat(dispatch): join handler conditionally exports parquet based on env and batch status"
```
