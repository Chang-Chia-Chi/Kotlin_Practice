# Dispatch Env P5: ScatterHandler Optional ConfigIds + Batch Creation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modify `DispatchScatterHandler` to accept an optional `configIds` list and `batchToken` from its input. When provided (dry-run), use those and skip batch creation (already created by endpoint). When absent (normal cron), generate a batch token, create the `dispatch_batch` record with NORMAL status, and query all active configs.

**Architecture:** The handler checks its `input.item` JSON for `configIds` (optional array) and `batchToken` (optional string). If `batchToken` is present, the batch record was already created by the dry-run endpoint. If absent, the handler generates a time-based token and calls `resultStore.createBatch()` with NORMAL status. The handler now depends on `SimulationResultStore` for batch creation.

**Tech Stack:** Kotlin, Jackson

**Key file to modify:** `src/main/kotlin/dispatch/usecase/service/handler/DispatchScatterHandler.kt`

**Current handler code (for reference):**
```kotlin
override suspend fun execute(input: HandlerInput): HandlerOutput {
    val now = LocalDateTime.now()
    val batchToken = now.truncatedTo(ChronoUnit.HOURS)
        .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    val configs = configRepo.findActiveConfigs(now)
    val items = configs.map { mapOf("configId" to it.id, "batchToken" to batchToken) }

    return HandlerOutput(objectMapper.writeValueAsString(items))
}
```

---

### Task 1: Write tests for updated scatter behavior

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

- [ ] **Step 1: Add test for scatter with explicit configIds and batchToken (dry-run path)**

In `DispatchHandlersTest`, add:

```kotlin
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
```

Add imports: `kotlin.test.assertEquals`, `org.mockito.kotlin.never`.

- [ ] **Step 2: Add test for scatter with no item (normal cron — creates batch)**

In `DispatchHandlersTest`, add:

```kotlin
@Test
fun `scatter handler creates NORMAL batch and uses all active configs when no item`() = runTest {
    val configRepo = mock<DispatchConfigRepository>()
    val resultStore = mock<SimulationResultStore>()
    val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
        listOf(SiteTarget("A", BigDecimal("100"))), null)
    whenever(configRepo.findActiveConfigs(any())).thenReturn(listOf(config))

    val handler = DispatchScatterHandler(configRepo, resultStore, objectMapper)
    val output = handler.execute(
        HandlerInput("t1", "w1", 1, null, null),
    )

    val arr = objectMapper.readTree(output.result)
    assertTrue(arr.isArray)
    assertEquals(1, arr.size())
    assertEquals("cfg1", arr[0]["configId"].asText())
    assertTrue(arr[0]["batchToken"].asText().matches(Regex("\\d{14}")))

    verify(configRepo).findActiveConfigs(any())
    verify(configRepo, never()).findById(any())
    verify(resultStore).createBatch(
        argThat { matches(Regex("\\d{14}")) },
        eq(BatchStatus.NORMAL),
        eq(1),
    )
}
```

Add imports: `com.workflow.dispatch.model.BatchStatus`, `org.mockito.kotlin.argThat`, `org.mockito.kotlin.eq`.

- [ ] **Step 3: Run tests to verify the new tests fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: New tests FAIL (scatter handler constructor doesn't match). The existing test may also fail due to constructor change.

- [ ] **Step 4: Commit failing tests**

```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "test(dispatch): add scatter handler tests for optional configIds and batch creation"
```

---

### Task 2: Implement updated ScatterHandler

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchScatterHandler.kt`

- [ ] **Step 1: Update the handler**

Replace the full `DispatchScatterHandler` class:

```kotlin
package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

@ApplicationScoped
class DispatchScatterHandler(
    private val configRepo: DispatchConfigRepository,
    private val resultStore: SimulationResultStore,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val now = LocalDateTime.now()
        val itemNode = input.item?.let { objectMapper.readTree(it) }

        val batchTokenProvided = itemNode?.get("batchToken")?.asText()
        val batchToken = batchTokenProvided
            ?: now.truncatedTo(ChronoUnit.HOURS)
                .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

        val configIdsNode = itemNode?.get("configIds")
        val configs = if (configIdsNode != null && configIdsNode.isArray) {
            configIdsNode.map { configRepo.findById(it.asText()) }
        } else {
            configRepo.findActiveConfigs(now)
        }

        if (batchTokenProvided == null) {
            resultStore.createBatch(batchToken, BatchStatus.NORMAL, configs.size)
        }

        val items = configs.map { mapOf("configId" to it.id, "batchToken" to batchToken) }
        return HandlerOutput(objectMapper.writeValueAsString(items))
    }
}
```

- [ ] **Step 2: Update existing scatter test to match new constructor**

Update the existing `scatter handler returns JSON array of config items` test in `DispatchHandlersTest` to include the `resultStore` mock:

```kotlin
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
}
```

- [ ] **Step 3: Run all handler tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: All scatter tests PASS (existing + 2 new).

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/service/handler/DispatchScatterHandler.kt src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "feat(dispatch): scatter handler creates batch for cron runs, skips for dry-run"
```
