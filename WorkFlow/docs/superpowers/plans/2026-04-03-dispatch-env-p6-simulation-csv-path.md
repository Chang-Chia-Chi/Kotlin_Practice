# Dispatch Env P6: SimulationHandler Env-Aware CSV Path

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modify `DispatchSimulationHandler` to read batch status from the result store and use `DispatchPathBuilder` for CSV upload paths instead of hardcoded paths.

**Architecture:** Handler gets `DispatchPathBuilder` injected via constructor. After reading `batchToken` from input, it looks up `BatchStatus` via `resultStore.findBatchStatus()`, then uses `pathBuilder.csvPath()` for the upload path.

**Tech Stack:** Kotlin

**Current CSV path (hardcoded):** `dispatch/$batchToken/simulation/$configId.csv.gz`
**New CSV path:** `env={env}/mode={mode}/dispatch/{batchToken}/simulation/{configId}.csv.gz`

---

### Task 1: Write test for env-aware CSV path

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

- [ ] **Step 1: Add test for env-aware CSV upload path**

In `DispatchHandlersTest`, add:

```kotlin
@Test
fun `simulation handler uploads CSV to env-aware path`() = runTest {
    val configRepo = mock<DispatchConfigRepository>()
    val candidateQuery = mock<CandidateRepository>()
    val baselineProvider = mock<BaselineProvider>()
    val simulationEngine = mock<SimulationEngine>()
    val resultStore = mock<SimulationResultStore>()
    val storage = mock<StorageGateway>()
    val csvFormatter = mock<CsvFormatter>()
    val pathBuilder = DispatchPathBuilder("prod")

    val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
        listOf(SiteTarget("A", BigDecimal("100"))), null)
    whenever(configRepo.findById("cfg1")).thenReturn(config)
    whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
    whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
    whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
        SimulationResult(emptyList(), emptyMap(), emptyMap()),
    )
    whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())
    whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)

    val handler = DispatchSimulationHandler(
        configRepo, candidateQuery, baselineProvider, simulationEngine,
        resultStore, storage, csvFormatter, pathBuilder, objectMapper,
    )

    val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "20260329060000"))
    handler.execute(HandlerInput("t1", "w1", 2, null, item))

    verify(storage).uploadCsv(eq("env=prod/mode=normal/dispatch/20260329060000/simulation/cfg1.csv.gz"), any())
}

@Test
fun `simulation handler uses dryrun mode path for dryrun batch`() = runTest {
    val configRepo = mock<DispatchConfigRepository>()
    val candidateQuery = mock<CandidateRepository>()
    val baselineProvider = mock<BaselineProvider>()
    val simulationEngine = mock<SimulationEngine>()
    val resultStore = mock<SimulationResultStore>()
    val storage = mock<StorageGateway>()
    val csvFormatter = mock<CsvFormatter>()
    val pathBuilder = DispatchPathBuilder("prod")

    val config = DispatchConfig("cfg1", DispatchMode.QTY, "default", "bom",
        listOf(SiteTarget("A", BigDecimal("100"))), null)
    whenever(configRepo.findById("cfg1")).thenReturn(config)
    whenever(candidateQuery.queryCandidates(config)).thenReturn(emptyList())
    whenever(baselineProvider.loadBaseline(config)).thenReturn(Baseline(emptyMap(), emptyMap()))
    whenever(simulationEngine.simulate(eq(config), any(), any())).thenReturn(
        SimulationResult(emptyList(), emptyMap(), emptyMap()),
    )
    whenever(csvFormatter.format(any(), any(), any())).thenReturn(byteArrayOf())
    whenever(resultStore.findBatchStatus("dryrun-abc")).thenReturn(BatchStatus.DRYRUN)

    val handler = DispatchSimulationHandler(
        configRepo, candidateQuery, baselineProvider, simulationEngine,
        resultStore, storage, csvFormatter, pathBuilder, objectMapper,
    )

    val item = objectMapper.writeValueAsString(mapOf("configId" to "cfg1", "batchToken" to "dryrun-abc"))
    handler.execute(HandlerInput("t1", "w1", 2, null, item))

    verify(storage).uploadCsv(eq("env=prod/mode=dryrun/dispatch/dryrun-abc/simulation/cfg1.csv.gz"), any())
}
```

Add imports: `com.workflow.dispatch.adapter.storage.DispatchPathBuilder`, `com.workflow.dispatch.model.BatchStatus`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: New tests FAIL — `DispatchSimulationHandler` constructor doesn't accept `DispatchPathBuilder` yet.

- [ ] **Step 3: Commit failing tests**

```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "test(dispatch): add simulation handler tests for env-aware CSV paths"
```

---

### Task 2: Update SimulationHandler to use DispatchPathBuilder

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchSimulationHandler.kt`

- [ ] **Step 1: Add pathBuilder parameter and update CSV path logic**

Update `DispatchSimulationHandler` to add `DispatchPathBuilder` to its constructor and use it for path building:

```kotlin
package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.CsvFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import com.workflow.dispatch.usecase.service.simulation.SimulationEngine
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.nio.file.Files
import java.util.zip.GZIPOutputStream

@ApplicationScoped
class DispatchSimulationHandler(
    private val configRepo: DispatchConfigRepository,
    private val candidateQuery: CandidateRepository,
    private val baselineProvider: BaselineProvider,
    private val simulationEngine: SimulationEngine,
    private val resultStore: SimulationResultStore,
    private val storage: StorageGateway,
    private val csvFormatter: CsvFormatter,
    private val pathBuilder: DispatchPathBuilder,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val item = objectMapper.readTree(input.item!!)
        val configId = item["configId"].asText()
        val batchToken = item["batchToken"].asText()

        val config = configRepo.findById(configId)

        val result = simulationEngine.simulate(
            config = config,
            candidates = candidateQuery.queryCandidates(config),
            baseline = baselineProvider.loadBaseline(config),
        )

        resultStore.saveDecisions(batchToken, configId, result.decisions)

        val batchStatus = resultStore.findBatchStatus(batchToken)
        val csvPath = pathBuilder.csvPath(batchStatus, batchToken, configId)

        val csv = csvFormatter.format(batchToken, configId, result.decisions)
        val tmpFile = Files.createTempFile("dispatch-$configId-", ".csv.gz").toFile()
        try {
            GZIPOutputStream(tmpFile.outputStream()).use { it.write(csv) }
            storage.uploadCsv(csvPath, tmpFile)
        } finally {
            tmpFile.delete()
        }

        return HandlerOutput(
            objectMapper.writeValueAsString(
                mapOf("configId" to configId, "batchToken" to batchToken),
            ),
        )
    }
}
```

- [ ] **Step 2: Update existing simulation test to pass pathBuilder**

In `DispatchHandlersTest`, update the existing `simulation handler calls engine and uploads CSV` test to pass a `DispatchPathBuilder("prod")` and stub `resultStore.findBatchStatus`. Also update the expected upload path:

Change the handler construction to:
```kotlin
val pathBuilder = DispatchPathBuilder("prod")
```
Add it as a constructor parameter:
```kotlin
val handler = DispatchSimulationHandler(
    configRepo, candidateQuery, baselineProvider, simulationEngine,
    resultStore, storage, csvFormatter, pathBuilder, objectMapper,
)
```
Add mock stub:
```kotlin
whenever(resultStore.findBatchStatus("20260329060000")).thenReturn(BatchStatus.NORMAL)
```
Update the verify:
```kotlin
verify(storage).uploadCsv(eq("env=prod/mode=normal/dispatch/20260329060000/simulation/cfg1.csv.gz"), any())
```

- [ ] **Step 3: Run all handler tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/service/handler/DispatchSimulationHandler.kt src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "feat(dispatch): simulation handler uses env-aware CSV paths via DispatchPathBuilder"
```
