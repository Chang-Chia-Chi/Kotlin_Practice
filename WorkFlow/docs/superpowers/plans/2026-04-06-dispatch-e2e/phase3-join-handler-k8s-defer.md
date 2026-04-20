# Phase 3: DispatchJoinHandler K8s Defer Path

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modify `DispatchJoinHandler` to return `HandlerResult.Defer(K8S_JOB, ...)` after completing the DuckDB Parquet conversion and upload. The K8s Job is a lightweight post-processing step.

**Architecture:** The handler does the heavy work (query Oracle, DuckDB → Parquet, upload to MinIO) then defers to a K8s Job for post-processing. Job name is derived from batch token: `dispatch-join-{batchToken}`.

**Tech Stack:** Kotlin, Fabric8 K8s client, existing `deferK8sJob()` helper from `TriggerTypes.kt`

---

### Task 1: Update DispatchJoinHandler Tests

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

- [ ] **Step 1: Read the existing join handler test**

Read `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt` and locate the join handler test section. Understand the current assertions that expect `HandlerResult.Completed(null)`.

- [ ] **Step 2: Update the join handler test to expect Defer**

Find the test that verifies `DispatchJoinHandler.execute()` and update the assertion from:

```kotlin
assertEquals(HandlerResult.Completed(null), result)
```

to assert that the result is a `Defer` with K8s trigger type:

```kotlin
val result = handler.execute(input)
assertTrue(result is HandlerResult.Defer)
val defer = result as HandlerResult.Defer
assertEquals(TriggerTypes.K8S_JOB, defer.triggerType)
assertTrue(defer.triggerMeta.contains(batchToken))
```

Also add the required import:

```kotlin
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DispatchHandlersTest"`
Expected: FAIL — handler still returns `Completed(null)`.

- [ ] **Step 4: Commit the failing test**

```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "test: update join handler test to expect K8s Defer (red)"
```

---

### Task 2: Modify DispatchJoinHandler to Defer

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchJoinHandler.kt`

- [ ] **Step 1: Update the handler to return Defer**

Replace the current `DispatchJoinHandler.execute()` method. The full updated file:

```kotlin
package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.adapter.storage.DispatchPathBuilder
import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import com.workflow.worker.usecase.port.inbound.trigger.deferK8sJob
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class DispatchJoinHandler(
    private val resultStore: SimulationResultStore,
    private val storage: StorageGateway,
    private val parquetFormatter: ParquetFormatter,
    private val pathBuilder: DispatchPathBuilder,
    @ConfigProperty(name = "dispatch.env", defaultValue = "prod") private val env: String,
    @ConfigProperty(name = "dispatch.k8s.namespace", defaultValue = "default") private val namespace: String,
    private val objectMapper: ObjectMapper,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val inputsNode = objectMapper.readTree(input.inputs!!)
        val batchTokenNode = inputsNode["batchToken"]
        val batchToken = when {
            batchTokenNode.isArray -> batchTokenNode[0].asText()
            else -> batchTokenNode.asText()
        }

        val batchStatus = resultStore.findBatchStatus(batchToken)

        if (env == "prod" && batchStatus == BatchStatus.NORMAL) {
            val allDecisions = resultStore.findByBatchToken(batchToken)
            val parquet = parquetFormatter.format(allDecisions)
            storage.uploadParquet(pathBuilder.prodParquetPath(), parquet)
        }

        val jobName = "dispatch-join-$batchToken"
        return deferK8sJob(jobName, namespace)
    }
}
```

- [ ] **Step 2: Add the new config property to application.properties**

Add to `src/main/resources/application.properties` in the dispatch section:

```properties
dispatch.k8s.namespace=${DISPATCH_K8S_NAMESPACE:default}
```

- [ ] **Step 3: Run the handler tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DispatchHandlersTest"`
Expected: All tests PASS.

- [ ] **Step 4: Run the full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/service/handler/DispatchJoinHandler.kt
git add src/main/resources/application.properties
git commit -m "feat: DispatchJoinHandler defers to K8s Job after Parquet upload"
```
