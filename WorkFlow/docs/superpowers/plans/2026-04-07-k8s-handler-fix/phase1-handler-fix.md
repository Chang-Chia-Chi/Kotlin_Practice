# K8s Handler Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `DispatchJoinHandler` to return `HandlerResult.Completed` directly, delete the now-unused `HandlerResult.Defer` type, and update all affected tests including the E2E happy path.

**Architecture:** TDD order — update test assertions first so they fail, then fix the implementation, then clean up the dead type and all its compile-time references.

**Tech Stack:** Kotlin, Quarkus, Mockito-Kotlin, kotlinx-coroutines-test

---

### Task 1: Update DispatchHandlersTest — Join Handler Assertions (Red)

**Files:**
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

- [ ] **Step 1: Remove `namespace` arg and replace Defer assertions in 4 join handler tests**

Replace the constructor call and assertions in all four join handler tests. Each test passes `"default"` as the 6th arg (namespace) — remove it. Each test asserts `HandlerResult.Defer` — replace with `HandlerResult.Completed`.

Test: `join handler uploads parquet with merged results` (currently lines ~325–353):

```kotlin
val handler =
    DispatchJoinHandler(
        resultStore,
        storage,
        parquetFormatter,
        pathBuilder,
        "prod",
        objectMapper,          // namespace removed
    )

val inputs =
    objectMapper.writeValueAsString(
        mapOf("batchToken" to "20260329060000", "20260329060000"),
    )
val result =
    handler.execute(
        HandlerInput("t1", "w1", 3, inputs, null),
    )

verify(storage).uploadParquet(eq("env=prod/dispatch/result.parquet"), any())
assertTrue(result is HandlerResult.Completed)
assertNull((result as HandlerResult.Completed).result)
```

Test: `join handler exports parquet for prod normal batch` (currently lines ~368–392):

```kotlin
val handler =
    DispatchJoinHandler(
        resultStore,
        storage,
        parquetFormatter,
        pathBuilder,
        "prod",
        objectMapper,
    )

val inputs =
    objectMapper.writeValueAsString(
        mapOf("batchToken" to "20260329060000"),
    )
val result = handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

verify(storage).uploadParquet(eq("env=prod/dispatch/result.parquet"), any())
assertTrue(result is HandlerResult.Completed)
assertNull((result as HandlerResult.Completed).result)
```

Test: `join handler skips parquet for prod dryrun batch` (currently lines ~404–430):

```kotlin
val handler =
    DispatchJoinHandler(
        resultStore,
        storage,
        parquetFormatter,
        pathBuilder,
        "prod",
        objectMapper,
    )

val inputs =
    objectMapper.writeValueAsString(
        mapOf("batchToken" to "dryrun-abc"),
    )
val result = handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

verify(storage, never()).uploadParquet(any(), any())
verify(parquetFormatter, never()).format(any())
assertTrue(result is HandlerResult.Completed)
assertNull((result as HandlerResult.Completed).result)
```

Test: `join handler skips parquet for stg env` (currently lines ~443–469):

```kotlin
val handler =
    DispatchJoinHandler(
        resultStore,
        storage,
        parquetFormatter,
        pathBuilder,
        "stg",
        objectMapper,
    )

val inputs =
    objectMapper.writeValueAsString(
        mapOf("batchToken" to "20260329060000"),
    )
val result = handler.execute(HandlerInput("t1", "w1", 3, inputs, null))

verify(resultStore).findBatchStatus("20260329060000")
verify(storage, never()).uploadParquet(any(), any())
verify(parquetFormatter, never()).format(any())
assertTrue(result is HandlerResult.Completed)
assertNull((result as HandlerResult.Completed).result)
```

- [ ] **Step 2: Remove unused imports from DispatchHandlersTest**

Remove:
```kotlin
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import com.fasterxml.jackson.module.kotlin.readValue   // only if no other test uses it
```

Add if missing:
```kotlin
import kotlin.test.assertNull
```

- [ ] **Step 3: Run DispatchHandlersTest to confirm 4 failures**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DispatchHandlersTest"`
Expected: 4 FAIL (handler still returns `Defer`, constructor still requires `namespace`)

---

### Task 2: Fix DispatchJoinHandler

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchJoinHandler.kt`

- [ ] **Step 1: Remove `namespace` param, remove `deferK8sJob` import, return `Completed`**

Replace the entire file content:

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
    override suspend fun execute(input: HandlerInput): HandlerResult {
        val inputsNode = objectMapper.readTree(input.inputs!!)
        val batchTokenNode = inputsNode["batchToken"]
        val batchToken =
            when {
                batchTokenNode.isArray -> batchTokenNode[0].asText()
                else -> batchTokenNode.asText()
            }
        val batchStatus = resultStore.findBatchStatus(batchToken)

        if (env == "prod" && batchStatus == BatchStatus.NORMAL) {
            val allDecisions = resultStore.findByBatchToken(batchToken)
            val parquet = parquetFormatter.format(allDecisions)
            storage.uploadParquet(pathBuilder.prodParquetPath(), parquet)
        }

        return HandlerResult.Completed(result = null)
    }
}
```

- [ ] **Step 2: Run DispatchHandlersTest — confirm 4 pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DispatchHandlersTest"`
Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/usecase/service/handler/DispatchJoinHandler.kt
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt
git commit -m "fix: DispatchJoinHandler returns Completed directly"
```

---

### Task 3: Delete HandlerResult.Defer and Fix All Compile Errors

**Files:**
- Modify: `src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt`
- Modify: `src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt`
- Modify: `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt`
- Modify: `src/test/kotlin/worker/usecase/port/inbound/execution/HandlerResultTest.kt`
- Modify: `src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerTypesTest.kt`
- Modify: `src/test/kotlin/worker/usecase/service/execution/WorkerLoopTest.kt`

- [ ] **Step 1: Delete `HandlerResult.Defer` subtype**

Replace `src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.execution

sealed interface HandlerResult {
    data class Completed(
        val result: String?,
        val items: String? = null,
    ) : HandlerResult
}
```

- [ ] **Step 2: Remove `Defer` branch from WorkerLoop**

In `src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt`, remove the entire `is HandlerResult.Defer -> { ... }` branch from `executeAndReport()`. The `when` block becomes:

```kotlin
when (result) {
    is HandlerResult.Completed -> {
        try {
            taskSettler.settle(
                taskId = task.id,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                status = TaskStatus.COMPLETED,
                resultJson = result.result,
                itemsJson = result.items,
                claimedBy = task.claimedBy,
                claimedAt = task.claimedAt,
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Barrier failed for COMPLETED task {}, falling through to failure path", task.id, e)
            handleTaskFailure(task, e)
        }
    }
}
```

- [ ] **Step 3: Remove `deferK8sJob` function from TriggerTypes.kt**

Replace `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

object TriggerTypes {
    const val K8S_JOB = "k8s-job"
}
```

- [ ] **Step 4: Fix HandlerResultTest — remove Defer tests, update exhaustive when**

Replace `src/test/kotlin/worker/usecase/port/inbound/execution/HandlerResultTest.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.execution

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class HandlerResultTest {

    @Test
    fun `HandlerResult Completed wraps result`() {
        val result = HandlerResult.Completed(result = "some-output")
        assertEquals("some-output", result.result)
    }

    @Test
    fun `HandlerResult Completed with null result`() {
        val result = HandlerResult.Completed(result = null)
        assertNull(result.result)
    }

    @Test
    fun `exhaustive when on HandlerResult`() {
        val results: List<HandlerResult> = listOf(
            HandlerResult.Completed(result = "done"),
        )
        val labels = results.map { hr ->
            when (hr) {
                is HandlerResult.Completed -> "completed"
            }
        }
        assertEquals(listOf("completed"), labels)
    }
}
```

- [ ] **Step 5: Fix TriggerTypesTest — remove deferK8sJob test**

Replace `src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerTypesTest.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

import kotlin.test.Test
import kotlin.test.assertEquals

class TriggerTypesTest {

    @Test
    fun `TriggerTypes constants have expected values`() {
        assertEquals("k8s-job", TriggerTypes.K8S_JOB)
    }
}
```

- [ ] **Step 6: Remove 5 Defer branch tests from WorkerLoopTest**

In `src/test/kotlin/worker/usecase/service/execution/WorkerLoopTest.kt`, delete the following 5 tests (search by name):
- `` `handler returning Defer calls taskRepo defer and does not call phaseGate` ``
- `` `handler returning Defer when defer fails falls through to handleTaskFailure` ``
- `` `handler returning Defer when taskRepo defer throws delegates to handleTaskFailure` ``
- `` `handler returning Defer when taskRepo defer throws and retries exhausted reports FAILED to barrier` ``
- `` `handler returning Defer when defer fails and retries exhausted reports FAILED to barrier` ``

- [ ] **Step 7: Compile check**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: BUILD SUCCESS with no errors.

- [ ] **Step 8: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="HandlerResultTest,TriggerTypesTest,WorkerLoopTest"`
Expected: All PASS.

- [ ] **Step 9: Commit**

```bash
git add src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt
git add src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt
git add src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt
git add src/test/kotlin/worker/usecase/port/inbound/execution/HandlerResultTest.kt
git add src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerTypesTest.kt
git add src/test/kotlin/worker/usecase/service/execution/WorkerLoopTest.kt
git commit -m "refactor: delete HandlerResult.Defer and clean up all references"
```

---

### Task 4: Update DispatchE2EHappyPathTest

**Files:**
- Modify: `src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt`

- [ ] **Step 1: Remove K8s test resource annotation and injections**

Remove from class annotations:
```kotlin
@QuarkusTestResource(K8sMockServerResource::class)
```

Remove injected fields:
```kotlin
@Inject
lateinit var k8sClient: KubernetesClient

@Inject
lateinit var triggerDriver: K8sJobTriggerDriver
```

Remove imports:
```kotlin
import com.workflow.infrastructure.k8s.K8sMockServerResource
import com.workflow.worker.adapter.trigger.K8sJobTriggerDriver
import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobConditionBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder
import io.fabric8.kubernetes.client.KubernetesClient
```

- [ ] **Step 2: Replace Steps 4–5 with a single COMPLETED assertion**

Remove Step 4 (await DEFERRED) and Step 5 (push K8s job + ConfigMap + Watch diagnostic).

Replace with:

```kotlin
// Step 4: Await join task COMPLETED
await().atMost(15, TimeUnit.SECONDS).untilAsserted {
    val tasks = findTasksByWorkflowId(workflowId)
    val joinTask = tasks.find { it["HANDLER_KEY"] == "DispatchJoinHandler" }
    assertEquals("COMPLETED", joinTask?.get("STATUS"), "Join task should be COMPLETED")
}
```

Step 6 (await workflow COMPLETED) and all assertions remain unchanged.

- [ ] **Step 3: Run the E2E test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DispatchE2EHappyPathTest"`
Expected: PASS.

- [ ] **Step 4: Run the full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt
git commit -m "test: update E2E happy path — join task completes directly, no K8s step"
```
