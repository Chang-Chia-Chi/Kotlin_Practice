# Phase 4 — Scheduler Per-Category Methods

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single `@Scheduled` method on `DispatchScheduler` with one method per `DispatchCategory`, all delegating to a shared private `trigger(categories: Set<DispatchCategory>)` helper. Replace the single `dispatch.cron` property with per-category property keys. Add a new `DispatchSchedulerTest` that exercises the helper's idempotency-key shaping and payload encoding.

**Architecture:** `DispatchScheduler` gains a dependency on `ObjectMapper` (to serialize the payload). The private helper computes a sorted, dash-joined key suffix (`ALL` for empty, otherwise `NORMAL-URGENT` style) so two sets with the same members always produce the same idempotency key. The test exercises the helper via package-private visibility — for Kotlin this means using `@VisibleForTesting internal` or calling the public `triggerXxx()` methods that the `@Scheduled` annotations decorate.

**Tech Stack:** Quarkus `@Scheduled`, Jackson `ObjectMapper`, Mockito Kotlin, `kotlinx-coroutines-test` `runTest`, `StartResult.Created` for mocked return values.

---

## Task 1 — TDD: Write failing `DispatchSchedulerTest`

**Files:**
- Create: `src/test/kotlin/dispatch/usecase/service/handler/DispatchSchedulerTest.kt`

Because the per-category `triggerXxx()` methods are the public entry points that `@Scheduled` decorates, test them directly. The private `trigger(categories)` helper is exercised transitively.

- [ ] **Step 1: Create the test file**

```kotlin
package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.model.DispatchCategory
import com.workflow.workflow.model.StartResult
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DispatchSchedulerTest {
    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    private fun newScheduler(engine: WorkflowEngine): DispatchScheduler =
        DispatchScheduler(engine, objectMapper)

    @Test
    fun `triggerUrgent emits URGENT-scoped idempotency key and payload`() = runTest {
        val engine = mock<WorkflowEngine>()
        whenever(engine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        newScheduler(engine).triggerUrgent()

        val defCaptor = argumentCaptor<WorkflowDefinition>()
        val keyCaptor = argumentCaptor<String?>()
        val itemCaptor = argumentCaptor<String?>()
        verify(engine).startWorkflow(defCaptor.capture(), keyCaptor.capture(), itemCaptor.capture())

        assertEquals(dispatchWorkflow, defCaptor.firstValue)
        assertTrue(
            keyCaptor.firstValue!!.startsWith("dispatch-URGENT-"),
            "expected idempotency key to start with 'dispatch-URGENT-', got ${keyCaptor.firstValue}",
        )
        val parsed = objectMapper.readTree(itemCaptor.firstValue!!)
        val categories = parsed["categories"].map { it.asText() }
        assertEquals(listOf("URGENT"), categories)
    }

    @Test
    fun `multi-category key is lexicographically sorted`() = runTest {
        val engine = mock<WorkflowEngine>()
        whenever(engine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))
        val scheduler = newScheduler(engine)

        // Invoke the internal helper via the package-private triggerBoth test seam added below.
        scheduler.triggerForTest(setOf(DispatchCategory.URGENT, DispatchCategory.NORMAL))

        val keyCaptor = argumentCaptor<String?>()
        verify(engine).startWorkflow(any(), keyCaptor.capture(), any())
        assertTrue(
            keyCaptor.firstValue!!.startsWith("dispatch-NORMAL-URGENT-"),
            "expected 'dispatch-NORMAL-URGENT-' (sorted), got ${keyCaptor.firstValue}",
        )
    }

    @Test
    fun `empty set produces ALL-scoped idempotency key`() = runTest {
        val engine = mock<WorkflowEngine>()
        whenever(engine.startWorkflow(any(), any(), any())).thenReturn(StartResult.Created("w1"))

        newScheduler(engine).triggerForTest(emptySet())

        val keyCaptor = argumentCaptor<String?>()
        verify(engine).startWorkflow(any(), keyCaptor.capture(), any())
        assertTrue(
            keyCaptor.firstValue!!.startsWith("dispatch-ALL-"),
            "expected 'dispatch-ALL-', got ${keyCaptor.firstValue}",
        )
    }
}
```

Note: this test uses a `triggerForTest` seam on `DispatchScheduler` that Task 2 will add. The seam is the ONLY way to exercise the helper for arbitrary sets without declaring a `@Scheduled` method for every combination.

- [ ] **Step 2: Add any missing Mockito Kotlin imports**

Confirm these imports exist; add any that are missing:

```kotlin
import org.mockito.kotlin.any
```

- [ ] **Step 3: Run the new test class — it must fail**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchSchedulerTest
```
Expected: **compile failure**, because (a) `DispatchScheduler` does not yet have a second constructor parameter for `ObjectMapper`, (b) `triggerUrgent` does not exist, (c) `triggerForTest` does not exist. Good — Task 2 makes them exist.

---

## Task 2 — Rewrite `DispatchScheduler`

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchScheduler.kt`

- [ ] **Step 1: Replace the file contents**

```kotlin
package com.workflow.dispatch.usecase.service.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dispatch.dsl.dispatchWorkflow
import com.workflow.dispatch.model.DispatchCategory
import com.workflow.infrastructure.leader.NotLeader
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.quarkus.scheduler.Scheduled
import io.smallrye.common.annotation.Blocking
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

@ApplicationScoped
class DispatchScheduler(
    private val workflowEngine: WorkflowEngine,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(DispatchScheduler::class.java)

    @Blocking
    @Scheduled(cron = "{dispatch.cron.urgent}", skipExecutionIf = NotLeader::class)
    fun triggerUrgent() = runBlocking { trigger(setOf(DispatchCategory.URGENT)) }

    @Blocking
    @Scheduled(cron = "{dispatch.cron.normal}", skipExecutionIf = NotLeader::class)
    fun triggerNormal() = runBlocking { trigger(setOf(DispatchCategory.NORMAL)) }

    @Blocking
    @Scheduled(cron = "{dispatch.cron.background}", skipExecutionIf = NotLeader::class)
    fun triggerBackground() = runBlocking { trigger(setOf(DispatchCategory.BACKGROUND)) }

    // Optional combined or all-categories entry points follow the same shape.
    // Operators add them when a single trigger should cover several categories at once:
    //
    // @Scheduled(cron = "{dispatch.cron.urgent-and-normal}", skipExecutionIf = NotLeader::class)
    // fun triggerUrgentAndNormal() = runBlocking {
    //     trigger(setOf(DispatchCategory.URGENT, DispatchCategory.NORMAL))
    // }
    //
    // @Scheduled(cron = "{dispatch.cron.all}", skipExecutionIf = NotLeader::class)
    // fun triggerAll() = runBlocking { trigger(emptySet()) }

    /** Test-only seam: exercise the private helper with an arbitrary set. */
    internal suspend fun triggerForTest(categories: Set<DispatchCategory>) = trigger(categories)

    private suspend fun trigger(categories: Set<DispatchCategory>) {
        val batchToken = currentBatchToken()
        val keyCats =
            if (categories.isEmpty()) "ALL"
            else categories.map { it.name }.sorted().joinToString("-")
        val payload = objectMapper.writeValueAsString(
            mapOf("categories" to categories.map { it.name }.sorted()),
        )
        val result = workflowEngine.startWorkflow(
            definition = dispatchWorkflow,
            idempotencyKey = "dispatch-$keyCats-$batchToken",
            initialItem = payload,
        )
        log.info(
            "Dispatch trigger: categories={}, batchToken={}, result={}",
            keyCats, batchToken, result,
        )
    }
}
```

- [ ] **Step 2: Verify the project compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test-compile -q`
Expected: BUILD SUCCESS. Note: Quarkus CDI will need an `ObjectMapper` bean available; Quarkus registers one automatically (bundled via `quarkus-resteasy-reactive-jackson` or the core Jackson extension), so no wiring changes are required.

---

## Task 3 — Replace `dispatch.cron` with per-category properties

**Files:**
- Modify: `src/main/resources/application.properties:64`

- [ ] **Step 1: Replace the single property with three**

Before (line 64):
```properties
dispatch.cron=${DISPATCH_CRON:0 0 0,6,12,18 * * ?}
```

After:
```properties
dispatch.cron.urgent=${DISPATCH_CRON_URGENT:0 0 0,6,12,18 * * ?}
dispatch.cron.normal=${DISPATCH_CRON_NORMAL:0 0 0,12 * * ?}
dispatch.cron.background=${DISPATCH_CRON_BACKGROUND:0 0 2 * * ?}
```

These default expressions are placeholders mirroring the original `DISPATCH_CRON` default — operators will override them via environment variables in production.

---

## Task 4 — Run the full test suite

- [ ] **Step 1: Run all tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q`
Expected: BUILD SUCCESS. The new `DispatchSchedulerTest` passes; no other tests regress.

Common failure modes to watch for:
- **Quarkus deployment fails** because `dispatch.cron` is referenced somewhere else. Search for `"dispatch.cron"` across the codebase — there should be no remaining references outside the three new keys.
- **`ObjectMapper` not injectable** — confirm Quarkus has a Jackson extension on the classpath. The existing handler tests already use `ObjectMapper` so this should be fine.

---

## Task 5 — Commit

- [ ] **Step 1: Stage**

```bash
git add src/main/kotlin/dispatch/usecase/service/handler/DispatchScheduler.kt
```
```bash
git add src/main/resources/application.properties
```
```bash
git add src/test/kotlin/dispatch/usecase/service/handler/DispatchSchedulerTest.kt
```

- [ ] **Step 2: Commit**

```bash
git commit -m "✨ feat(dispatch): per-category scheduler methods with set-encoded payload

Replaces the single @Scheduled method with triggerUrgent, triggerNormal,
and triggerBackground. All three delegate to a private helper that
sorts the category set into a deterministic idempotency-key suffix
(dispatch-NORMAL-URGENT-{token}) and serializes the set into the
initialItem payload the scatter handler now consumes. Empty set
produces dispatch-ALL-{token}. Drops the legacy dispatch.cron property
in favor of dispatch.cron.urgent / .normal / .background."
```

- [ ] **Step 3: Verify**

```bash
git status
```
Expected: working tree clean of Phase 4 changes.
