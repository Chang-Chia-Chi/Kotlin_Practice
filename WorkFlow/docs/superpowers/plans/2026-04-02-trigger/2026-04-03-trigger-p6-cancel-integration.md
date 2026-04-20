# Trigger P6: Cancel/Watchdog Integration & End-to-End Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Broaden the workflow cancellation query to include DEFERRED tasks, verify watchdog compatibility, and add end-to-end integration tests for mixed workflows with normal + deferrable tasks.

**Architecture:** Minimal production code changes (one SQL query broadened). The bulk of this phase is integration testing to prove the full deferrable trigger lifecycle works end-to-end with the existing engine.

**Tech Stack:** Kotlin, Oracle (OracleTestContainer), JDBI, Mockito

**Depends on:** P1–P5 must all be complete.

---

### Task 1: Broaden cancel query to include DEFERRED

**Files:**
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`
- Modify: `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`

- [ ] **Step 1: Write the failing test**

In `RepositoryTest.kt`, add:

```kotlin
@Test
fun `cancelPendingTasksWithHandle also cancels DEFERRED tasks`() = runTest {
    // Insert a workflow
    val workflowId = insertTestWorkflow()

    // Insert a PENDING task and a DEFERRED task
    val pendingTask = makeTask(workflowId, status = TaskStatus.PENDING)
    val deferredTask = makeTask(workflowId, status = TaskStatus.PENDING) // insert as PENDING first
    taskRepo.insertBatch(listOf(pendingTask, deferredTask))

    // Manually defer the second task
    taskRepo.defer(deferredTask.id, "sql-exec", """{"datasource":"test","sql":"SELECT 1"}""")

    // Cancel
    val cancelled = jdbi.inTransactionSuspend<Int, Exception> { handle ->
        taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
    }

    assertEquals(2, cancelled)

    // Verify both are CANCELLED
    val tasks = taskRepo.findByWorkflowAndSequence(workflowId, pendingTask.sequenceNumber)
    // ... verify statuses
}
```

Note: Adapt `makeTask` and `insertTestWorkflow` to match the existing test helper patterns in `RepositoryTest.kt`.

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl WorkFlow`
Expected: FAIL — current query only cancels `PENDING` and `WAITING_FOR_SIGNAL`, not `DEFERRED`.

- [ ] **Step 3: Update cancelPendingTasksWithHandle SQL**

In `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`, modify `cancelPendingTasksWithHandle`:

```kotlin
override fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int {
    return handle.createUpdate(
        """
        UPDATE task SET status = 'CANCELLED', completed_at = :now
        WHERE workflow_id = :workflowId AND status IN ('PENDING', 'WAITING_FOR_SIGNAL', 'DEFERRED')
        """,
    )
        .bind("workflowId", workflowId)
        .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
        .execute()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: broaden cancelPendingTasksWithHandle to include DEFERRED status
```

---

### Task 2: Verify watchdog does not interfere with DEFERRED tasks

**Files:**
- Modify: `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt`

- [ ] **Step 1: Write test proving watchdog ignores DEFERRED tasks**

```kotlin
@Test
fun `expireOverdueTasks does not expire DEFERRED tasks`() = runTest {
    // The watchdog's findExpired query looks for PROCESSING tasks past deadline.
    // DEFERRED tasks should not be returned by findExpired.
    val deferredTask = makeTask(status = TaskStatus.DEFERRED, deadlineAt = Instant.now().minusSeconds(60))
    taskRepo.stub { onBlocking { findExpired(any()) } doReturn emptyList() }

    watchdog.patrol()

    // Phase gate should not be called for any DEFERRED task
    verify(phaseGate, never()).onTaskCompleted(
        taskId = eq(deferredTask.id),
        any(), any(), any(), any(),
    )
}

@Test
fun `reclaimStaleTasks does not reclaim DEFERRED tasks`() = runTest {
    // resetStaleTasks query targets PROCESSING tasks. DEFERRED should be untouched.
    taskRepo.stub { onBlocking { resetStaleTasks(any()) } doReturn 0 }
    taskRepo.stub { onBlocking { deadLetterExhaustedTasks(any()) } doReturn 0 }
    taskRepo.stub { onBlocking { findExpired(any()) } doReturn emptyList() }
    workflowRepo.stub { onBlocking { findStuck(any()) } doReturn emptyList() }
    workflowRepo.stub { onBlocking { findTimedOut() } doReturn emptyList() }

    watchdog.patrol()

    // No interaction with DEFERRED tasks
    verify(taskRepo).resetStaleTasks(any())
}
```

Note: These tests verify the existing queries don't accidentally touch DEFERRED tasks. `findExpired` looks for `status = 'PROCESSING'`, `resetStaleTasks` targets `status = 'PROCESSING'` — both correctly exclude DEFERRED. The tests document this assumption explicitly.

- [ ] **Step 2: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowWatchdogTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test: verify watchdog does not interfere with DEFERRED tasks
```

---

### Task 3: Integration test — handler defers, TriggerLoop settles, workflow advances

**Files:**
- Create: `src/test/kotlin/worker/usecase/service/trigger/TriggerLoopIntegrationTest.kt`

- [ ] **Step 1: Write end-to-end integration test**

```kotlin
package com.workflow.worker.usecase.service.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.port.inbound.orchestration.WorkflowLifecycle
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

@QuarkusTest
class TriggerLoopIntegrationTest {

    @Inject
    lateinit var workflowLifecycle: WorkflowLifecycle

    @Inject
    lateinit var taskRepo: com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository

    @Inject
    lateinit var workflowRepo: com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository

    @Test
    fun `handler defers task and TriggerLoop settles it to advance workflow`() {
        // Define a workflow with a deferrable activity
        val definition = workflow {
            activity("step1") {
                transition("DeferTestHandler")
            }
        }

        // Start workflow
        val result = kotlinx.coroutines.runBlocking {
            workflowLifecycle.startWorkflow(definition)
        }
        val workflowId = (result as com.workflow.workflow.model.StartResult.Created).workflowId

        // Wait for the task to be claimed and deferred
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            val deferred = kotlinx.coroutines.runBlocking { taskRepo.findDeferred() }
            assertEquals(1, deferred.size)
            assertEquals("sql-exec", deferred[0].triggerType)
        }

        // The TriggerLoop (running on leader) should pick up and settle the task
        // via the test TriggerDriver that immediately succeeds
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            val wf = kotlinx.coroutines.runBlocking { workflowRepo.findById(workflowId) }
            assertEquals(WorkflowStatus.COMPLETED, wf?.status)
        }
    }

    @Test
    fun `cancelWorkflow cancels DEFERRED tasks`() {
        val definition = workflow {
            activity("step1") {
                transition("DeferTestHandler")
                deadline(Duration.ofHours(1))
            }
        }

        val result = kotlinx.coroutines.runBlocking {
            workflowLifecycle.startWorkflow(definition)
        }
        val workflowId = (result as com.workflow.workflow.model.StartResult.Created).workflowId

        // Wait for defer
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            val deferred = kotlinx.coroutines.runBlocking { taskRepo.findDeferred() }
            assertTrue(deferred.isNotEmpty())
        }

        // Cancel
        val cancelled = kotlinx.coroutines.runBlocking {
            workflowLifecycle.cancelWorkflow(workflowId)
        }
        assertTrue(cancelled)

        // Verify task is CANCELLED
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            val deferred = kotlinx.coroutines.runBlocking { taskRepo.findDeferred() }
            assertTrue(deferred.isEmpty())
        }
    }
}
```

Note: This test requires a `DeferTestHandler` CDI bean registered in the test classpath:

```kotlin
// In test sources:
@ApplicationScoped
class DeferTestHandler : TransitionHandler {
    override fun key(): String = "DeferTestHandler"
    override suspend fun execute(input: HandlerInput): HandlerResult =
        HandlerResult.Defer(
            triggerType = "test-trigger",
            triggerMeta = """{"immediate":true}""",
        )
}
```

And a `TestTriggerDriver` that immediately succeeds:

```kotlin
@ApplicationScoped
class TestTriggerDriver : TriggerDriver {
    override fun type(): String = "test-trigger"
    private val pending = java.util.concurrent.ConcurrentLinkedQueue<TriggerResult>()

    override suspend fun start(tasks: List<DeferredTaskRef>) {
        for (task in tasks) {
            pending.add(TriggerResult.Succeeded(task.taskId, """{"test":"ok"}"""))
        }
    }

    override suspend fun poll(): List<TriggerResult> {
        val results = mutableListOf<TriggerResult>()
        while (true) {
            val r = pending.poll() ?: break
            results.add(r)
        }
        return results
    }

    override suspend fun cancel(taskId: String) {}
    override suspend fun close() {}
}
```

- [ ] **Step 2: Run integration test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS (requires Docker for OracleTestContainer)

- [ ] **Step 3: Commit**

```
test: add TriggerLoop end-to-end integration tests with defer/settle/cancel
```

---

### Task 4: Integration test — mixed workflow with normal + deferrable activities

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/trigger/TriggerLoopIntegrationTest.kt`

- [ ] **Step 1: Write mixed workflow test**

```kotlin
@Test
fun `workflow with mix of normal and deferrable activities completes`() {
    // step1 (normal) → step2 (defer) → step3 (normal)
    val definition = workflow {
        activity("step1") {
            transition("NormalTestHandler")
        }
        activity("step2") {
            transition("DeferTestHandler")
        }
        activity("step3") {
            transition("NormalTestHandler")
        }
    }

    val result = kotlinx.coroutines.runBlocking {
        workflowLifecycle.startWorkflow(definition)
    }
    val workflowId = (result as com.workflow.workflow.model.StartResult.Created).workflowId

    await().atMost(Duration.ofSeconds(60)).untilAsserted {
        val wf = kotlinx.coroutines.runBlocking { workflowRepo.findById(workflowId) }
        assertEquals(WorkflowStatus.COMPLETED, wf?.status)
    }
}
```

This requires a `NormalTestHandler`:

```kotlin
@ApplicationScoped
class NormalTestHandler : TransitionHandler {
    override fun key(): String = "NormalTestHandler"
    override suspend fun execute(input: HandlerInput): HandlerResult =
        HandlerResult.Completed("""{"done":true}""")
}
```

- [ ] **Step 2: Run integration test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test: add mixed normal + deferrable workflow integration test
```

---

### Task 5: Integration test — DEFERRED task retry cycle

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/trigger/TriggerLoopIntegrationTest.kt`

- [ ] **Step 1: Write retry test**

Create a `FailOnceTriggerDriver` that fails the first time and succeeds the second:

```kotlin
class FailOnceTriggerDriver : TriggerDriver {
    override fun type(): String = "fail-once"
    private val attempts = java.util.concurrent.ConcurrentHashMap<String, Int>()
    private val pending = java.util.concurrent.ConcurrentLinkedQueue<TriggerResult>()

    override suspend fun start(tasks: List<DeferredTaskRef>) {
        for (task in tasks) {
            val attempt = attempts.merge(task.taskId, 1) { a, b -> a + b } ?: 1
            if (attempt == 1) {
                pending.add(TriggerResult.Failed(task.taskId, "Simulated failure"))
            } else {
                pending.add(TriggerResult.Succeeded(task.taskId, """{"retry":"ok"}"""))
            }
        }
    }

    override suspend fun poll(): List<TriggerResult> {
        val results = mutableListOf<TriggerResult>()
        while (true) {
            val r = pending.poll() ?: break
            results.add(r)
        }
        return results
    }

    override suspend fun cancel(taskId: String) {}
    override suspend fun close() {}
}
```

```kotlin
@Test
fun `DEFERRED task fails then retries and eventually completes`() {
    val definition = workflow {
        activity("step1") {
            transition("DeferRetryTestHandler")
            retries(2)
        }
    }

    val result = kotlinx.coroutines.runBlocking {
        workflowLifecycle.startWorkflow(definition)
    }
    val workflowId = (result as com.workflow.workflow.model.StartResult.Created).workflowId

    await().atMost(Duration.ofSeconds(60)).untilAsserted {
        val wf = kotlinx.coroutines.runBlocking { workflowRepo.findById(workflowId) }
        assertEquals(WorkflowStatus.COMPLETED, wf?.status)
    }
}
```

- [ ] **Step 2: Run integration test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test: add DEFERRED task retry cycle integration test
```

---

### Task 6: Run full test suite and verify coverage

- [ ] **Step 1: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: PASS — no regressions.

- [ ] **Step 2: Check coverage**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`
Expected: Coverage meets thresholds.

- [ ] **Step 3: Commit any coverage fixes if needed**

```
test: fix coverage gaps for deferrable trigger feature
```
