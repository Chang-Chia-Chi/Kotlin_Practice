# TriggerLoop E2E Integration Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the mock-based `TriggerLoopIntegrationTest` with a real E2E integration test that exercises `findDeferred` -> driver dispatch -> poll -> `TaskSettler` -> `DefaultPhaseGate` -> Oracle roundtrip, following the `WorkflowIntegrationTest` pattern.

**Architecture:** Manually wire real production classes (`TriggerLoop`, `TaskSettler`, `DefaultPhaseGate`, `JdbiTaskRepository`, `JdbiWorkflowRepository`) against `OracleTestContainer`. Test doubles only for: `LeaderGuard` (always-leader), `TriggerDriver` implementations, config objects. The test uses `WorkflowEngine` + manual `taskRepo.defer()` to set up DEFERRED tasks, then calls `loop.sweep()` to drive them.

**Tech Stack:** Kotlin, Oracle (OracleTestContainer), JDBI, JUnit 5

**Reference:** Follow the wiring pattern established in `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt`.

**Depends on:** The WorkerLoop E2E plan should be done first (shared patterns), but is not strictly required.

---

### Task 1: Replace TriggerLoopIntegrationTest with real E2E test — defer + settle

**Files:**
- Rewrite: `src/test/kotlin/worker/usecase/service/trigger/TriggerLoopIntegrationTest.kt`

- [ ] **Step 1: Rewrite the test file with real wiring**

Replace the entire contents of `TriggerLoopIntegrationTest.kt`:

```kotlin
package com.workflow.worker.usecase.service.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.service.TaskSettler
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Real E2E integration tests for [TriggerLoop] against OracleTestContainer.
 *
 * Unlike [TriggerLoopTest] (mock-based unit tests), these tests wire up real
 * [JdbiTaskRepository], [DefaultPhaseGate], and [TaskSettler] against Oracle,
 * using test [TriggerDriver] implementations to control trigger outcomes.
 *
 * Flow: WorkflowEngine creates workflow -> manually defer task -> TriggerLoop.sweep()
 * -> driver returns result -> TaskSettler settles via real PhaseGate -> verify Oracle state.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TriggerLoopIntegrationTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var engine: WorkflowEngine
    private lateinit var phaseGate: DefaultPhaseGate
    private val notifier = FakeWorkerNotifier()

    private val alwaysLeader = object : LeaderGuard {
        override val leaderState: StateFlow<Boolean> = MutableStateFlow(true)
    }

    private val testConfig = object : TriggerLoopConfig {
        override fun sweepInterval(): Duration = Duration.ofSeconds(5)
        override fun sqlMaxConcurrent(): Int = 2
    }

    private val testShutdownConfig = object : ShutdownConfig {
        override fun globalTimeout(): Duration = Duration.ofSeconds(10)
    }

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        phaseGate = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
    }

    @AfterEach
    fun cleanTables() {
        jdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    private fun buildLoop(vararg drivers: TriggerDriver): TriggerLoop {
        val beans = mock<Instance<TriggerDriver>> {
            on { iterator() } doAnswer { drivers.toMutableList().iterator() }
        }
        val taskSettler = TaskSettler(taskRepo, phaseGate)
        val loop = TriggerLoop(
            taskRepo, beans, taskSettler, alwaysLeader,
            SimpleMeterRegistry(), testConfig, testShutdownConfig,
        )
        // Initialize drivers map without starting the background loop
        val scope = TestScope(SupervisorJob())
        val job = loop.start(scope)
        job.cancel()
        return loop
    }

    /** Start a 1-step workflow and defer its task, returning the workflow ID. */
    private fun startAndDefer(
        handlerKey: String = "e2e.handler",
        triggerType: String,
        triggerMeta: String,
    ): String {
        val definition = workflow {
            activity("step1") { transition(handlerKey); retries(3) }
        }
        val wfId = runBlocking { engine.startWorkflow(definition) }.workflowId

        // Claim the PENDING task (simulates WorkerLoop claiming it)
        val tasks = runBlocking { taskRepo.claimNext("e2e-worker", 1) }
        assertEquals(1, tasks.size)

        // Defer it (simulates handler returning HandlerResult.Defer)
        val deferred = runBlocking { taskRepo.defer(tasks[0].id, triggerType, triggerMeta) }
        assertTrue(deferred)

        return wfId
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Defer → Settle → Workflow advances
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class DeferAndSettle {

        @Test
        fun `sweep settles DEFERRED task as COMPLETED and workflow advances to COMPLETED`() = runTest {
            val resultQueue = ConcurrentLinkedQueue<TriggerResult>()
            val driver = object : TriggerDriver {
                override fun type(): String = "test-trigger"
                override suspend fun start(tasks: List<DeferredTaskRef>) {
                    for (t in tasks) {
                        resultQueue.add(TriggerResult.Succeeded(t.taskId, """{"settled":true}"""))
                    }
                }
                override suspend fun poll(): List<TriggerResult> {
                    val results = mutableListOf<TriggerResult>()
                    while (true) { results.add(resultQueue.poll() ?: break) }
                    return results
                }
                override suspend fun cancel(taskId: String) {}
                override suspend fun close() {}
            }

            val loop = buildLoop(driver)
            val wfId = startAndDefer(triggerType = "test-trigger", triggerMeta = """{"key":"v"}""")

            // Verify DEFERRED task exists in Oracle
            val deferred = taskRepo.findDeferred()
            assertEquals(1, deferred.size)
            assertEquals("test-trigger", deferred[0].triggerType)

            // Sweep: findDeferred -> start driver -> poll -> settle via PhaseGate
            loop.sweep()

            // Verify: task settled, workflow COMPLETED
            val wf = workflowRepo.findById(wfId)
            assertEquals(WorkflowStatus.COMPLETED, wf?.status)

            // No more deferred tasks
            val remaining = taskRepo.findDeferred()
            assertTrue(remaining.isEmpty())
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Defer → Fail → Retry → Succeed
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class RetryLifecycle {

        @Test
        fun `sweep fails DEFERRED task, resetForRetry re-defers, second sweep succeeds`() = runTest {
            val attempts = java.util.concurrent.atomic.AtomicInteger(0)
            val resultQueue = ConcurrentLinkedQueue<TriggerResult>()
            val driver = object : TriggerDriver {
                override fun type(): String = "flaky-trigger"
                override suspend fun start(tasks: List<DeferredTaskRef>) {
                    for (t in tasks) {
                        val attempt = attempts.incrementAndGet()
                        if (attempt == 1) {
                            resultQueue.add(TriggerResult.Failed(t.taskId, "Transient error"))
                        } else {
                            resultQueue.add(TriggerResult.Succeeded(t.taskId, """{"retry":"ok"}"""))
                        }
                    }
                }
                override suspend fun poll(): List<TriggerResult> {
                    val results = mutableListOf<TriggerResult>()
                    while (true) { results.add(resultQueue.poll() ?: break) }
                    return results
                }
                override suspend fun cancel(taskId: String) {}
                override suspend fun close() {}
            }

            val loop = buildLoop(driver)
            val wfId = startAndDefer(triggerType = "flaky-trigger", triggerMeta = "{}")

            // Sweep 1: driver returns Failed -> resetForRetry
            loop.sweep()

            // Task should be back in DEFERRED state (resetForRetry resets to PENDING,
            // but since the handler originally deferred it, it needs to go through
            // the claim -> defer cycle again). Verify task is NOT in a terminal state.
            val wfAfterSweep1 = workflowRepo.findById(wfId)
            assertEquals(WorkflowStatus.RUNNING, wfAfterSweep1?.status)

            // The task was reset to PENDING by resetForRetry.
            // Re-claim and re-defer to simulate WorkerLoop processing it again.
            val reclaimedTasks = taskRepo.claimNext("e2e-worker", 1)
            assertEquals(1, reclaimedTasks.size)
            assertEquals(1, reclaimedTasks[0].retryCount)
            taskRepo.defer(reclaimedTasks[0].id, "flaky-trigger", "{}")

            // Sweep 2: driver returns Succeeded -> settle as COMPLETED
            loop.sweep()

            val wf = workflowRepo.findById(wfId)
            assertEquals(WorkflowStatus.COMPLETED, wf?.status)
            assertEquals(2, attempts.get())
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Cancel includes DEFERRED tasks
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class CancelDeferred {

        @Test
        fun `cancelPendingTasksWithHandle cancels DEFERRED tasks in Oracle`() = runTest {
            val driver = object : TriggerDriver {
                override fun type(): String = "cancel-trigger"
                override suspend fun start(tasks: List<DeferredTaskRef>) {}
                override suspend fun poll(): List<TriggerResult> = emptyList()
                override suspend fun cancel(taskId: String) {}
                override suspend fun close() {}
            }

            buildLoop(driver)
            val wfId = startAndDefer(triggerType = "cancel-trigger", triggerMeta = "{}")

            // Verify task is DEFERRED
            assertEquals(1, taskRepo.findDeferred().size)

            // Cancel all pending/deferred tasks via the real repo against Oracle
            val cancelled = jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.cancelPendingTasksWithHandle(handle, wfId)
            }

            assertEquals(1, cancelled)
            assertTrue(taskRepo.findDeferred().isEmpty())
        }
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test(trigger): rewrite TriggerLoopIntegrationTest as real E2E against Oracle
```

---

### Task 2: Add mixed workflow test — normal step + deferred step

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/trigger/TriggerLoopIntegrationTest.kt`

- [ ] **Step 1: Add a 2-step test combining WorkerLoop settle + TriggerLoop settle**

Add a new `@Nested` class:

```kotlin
@Nested
inner class MixedWorkflow {

    @Test
    fun `2-step workflow — step1 completes normally, step2 defers and TriggerLoop settles`() = runTest {
        val resultQueue = ConcurrentLinkedQueue<TriggerResult>()
        val driver = object : TriggerDriver {
            override fun type(): String = "test-trigger"
            override suspend fun start(tasks: List<DeferredTaskRef>) {
                for (t in tasks) {
                    resultQueue.add(TriggerResult.Succeeded(t.taskId, """{"trigger":"done"}"""))
                }
            }
            override suspend fun poll(): List<TriggerResult> {
                val results = mutableListOf<TriggerResult>()
                while (true) { results.add(resultQueue.poll() ?: break) }
                return results
            }
            override suspend fun cancel(taskId: String) {}
            override suspend fun close() {}
        }

        val loop = buildLoop(driver)

        // 2-step workflow: step1 (normal) -> step2 (will be deferred)
        val definition = workflow {
            activity("step1") { transition("e2e.handler"); next("step2") }
            activity("step2") { transition("e2e.handler") }
        }
        val wfId = engine.startWorkflow(definition).workflowId

        // Step 1: manually complete via PhaseGate (simulates WorkerLoop settling)
        val step1Tasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
        assertEquals(1, step1Tasks.size)
        phaseGate.onTaskCompleted(
            step1Tasks[0].id, wfId, 1, TaskStatus.COMPLETED, """{"step1":"ok"}""",
        )

        // Step 2: claim and defer (simulates WorkerLoop handler returning Defer)
        val step2Tasks = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(1, step2Tasks.size)
        assertEquals(TaskStatus.PENDING, step2Tasks[0].status)
        val claimed = taskRepo.claimNext("e2e-worker", 1)
        assertEquals(1, claimed.size)
        taskRepo.defer(claimed[0].id, "test-trigger", """{"step":"2"}""")

        // Verify DEFERRED
        assertEquals(1, taskRepo.findDeferred().size)

        // TriggerLoop sweep settles it
        loop.sweep()

        // Workflow should be COMPLETED
        val wf = workflowRepo.findById(wfId)
        assertEquals(WorkflowStatus.COMPLETED, wf?.status)
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test(trigger): add mixed normal + deferred workflow E2E test
```

---

### Task 3: Run full test suite

- [ ] **Step 1: Run all tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: PASS — no regressions. The old mock-based TriggerLoopIntegrationTest is fully replaced.

- [ ] **Step 2: Commit any fixes if needed**
