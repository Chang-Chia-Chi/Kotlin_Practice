# WorkerLoop E2E Integration Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real E2E integration test for `WorkerLoop` that exercises `claimNext` -> handler execution -> `TaskSettler` -> `DefaultPhaseGate` -> Oracle roundtrip, following the `WorkflowIntegrationTest` pattern.

**Architecture:** Manually wire real production classes (`WorkerLoop`, `HandlerRegistry`, `TaskSettler`, `DefaultPhaseGate`, `ActivityInputResolver`, `JdbiTaskRepository`, `JdbiWorkflowRepository`) against `OracleTestContainer`. Test doubles only for external boundaries: `FakeWorkerNotifier`, test `TransitionHandler` implementations, and config objects. The loop runs in a real `CoroutineScope`; assertions use `Awaitility`.

**Tech Stack:** Kotlin, Oracle (OracleTestContainer), JDBI, Awaitility, JUnit 5

**Reference:** Follow the wiring pattern established in `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt`.

---

### Task 1: Create WorkerLoopIntegrationTest with linear workflow E2E

**Files:**
- Create: `src/test/kotlin/worker/usecase/service/execution/WorkerLoopIntegrationTest.kt`

- [ ] **Step 1: Write the test class with wiring and first test**

```kotlin
package com.workflow.worker.usecase.service.execution

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import com.workflow.worker.config.WorkerLoopConfig
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import com.workflow.worker.usecase.service.TaskSettler
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.service.orchestration.ActivityInputResolver
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import java.time.Duration
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerLoopIntegrationTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var engine: WorkflowEngine
    private lateinit var phaseGate: DefaultPhaseGate
    private val notifier = FakeWorkerNotifier()

    private val testWorkerConfig = object : WorkerLoopConfig {
        override fun id(): String = "e2e-worker"
        override fun pollInterval(): Duration = Duration.ofMillis(200)
        override fun fallbackPollInterval(): Duration = Duration.ofMillis(500)
        override fun concurrency(): Int = 1
        override fun batchSize(): Int = 1
        override fun maxBatchSize(): Int = 16
        override fun podIp(): String = "localhost"
        override fun appName(): String = "workflow-engine"
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

    private fun buildWorkerLoop(vararg handlers: TransitionHandler): Pair<WorkerLoop, Job> {
        val handlerBeans = mock<Instance<TransitionHandler>> {
            on { iterator() } doAnswer { handlers.toMutableList().iterator() }
        }
        val handlerRegistry = HandlerRegistry(handlerBeans)
        val taskSettler = TaskSettler(taskRepo, phaseGate)
        val inputResolver = ActivityInputResolver(objectMapper)
        val loop = WorkerLoop(
            testWorkerConfig, testShutdownConfig, taskRepo, handlerRegistry,
            taskSettler, SimpleMeterRegistry(), inputResolver, workflowRepo,
            objectMapper, notifier,
        )
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val job = loop.start(scope)
        return loop to job
    }

    @Nested
    inner class LinearWorkflow {

        @Test
        fun `WorkerLoop claims and completes 2-step linear workflow end-to-end`() {
            // Handler that immediately completes
            val handler = object : TransitionHandler {
                override fun key(): String = "e2e.complete"
                override suspend fun execute(input: HandlerInput): HandlerResult =
                    HandlerResult.Completed("""{"step":"${input.sequenceNumber}"}""")
            }

            val (_, job) = buildWorkerLoop(handler)
            try {
                // Start a 2-step linear workflow
                val definition = workflow {
                    activity("step1") { transition("e2e.complete"); next("step2") }
                    activity("step2") { transition("e2e.complete") }
                }
                val wfId = runBlocking { engine.startWorkflow(definition) }.workflowId

                // Wait for WorkerLoop to drive the workflow to COMPLETED
                await().atMost(Duration.ofSeconds(30)).untilAsserted {
                    val wf = runBlocking { workflowRepo.findById(wfId) }
                    assertEquals(WorkflowStatus.COMPLETED, wf?.status)
                }
            } finally {
                job.cancel()
            }
        }
    }
}
```

Note: The `WorkerLoopConfig` interface may have additional methods not shown above (e.g. `appName()`). Check the actual interface at `src/main/kotlin/worker/config/WorkerLoopConfig.kt` and implement all methods in `testWorkerConfig`. If `appName()` is not in the interface, remove it.

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS — the loop claims the PENDING task at seq 1, executes handler, settles via PhaseGate which advances to seq 2, loop claims again, completes, workflow reaches COMPLETED.

- [ ] **Step 3: Commit**

```
test(worker): add WorkerLoop E2E integration test against real Oracle
```

---

### Task 2: Add handler-defers-task E2E test

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/execution/WorkerLoopIntegrationTest.kt`

- [ ] **Step 1: Add the Defer E2E test**

Add a new `@Nested` class in `WorkerLoopIntegrationTest`:

```kotlin
@Nested
inner class DeferPath {

    @Test
    fun `WorkerLoop handler defers task — task transitions to DEFERRED in Oracle`() {
        // Handler that returns Defer
        val deferHandler = object : TransitionHandler {
            override fun key(): String = "e2e.defer"
            override suspend fun execute(input: HandlerInput): HandlerResult =
                HandlerResult.Defer(triggerType = "test-trigger", triggerMeta = """{"key":"value"}""")
        }

        val (_, job) = buildWorkerLoop(deferHandler)
        try {
            val definition = workflow {
                activity("step1") { transition("e2e.defer") }
            }
            val wfId = runBlocking { engine.startWorkflow(definition) }.workflowId

            // Wait for task to become DEFERRED
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                val deferred = runBlocking { taskRepo.findDeferred() }
                assertEquals(1, deferred.size)
                assertEquals("test-trigger", deferred[0].triggerType)
                assertEquals("""{"key":"value"}""", deferred[0].triggerMeta)
                assertEquals(wfId, deferred[0].workflowId)
            }

            // Workflow should still be RUNNING (waiting for trigger to settle)
            val wf = runBlocking { workflowRepo.findById(wfId) }
            assertEquals(WorkflowStatus.RUNNING, wf?.status)
        } finally {
            job.cancel()
        }
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test(worker): add WorkerLoop Defer path E2E test
```

---

### Task 3: Add handler-failure-retry E2E test

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/execution/WorkerLoopIntegrationTest.kt`

- [ ] **Step 1: Add the failure + retry test**

Add a new `@Nested` class:

```kotlin
@Nested
inner class RetryPath {

    @Test
    fun `WorkerLoop retries failed task and succeeds on second attempt`() {
        val attempts = java.util.concurrent.atomic.AtomicInteger(0)
        val handler = object : TransitionHandler {
            override fun key(): String = "e2e.fail-once"
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val attempt = attempts.incrementAndGet()
                if (attempt == 1) throw RuntimeException("Simulated transient failure")
                return HandlerResult.Completed("""{"attempt":$attempt}""")
            }
        }

        val (_, job) = buildWorkerLoop(handler)
        try {
            val definition = workflow {
                activity("step1") { transition("e2e.fail-once"); retries(3) }
            }
            val wfId = runBlocking { engine.startWorkflow(definition) }.workflowId

            // Wait for workflow to complete (handler fails once, resets for retry, succeeds on second claim)
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                val wf = runBlocking { workflowRepo.findById(wfId) }
                assertEquals(WorkflowStatus.COMPLETED, wf?.status)
            }

            // Verify handler was called twice
            assertEquals(2, attempts.get())
        } finally {
            job.cancel()
        }
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopIntegrationTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test(worker): add WorkerLoop retry E2E test
```

---

### Task 4: Run full test suite

- [ ] **Step 1: Run all tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: PASS — no regressions.

- [ ] **Step 2: Commit any fixes if needed**
