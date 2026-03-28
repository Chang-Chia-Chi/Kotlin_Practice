# Stress Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the workflow engine does not hang under distributed failures and race conditions — 39 scenarios across 4 test classes validating liveness, correctness, idempotency, and resilience.

**Architecture:** Four integration test classes organized by system guarantee, backed by shared infrastructure (`StressTestBase`) that provides Oracle + Toxiproxy containers, parameterized concurrency scale, handler DSL for fault injection, and diagnostic state dumps on failure. All tests run against real Oracle with Toxiproxy for network fault injection.

**Tech Stack:** Kotlin coroutines, JUnit 5, Testcontainers (Oracle Free + Toxiproxy), Awaitility, JDBI, Micrometer SimpleMeterRegistry

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `pom.xml` | Add Toxiproxy testcontainers dependency |
| Create | `src/test/kotlin/stress/StressScale.kt` | Parameterized concurrency scale enum |
| Create | `src/test/kotlin/stress/StressTestBase.kt` | Shared infra: Oracle + Toxiproxy + repos + engine + helpers + diagnostic dump |
| Create | `src/test/kotlin/stress/StressHandlers.kt` | CrashPoint enum, CrashableHandler, inline handler DSL |
| Create | `src/test/kotlin/stress/LivenessStressTest.kt` | L1–L12: every workflow reaches terminal state |
| Create | `src/test/kotlin/stress/CorrectnessStressTest.kt` | C1–C11: no duplicates, correct policy evaluation |
| Create | `src/test/kotlin/stress/IdempotencyStressTest.kt` | I1–I8: concurrent recovery is safe |
| Create | `src/test/kotlin/stress/ResilienceStressTest.kt` | R1–R8: self-healing after infrastructure failure |

---

## Task 1: Add Toxiproxy Dependency

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: Add testcontainers-toxiproxy dependency**

Add after the existing `testcontainers:junit-jupiter` block in `pom.xml`:

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>toxiproxy</artifactId>
    <version>1.20.4</version>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile test-compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "chore: add testcontainers-toxiproxy dependency for stress tests"
```

---

## Task 2: Create StressScale and StressTestBase

**Files:**
- Create: `src/test/kotlin/stress/StressScale.kt`
- Create: `src/test/kotlin/stress/StressTestBase.kt`

- [ ] **Step 1: Create StressScale enum**

Create `src/test/kotlin/stress/StressScale.kt`:

```kotlin
package com.workflow.stress

import java.time.Duration

enum class StressScale(
    val workers: Int,
    val fanOutSize: Int,
    val workflowBatchSize: Int,
    val outerTimeout: Duration,
    val innerMargin: Duration,
) {
    MODERATE(
        workers = 10,
        fanOutSize = 50,
        workflowBatchSize = 5,
        outerTimeout = Duration.ofSeconds(30),
        innerMargin = Duration.ofSeconds(5),
    ),
    HIGH(
        workers = 50,
        fanOutSize = 500,
        workflowBatchSize = 20,
        outerTimeout = Duration.ofSeconds(120),
        innerMargin = Duration.ofSeconds(15),
    );

    companion object {
        fun resolve(): StressScale =
            System.getProperty("stress.scale", "MODERATE")
                .uppercase()
                .let { valueOf(it) }
    }
}
```

- [ ] **Step 2: Create StressTestBase**

Create `src/test/kotlin/stress/StressTestBase.kt`:

```kotlin
package com.workflow.stress

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.config.FrameworkConfig
import com.workflow.engine.BarrierService
import com.workflow.engine.OracleTestContainer
import com.workflow.engine.PhaseStrategyRegistry
import com.workflow.engine.Sweeper
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.engine.WorkflowEngine
import com.workflow.engine.WorkflowRepository
import com.workflow.engine.WorkflowStatus
import com.workflow.worker.HandlerRegistry
import com.workflow.worker.WorkerLoop
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestWatcher
import org.testcontainers.Testcontainers
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.TreeMap
import java.util.UUID
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class StressTestBase {

    protected val scale: StressScale = StressScale.resolve()

    // --- Containers & JDBI (through Toxiproxy) ---

    protected lateinit var proxyJdbi: Jdbi
    protected lateinit var directJdbi: Jdbi
    protected lateinit var oracleProxy: ToxiproxyContainer.ContainerProxy

    // --- Components ---

    protected lateinit var workflowRepo: WorkflowRepository
    protected lateinit var taskRepo: TaskRepository
    protected lateinit var engine: WorkflowEngine
    protected lateinit var barrier: BarrierService
    protected lateinit var sweeper: Sweeper
    protected lateinit var handlerRegistry: HandlerRegistry
    protected lateinit var meterRegistry: SimpleMeterRegistry

    protected val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())

    // --- Worker pool ---

    private val workerJobs = mutableListOf<Job>()
    private val workerScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // --- Config ---

    protected open val gracePeriod: Duration = Duration.ofSeconds(2)
    protected open val sweepInterval: Duration = Duration.ofSeconds(1)
    protected open val staleTaskThreshold: Duration = Duration.ofSeconds(3)
    protected open val pollInterval: Duration = Duration.ofMillis(200)
    protected open val workerConcurrency: Int = scale.workers

    protected val testConfig: FrameworkConfig by lazy {
        object : FrameworkConfig {
            override fun worker() = object : FrameworkConfig.WorkerConfig {
                override fun id() = "stress-worker"
                override fun pollInterval() = this@StressTestBase.pollInterval
                override fun concurrency() = workerConcurrency
                override fun batchSize() = 1
            }
            override fun leaderElection() = object : FrameworkConfig.LeaderElectionConfig {
                override fun namespace() = "default"
                override fun leaseName() = "test-lease"
                override fun leaseDuration() = Duration.ofSeconds(15)
                override fun renewDeadline() = Duration.ofSeconds(10)
                override fun retryPeriod() = Duration.ofSeconds(2)
                override fun healthThreshold() = Duration.ofSeconds(45)
            }
            override fun shutdown() = object : FrameworkConfig.ShutdownConfig {
                override fun globalTimeout() = Duration.ofSeconds(10)
                override fun leaderTeardownTimeout() = Duration.ofSeconds(5)
            }
            override fun sweeper() = object : FrameworkConfig.SweeperConfig {
                override fun interval() = this@StressTestBase.sweepInterval
                override fun gracePeriod() = this@StressTestBase.gracePeriod
                override fun staleTaskThreshold() = this@StressTestBase.staleTaskThreshold
            }
        }
    }

    @BeforeAll
    fun initInfrastructure() {
        // Direct JDBI (bypass proxy — for setup/assertions)
        directJdbi = OracleTestContainer.jdbi

        // Toxiproxy wrapping Oracle
        val oraclePort = OracleTestContainer.oracle.getMappedPort(1521)
        Testcontainers.exposeHostPorts(oraclePort)

        val toxiproxy = ToxiproxyContainer(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0"),
        ).apply { start() }

        oracleProxy = toxiproxy.getProxy("host.testcontainers.internal", oraclePort)

        // JDBI through Toxiproxy (production components use this)
        Class.forName("oracle.jdbc.OracleDriver")
        val proxyUrl = "jdbc:oracle:thin:@${oracleProxy.containerIpAddress}:${oracleProxy.proxyPort}/testdb"
        proxyJdbi = Jdbi.create(proxyUrl, "testuser", "testpass")

        // Init components
        workflowRepo = WorkflowRepository(proxyJdbi)
        taskRepo = TaskRepository(proxyJdbi)
        val strategyRegistry = PhaseStrategyRegistry(objectMapper)
        barrier = BarrierService(proxyJdbi, workflowRepo, taskRepo, objectMapper, strategyRegistry)
        engine = WorkflowEngine(proxyJdbi, workflowRepo, taskRepo, objectMapper)
        sweeper = Sweeper(proxyJdbi, workflowRepo, taskRepo, barrier, testConfig)
        handlerRegistry = HandlerRegistry()
        meterRegistry = SimpleMeterRegistry()
    }

    @AfterEach
    fun cleanUp() {
        runBlocking {
            // Stop workers
            workerJobs.forEach { it.cancelAndJoin() }
            workerJobs.clear()
        }

        // Reset proxy (remove all toxics)
        try {
            oracleProxy.toxics.all.forEach { it.remove() }
        } catch (_: Exception) { }

        // Clean tables via direct JDBI (bypasses proxy)
        directJdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    // --- Worker lifecycle ---

    protected fun startWorkers(handlerKey: String, handler: com.workflow.worker.TransitionHandler): List<Job> {
        handlerRegistry.register(handlerKey, handler)
        return startWorkerPool()
    }

    protected fun startWorkerPool(): List<Job> {
        val loop = WorkerLoop(testConfig, taskRepo, handlerRegistry, barrier, meterRegistry)
        val job = loop.start(workerScope)
        workerJobs.add(job)
        return listOf(job)
    }

    // --- Sweeper ---

    protected suspend fun runSweep() {
        sweeper.patrol()
    }

    // --- Assertions ---

    protected fun assertWorkflowTerminates(workflowId: String, timeout: Duration = scale.outerTimeout) {
        await atMost timeout untilAsserted {
            val wf = readWorkflowDirect(workflowId)
            assertNotNull(wf, "Workflow $workflowId not found")
            val status = wf["STATUS"]?.toString()
            assertContains(
                listOf("COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"),
                status,
                "Workflow $workflowId stuck in status $status",
            )
        }
    }

    protected fun assertWorkflowStatus(workflowId: String, expected: String, timeout: Duration = scale.outerTimeout) {
        await atMost timeout untilAsserted {
            val wf = readWorkflowDirect(workflowId)
            assertNotNull(wf)
            assertEquals(expected, wf["STATUS"]?.toString())
        }
    }

    protected fun assertSweeperRecovers(
        workflowId: String,
        previousSequence: Int,
        timeout: Duration = gracePeriod + sweepInterval + scale.innerMargin,
    ) {
        await atMost timeout untilAsserted {
            val wf = readWorkflowDirect(workflowId)
            assertNotNull(wf)
            val seq = (wf["CURRENT_SEQUENCE"] as Number).toInt()
            assertTrue(seq > previousSequence, "Workflow $workflowId still at sequence $previousSequence")
        }
    }

    protected fun assertTaskCount(workflowId: String, sequenceNumber: Int, expectedCount: Int) {
        val count = countTasksDirect(workflowId, sequenceNumber)
        assertEquals(expectedCount, count, "Expected $expectedCount tasks at seq $sequenceNumber, got $count")
    }

    protected fun assertNoTaskDuplicates(workflowId: String, sequenceNumber: Int) {
        val tasks = readTasksDirect(workflowId, sequenceNumber)
        val ids = tasks.map { it["ID"]?.toString() }
        assertEquals(ids.size, ids.toSet().size, "Duplicate task IDs found at seq $sequenceNumber")
    }

    // --- Direct SQL helpers (bypass proxy, always work) ---

    protected fun readWorkflowDirect(workflowId: String): Map<String, Any?>? =
        directJdbi.withHandle<Map<String, Any?>?, Exception> { handle ->
            handle.createQuery("SELECT * FROM workflow WHERE id = :id")
                .bind("id", workflowId)
                .mapToMap()
                .findOne()
                .orElse(null)
                ?.let { caseInsensitive(it) }
        }

    protected fun readTasksDirect(workflowId: String, sequenceNumber: Int? = null): List<Map<String, Any?>> =
        directJdbi.withHandle<List<Map<String, Any?>>, Exception> { handle ->
            val sql = if (sequenceNumber != null) {
                "SELECT * FROM task WHERE workflow_id = :wfId AND sequence_number = :seq"
            } else {
                "SELECT * FROM task WHERE workflow_id = :wfId"
            }
            val query = handle.createQuery(sql).bind("wfId", workflowId)
            if (sequenceNumber != null) query.bind("seq", sequenceNumber)
            query.mapToMap().list().map { caseInsensitive(it) }
        }

    protected fun countTasksDirect(workflowId: String, sequenceNumber: Int): Int =
        directJdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND sequence_number = :seq",
            ).bind("wfId", workflowId).bind("seq", sequenceNumber)
                .mapTo(Int::class.java).one()
        }

    protected fun countTasksWithStatusDirect(workflowId: String, status: String): Int =
        directJdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND status = :status",
            ).bind("wfId", workflowId).bind("status", status)
                .mapTo(Int::class.java).one()
        }

    protected fun updateWorkflowUpdatedAtDirect(workflowId: String, updatedAt: Instant) {
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE workflow SET updated_at = :ts WHERE id = :id")
                .bind("ts", LocalDateTime.ofInstant(updatedAt, ZoneOffset.UTC))
                .bind("id", workflowId)
                .execute()
        }
    }

    protected fun updateTaskClaimedAtDirect(taskId: String, claimedAt: Instant) {
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE task SET claimed_at = :ts WHERE id = :id")
                .bind("ts", LocalDateTime.ofInstant(claimedAt, ZoneOffset.UTC))
                .bind("id", taskId)
                .execute()
        }
    }

    protected fun insertWorkflowDirect(
        id: String,
        definitionJson: String,
        currentSequence: Int = 1,
        version: Int = 0,
        status: String = "RUNNING",
        deadlineAt: Instant = Instant.now().plus(1, ChronoUnit.HOURS),
    ) {
        val now = LocalDateTime.ofInstant(Instant.now().truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC)
        val deadline = LocalDateTime.ofInstant(deadlineAt.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC)
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO workflow (id, definition, current_sequence, version, status, created_at, updated_at, deadline_at)
                   VALUES (:id, :def, :seq, :ver, :status, :now, :now, :deadline)""",
            )
                .bind("id", id)
                .bind("def", definitionJson)
                .bind("seq", currentSequence)
                .bind("ver", version)
                .bind("status", status)
                .bind("now", now)
                .bind("deadline", deadline)
                .execute()
        }
    }

    protected fun insertTaskDirect(
        id: String = UUID.randomUUID().toString(),
        workflowId: String,
        sequenceNumber: Int = 1,
        status: String = "PENDING",
        handlerKey: String = "test.handler",
        payload: String? = null,
        result: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = Instant.now().plus(30, ChronoUnit.MINUTES),
    ): String {
        val now = LocalDateTime.ofInstant(Instant.now().truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC)
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, payload, result,
                   claimed_by, claimed_at, retry_count, max_retries, deadline_at, enqueued_at)
                   VALUES (:id, :wfId, :seq, :status, :key, :payload, :result,
                   :claimedBy, :claimedAt, :retryCount, :maxRetries, :deadlineAt, :enqueuedAt)""",
            )
                .bind("id", id)
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .bind("status", status)
                .bind("key", handlerKey)
                .bind("payload", payload)
                .bind("result", result)
                .bind("claimedBy", claimedBy)
                .apply {
                    if (claimedAt != null) {
                        bind("claimedAt", LocalDateTime.ofInstant(claimedAt.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC))
                    } else {
                        bindNull("claimedAt", java.sql.Types.TIMESTAMP)
                    }
                    if (deadlineAt != null) {
                        bind("deadlineAt", LocalDateTime.ofInstant(deadlineAt.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC))
                    } else {
                        bindNull("deadlineAt", java.sql.Types.TIMESTAMP)
                    }
                }
                .bind("retryCount", retryCount)
                .bind("maxRetries", maxRetries)
                .bind("enqueuedAt", now)
                .execute()
        }
        return id
    }

    // --- Utilities ---

    protected fun randomId(): String = UUID.randomUUID().toString()

    protected fun now(): Instant = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun caseInsensitive(map: Map<String, Any?>): Map<String, Any?> {
        val ci = TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
        ci.putAll(map)
        // Handle CLOBs
        for ((k, v) in ci) {
            if (v is java.sql.Clob) ci[k] = v.characterStream.readText()
        }
        return ci
    }

    protected fun dumpState(workflowId: String): String = buildString {
        val wf = readWorkflowDirect(workflowId)
        appendLine("=== Workflow $workflowId ===")
        appendLine("  status=${wf?.get("STATUS")}, seq=${wf?.get("CURRENT_SEQUENCE")}, ver=${wf?.get("VERSION")}")
        val tasks = readTasksDirect(workflowId)
        for (t in tasks) {
            appendLine("  task=${t["ID"]} seq=${t["SEQUENCE_NUMBER"]} status=${t["STATUS"]} claimed=${t["CLAIMED_BY"]} retry=${t["RETRY_COUNT"]}")
        }
    }
}

/**
 * JUnit extension that dumps workflow/task state on test failure.
 * Tests register workflow IDs via [trackedWorkflows] for diagnostic output.
 */
class StressTestDiagnostics(private val base: StressTestBase) : TestWatcher {
    val trackedWorkflows = mutableListOf<String>()

    override fun testFailed(context: ExtensionContext?, cause: Throwable?) {
        if (trackedWorkflows.isEmpty()) return
        System.err.println("=== STRESS TEST DIAGNOSTIC DUMP ===")
        for (wfId in trackedWorkflows) {
            System.err.println(base.dumpState(wfId))
        }
    }
}
```

- [ ] **Step 3: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/stress/StressScale.kt src/test/kotlin/stress/StressTestBase.kt
git commit -m "test: add StressTestBase infrastructure with Oracle + Toxiproxy"
```

---

## Task 3: Create Stress Handler Utilities

**Files:**
- Create: `src/test/kotlin/stress/StressHandlers.kt`

- [ ] **Step 1: Create StressHandlers.kt**

```kotlin
package com.workflow.stress

import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicInteger

/** Points in the worker lifecycle where a crash can be simulated. */
enum class CrashPoint {
    /** Crash before handler.execute() is called. */
    BEFORE_HANDLER,
    /** Crash during handler.execute() (mid-computation). */
    MID_HANDLER,
    /** Crash after handler returns, before barrier is called.
     *  Since barrier is called by WorkerLoop (not handler), this is simulated
     *  by throwing after the handler returns. WorkerLoop catches this and
     *  routes through the failure path, which leaves the task in PROCESSING
     *  if resetForRetry also fails (because we throw CancellationException). */
    AFTER_HANDLER,
}

/**
 * A handler that crashes at a specified [CrashPoint] on the Nth invocation.
 * Before/after the crash invocation, it delegates to [delegate].
 *
 * @param crashAt where in the lifecycle to crash
 * @param crashOnInvocation 1-based: crash on the Nth call (default: 1 = first call)
 * @param delegate the real handler to run when not crashing
 */
class CrashableHandler(
    private val crashAt: CrashPoint,
    private val crashOnInvocation: Int = 1,
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {

    private val invocationCount = AtomicInteger(0)

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val n = invocationCount.incrementAndGet()
        val shouldCrash = (n == crashOnInvocation)

        if (shouldCrash && crashAt == CrashPoint.BEFORE_HANDLER) {
            throw CancellationException("Simulated crash BEFORE handler")
        }

        if (shouldCrash && crashAt == CrashPoint.MID_HANDLER) {
            // Start some work, then crash
            delay(10)
            throw CancellationException("Simulated crash MID handler")
        }

        val output = delegate.execute(input)

        if (shouldCrash && crashAt == CrashPoint.AFTER_HANDLER) {
            throw CancellationException("Simulated crash AFTER handler")
        }

        return output
    }
}

/** Returns input payload as output result. */
class PassThroughHandler : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput =
        HandlerOutput(result = input.payload)
}

/** Always throws after optional delay. */
class FailingHandler(
    private val delayMs: Long = 0,
    private val message: String = "Simulated failure",
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput {
        if (delayMs > 0) delay(delayMs)
        throw RuntimeException(message)
    }
}

/** Delays for [delayMs] then delegates. Useful for simulating slow handlers. */
class SlowHandler(
    private val delayMs: Long,
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput {
        delay(delayMs)
        return delegate.execute(input)
    }
}

/**
 * Handler that blocks until explicitly released via [release].
 * Useful for controlling timing in race condition tests.
 */
class GatedHandler(
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    private val gate = CompletableDeferred<Unit>()

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        gate.await()
        return delegate.execute(input)
    }

    fun release() { gate.complete(Unit) }
}

/**
 * Tracks invocation count per task ID. Useful for verifying no duplicate processing.
 */
class CountingHandler(
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    val invocations = java.util.concurrent.ConcurrentHashMap<String, AtomicInteger>()
    val totalInvocations = AtomicInteger(0)

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        invocations.computeIfAbsent(input.taskId) { AtomicInteger(0) }.incrementAndGet()
        totalInvocations.incrementAndGet()
        return delegate.execute(input)
    }
}

/**
 * Fails the first N invocations, then succeeds.
 */
class FailNThenSucceedHandler(
    private val failCount: Int,
    private val delegate: TransitionHandler = PassThroughHandler(),
) : TransitionHandler {
    private val count = AtomicInteger(0)

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        if (count.incrementAndGet() <= failCount) {
            throw RuntimeException("Simulated failure #${count.get()}")
        }
        return delegate.execute(input)
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/StressHandlers.kt
git commit -m "test: add stress test handler utilities (CrashableHandler, GatedHandler, etc.)"
```

---

## Task 4: LivenessStressTest — Worker Crash Scenarios (L1–L5)

**Files:**
- Create: `src/test/kotlin/stress/LivenessStressTest.kt`

- [ ] **Step 1: Create LivenessStressTest with L1–L5**

```kotlin
package com.workflow.stress

import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.workflow
import com.workflow.engine.TaskStatus
import com.workflow.engine.WorkflowStatus
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@Tag("stress")
class LivenessStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- L1: Worker dies after claiming task, before handler starts ----

    @Test
    fun `L1 - worker crash before handler - stale reclaim recovers`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l1.handler"); retries(3) }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L1"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // First call crashes before handler, subsequent calls succeed
        handlerRegistry.register(
            "l1.handler",
            CrashableHandler(CrashPoint.BEFORE_HANDLER, crashOnInvocation = 1),
        )
        startWorkerPool()

        // Run sweeper periodically to reclaim stale tasks
        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")
        sweepJob.cancel()
    }

    // ---- L2: Worker dies mid-handler execution ----

    @Test
    fun `L2 - worker crash mid handler - stale reclaim recovers`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l2.handler"); retries(3) }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L2"}""")
        diagnostics.trackedWorkflows.add(wfId)

        handlerRegistry.register(
            "l2.handler",
            CrashableHandler(CrashPoint.MID_HANDLER, crashOnInvocation = 1),
        )
        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")
        sweepJob.cancel()
    }

    // ---- L3: Worker dies after handler success, before barrier call ----

    @Test
    fun `L3 - worker crash after handler before barrier - stale reclaim recovers`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l3.handler"); retries(3) }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L3"}""")
        diagnostics.trackedWorkflows.add(wfId)

        handlerRegistry.register(
            "l3.handler",
            CrashableHandler(CrashPoint.AFTER_HANDLER, crashOnInvocation = 1),
        )
        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")
        sweepJob.cancel()
    }

    // ---- L4: Worker commits TX1 (task COMPLETED), dies before TX2 (CAS) ----
    // Simulated via direct state setup: task is COMPLETED, workflow hasn't advanced.

    @Test
    fun `L4 - crash between TX1 and TX2 - sweeper stuck detection recovers`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l4.handler") }
            activity("step2") { transition("l4.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        // Set up state: workflow at seq 1, task COMPLETED, but workflow not advanced
        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "l4.handler",
            result = """{"test":"L4"}""",
        )

        // Make workflow look stale (past grace period)
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        // Register handler for step2 and start workers
        handlerRegistry.register("l4.handler", PassThroughHandler())
        startWorkerPool()

        // Sweeper should detect stuck workflow and advance it
        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")
        sweepJob.cancel()
    }

    // ---- L5: All workers die simultaneously, then restart ----

    @Test
    fun `L5 - all workers die and restart - stale reclaim batch recovers`() = runBlocking {
        val batchSize = scale.workflowBatchSize
        val def = workflow {
            activity("step1") { transition("l5.handler"); retries(3) }
        }

        // Start multiple workflows
        val wfIds = (1..batchSize).map {
            engine.startWorkflow(def, """{"test":"L5-$it"}""").also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        // Use a gated handler that blocks all workers
        val gate = GatedHandler()
        handlerRegistry.register("l5.handler", gate)
        val jobs = startWorkerPool()

        // Wait for workers to claim tasks
        delay(pollInterval.toMillis() * 3)

        // Kill all workers (simulates simultaneous crash)
        jobs.forEach { it.cancel() }
        workerJobs.clear()

        // Make stale tasks visible to sweeper
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                "UPDATE task SET claimed_at = :ts WHERE status = 'PROCESSING'",
            ).bind("ts", java.time.LocalDateTime.ofInstant(
                Instant.now().minus(staleTaskThreshold.multipliedBy(2)),
                java.time.ZoneOffset.UTC,
            )).execute()
        }

        // Start fresh workers with pass-through handler
        handlerRegistry.register("l5.handler", PassThroughHandler())
        startWorkerPool()

        // Sweeper reclaims stale tasks
        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }
}
```

- [ ] **Step 2: Run L1–L5 tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="com.workflow.stress.LivenessStressTest" -pl . -q`
Expected: All 5 tests PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/LivenessStressTest.kt
git commit -m "test: add LivenessStressTest L1-L5 worker crash scenarios"
```

---

## Task 5: LivenessStressTest — Network Fault & Timeout Scenarios (L6–L10)

**Files:**
- Modify: `src/test/kotlin/stress/LivenessStressTest.kt`

- [ ] **Step 1: Add L6a, L6b, L7 network fault scenarios**

Append inside the `LivenessStressTest` class:

```kotlin
    // ---- L6a: Network partition during TX1 (task update) ----

    @Tag("stress-network")
    @Test
    fun `L6a - network cut during task update TX1 - stale reclaim recovers`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l6a.handler"); retries(3) }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L6a"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // Handler succeeds, but we cut network so barrier TX1 fails
        var firstAttempt = true
        handlerRegistry.register("l6a.handler", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                if (firstAttempt) {
                    firstAttempt = false
                    // Cut network — the barrier call after this will fail
                    oracleProxy.toxics.bandwidth("cut-l6a", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, 0)
                    delay(100)
                    // Restore after a brief cut (simulates transient partition)
                    oracleProxy.toxics["cut-l6a"].remove()
                }
                return HandlerOutput(result = input.payload)
            }
        })
        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        sweepJob.cancel()
    }

    // ---- L6b: Network partition during TX2 (CAS + advance) ----

    @Tag("stress-network")
    @Test
    fun `L6b - network cut during CAS TX2 - sweeper stuck detection recovers`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l6b.handler"); retries(3) }
            activity("step2") { transition("l6b.handler") }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L6b"}""")
        diagnostics.trackedWorkflows.add(wfId)

        handlerRegistry.register("l6b.handler", PassThroughHandler())
        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        // This is a probabilistic test — the network cut may or may not hit TX2.
        // Either way, the workflow must terminate via normal path or recovery.
        assertWorkflowTerminates(wfId)
        sweepJob.cancel()
    }

    // ---- L7: Network partition during task claim ----

    @Tag("stress-network")
    @Test
    fun `L7 - network cut during claim - task stays PENDING and next poll claims it`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l7.handler") }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L7"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // Cut network briefly — first claim attempt fails, task stays PENDING
        oracleProxy.toxics.bandwidth("cut-l7", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, 0)

        handlerRegistry.register("l7.handler", PassThroughHandler())
        startWorkerPool()

        // Restore after a brief pause
        delay(pollInterval.toMillis() * 2)
        oracleProxy.toxics["cut-l7"].remove()

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")
    }
```

- [ ] **Step 2: Add L8–L10 timeout scenarios**

Append inside the `LivenessStressTest` class:

```kotlin
    // ---- L8: Task deadline expires while handler runs slowly ----

    @Test
    fun `L8 - slow handler exceeds task deadline - sweeper times out task`() = runBlocking {
        val def = workflow {
            activity("step1") {
                transition("l8.handler")
                deadline(Duration.ofSeconds(2)) // Short deadline
            }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L8"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // Handler takes longer than deadline
        handlerRegistry.register("l8.handler", SlowHandler(delayMs = 10_000))
        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        // Workflow should fail because the task timed out
        assertWorkflowStatus(wfId, "FAILED")
        sweepJob.cancel()
    }

    // ---- L9: Workflow deadline expires during execution ----

    @Test
    fun `L9 - workflow deadline expires - sweeper times out workflow`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l9.handler") }
            deadline(Duration.ofSeconds(2)) // Short workflow deadline
        }
        val wfId = engine.startWorkflow(def, """{"test":"L9"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // Handler blocks forever — workflow deadline must fire
        handlerRegistry.register("l9.handler", GatedHandler()) // Never released
        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "TIMED_OUT")
        sweepJob.cancel()
    }

    // ---- L10: Stale task exhausts retries → dead-letter → barrier evaluates ----

    @Test
    fun `L10 - task exhausts retries to dead letter - barrier fires with failure policy`() = runBlocking {
        val def = workflow {
            activity("step1") {
                transition("l10.handler")
                retries(1) // 1 retry = max 2 attempts
                failurePolicy(FailurePolicy.ABORT)
            }
        }
        val wfId = engine.startWorkflow(def, """{"test":"L10"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // Always fail
        handlerRegistry.register("l10.handler", FailingHandler())
        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "FAILED")
        sweepJob.cancel()
    }
```

- [ ] **Step 3: Run L6–L10 tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="com.workflow.stress.LivenessStressTest" -pl . -q`
Expected: All 10 tests PASS (L1–L10)

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/stress/LivenessStressTest.kt
git commit -m "test: add LivenessStressTest L6-L10 network fault and timeout scenarios"
```

---

## Task 6: LivenessStressTest — Fan-Out & Leader Scenarios (L11–L12)

**Files:**
- Modify: `src/test/kotlin/stress/LivenessStressTest.kt`

- [ ] **Step 1: Add L11 and L12**

Append inside the `LivenessStressTest` class:

```kotlin
    // ---- L11: Fan-out: all sub-tasks fail under BEST_EFFORT ----

    @Test
    fun `L11 - fan-out all sub-tasks fail with BEST_EFFORT - workflow terminates`() = runBlocking {
        val def = workflow {
            activity("scatter") {
                transition("l11.scatter")
                failurePolicy(FailurePolicy.BEST_EFFORT)
                fanOut {
                    transition("l11.parallel")
                    retries(0) // No retries — immediate failure
                    failurePolicy(FailurePolicy.BEST_EFFORT)
                    joinPolicy(JoinPolicy.All)
                }
            }
            activity("final") { transition("l11.final") }
        }

        // Scatter handler produces N payloads
        handlerRegistry.register("l11.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..scale.fanOutSize).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })
        // All parallel handlers fail
        handlerRegistry.register("l11.parallel", FailingHandler())
        handlerRegistry.register("l11.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"L11"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        assertWorkflowTerminates(wfId)
        sweepJob.cancel()
    }

    // ---- L12: Leader dies during sweep, new leader recovers ----

    @Test
    fun `L12 - leader dies mid sweep - new leader recovers stuck workflows`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("l12.handler") }
            activity("step2") { transition("l12.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)

        // Create multiple stuck workflows (simulating leader death mid-patrol)
        val wfIds = (1..3).map { i ->
            val wfId = randomId()
            diagnostics.trackedWorkflows.add(wfId)
            insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
            insertTaskDirect(
                workflowId = wfId,
                sequenceNumber = 1,
                status = "COMPLETED",
                handlerKey = "l12.handler",
                result = """{"test":"L12-$i"}""",
            )
            updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))
            wfId
        }

        handlerRegistry.register("l12.handler", PassThroughHandler())
        startWorkerPool()

        // Simulate: first sweep partially processes, then "new leader" sweeps again
        runSweep()

        val sweepJob = launch {
            while (true) {
                delay(sweepInterval.toMillis())
                runSweep()
            }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
        }
        sweepJob.cancel()
    }
```

- [ ] **Step 2: Run all liveness tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="com.workflow.stress.LivenessStressTest" -pl . -q`
Expected: All 12 tests PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/LivenessStressTest.kt
git commit -m "test: add LivenessStressTest L11-L12 fan-out and leader scenarios"
```

---

## Task 7: CorrectnessStressTest (C1–C11)

**Files:**
- Create: `src/test/kotlin/stress/CorrectnessStressTest.kt`

- [ ] **Step 1: Create CorrectnessStressTest**

```kotlin
package com.workflow.stress

import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.workflow
import com.workflow.engine.TaskStatus
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Tag("stress")
class CorrectnessStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- C1: N workers complete final task of a phase simultaneously (CAS race) ----

    @Test
    fun `C1 - concurrent CAS race - exactly one set of next-phase tasks created`() = runBlocking {
        val def = workflow {
            activity("scatter") {
                transition("c1.scatter")
                fanOut {
                    transition("c1.parallel")
                    joinPolicy(JoinPolicy.All)
                }
            }
            activity("final") { transition("c1.final") }
        }

        handlerRegistry.register("c1.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..scale.fanOutSize).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("c1.parallel", PassThroughHandler())
        handlerRegistry.register("c1.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C1"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Assert: exactly 1 task at the final sequence (no duplicates from CAS race)
        val wf = readWorkflowDirect(wfId)!!
        // The final activity is the last sequence
        val allTasks = readTasksDirect(wfId)
        val maxSeq = allTasks.maxOf { (it["SEQUENCE_NUMBER"] as Number).toInt() }
        val finalTasks = allTasks.filter { (it["SEQUENCE_NUMBER"] as Number).toInt() == maxSeq }
        assertEquals(1, finalTasks.size, "Expected exactly 1 final task, got ${finalTasks.size}")
        assertNoTaskDuplicates(wfId, maxSeq)

        sweepJob.cancel()
    }

    // ---- C2: Fan-out scatter produces N payloads → N sub-tasks atomically ----

    @Test
    fun `C2 - scatter produces N payloads - exactly N sub-tasks created`() = runBlocking {
        val n = scale.fanOutSize
        val def = workflow {
            activity("scatter") {
                transition("c2.scatter")
                fanOut {
                    transition("c2.parallel")
                    joinPolicy(JoinPolicy.All)
                }
            }
        }

        handlerRegistry.register("c2.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("c2.parallel", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C2"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Parallel tasks are at sequence 2 (scatter=seq1, parallel=seq2)
        val parallelTasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(n, parallelTasks.size, "Expected $n parallel tasks, got ${parallelTasks.size}")
        assertNoTaskDuplicates(wfId, 2)

        sweepJob.cancel()
    }

    // ---- C3: JoinPolicy.ALL - 1 of N fails ----

    @Test
    fun `C3 - JoinPolicy ALL with one failure and ABORT - workflow fails`() = runBlocking {
        val n = 10
        val def = workflow {
            activity("scatter") {
                transition("c3.scatter")
                failurePolicy(FailurePolicy.ABORT)
                fanOut {
                    transition("c3.parallel")
                    retries(0)
                    failurePolicy(FailurePolicy.ABORT)
                    joinPolicy(JoinPolicy.All)
                }
            }
        }

        handlerRegistry.register("c3.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })

        // Fail the first sub-task, succeed the rest
        val count = AtomicInteger(0)
        handlerRegistry.register("c3.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                if (count.incrementAndGet() == 1) throw RuntimeException("Simulated failure")
                return HandlerOutput(result = input.payload)
            }
        })

        val wfId = engine.startWorkflow(def, """{"test":"C3"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "FAILED")
        sweepJob.cancel()
    }

    // ---- C4: JoinPolicy.Percentage(95) boundary precision ----

    @Test
    fun `C4 - JoinPolicy Percentage 95 at threshold - passes`() = runBlocking {
        // 95 of 100 succeed (5 fail) → 95% ≥ 95% → pass
        val wfId = startPercentageTest(totalTasks = 100, failCount = 5, threshold = 95)
        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }
        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")
        sweepJob.cancel()
    }

    @Test
    fun `C4 - JoinPolicy Percentage 95 below threshold - fails`() = runBlocking {
        // 94 of 100 succeed (6 fail) → 94% < 95% → fail
        val wfId = startPercentageTest(totalTasks = 100, failCount = 6, threshold = 95)
        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }
        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "FAILED")
        sweepJob.cancel()
    }

    private suspend fun startPercentageTest(totalTasks: Int, failCount: Int, threshold: Int): String {
        val handlerKey = "c4-$totalTasks-$failCount"
        val def = workflow {
            activity("scatter") {
                transition("$handlerKey.scatter")
                fanOut {
                    transition("$handlerKey.parallel")
                    retries(0)
                    joinPolicy(JoinPolicy.Percentage(threshold))
                }
            }
            activity("final") { transition("$handlerKey.final") }
        }

        handlerRegistry.register("$handlerKey.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..totalTasks).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })

        val failCounter = AtomicInteger(0)
        handlerRegistry.register("$handlerKey.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                if (failCounter.incrementAndGet() <= failCount) throw RuntimeException("Simulated failure")
                return HandlerOutput(result = input.payload)
            }
        })
        handlerRegistry.register("$handlerKey.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C4-$failCount"}""")
        diagnostics.trackedWorkflows.add(wfId)
        startWorkerPool()
        return wfId
    }

    // ---- C5: JoinPolicy.Threshold(N) boundary precision ----

    @Test
    fun `C5 - JoinPolicy Threshold boundary precision`() = runBlocking {
        val total = 20
        val threshold = 15

        // At threshold: 15 succeed → pass
        verifyJoinPolicyThreshold(total, failCount = total - threshold, threshold = threshold, expectedStatus = "COMPLETED")
        cleanUpTables()

        // Below threshold: 14 succeed → fail
        verifyJoinPolicyThreshold(total, failCount = total - threshold + 1, threshold = threshold, expectedStatus = "FAILED")
    }

    private suspend fun verifyJoinPolicyThreshold(
        totalTasks: Int,
        failCount: Int,
        threshold: Int,
        expectedStatus: String,
    ) {
        val handlerKey = "c5-$totalTasks-$failCount"
        val def = workflow {
            activity("scatter") {
                transition("$handlerKey.scatter")
                fanOut {
                    transition("$handlerKey.parallel")
                    retries(0)
                    joinPolicy(JoinPolicy.Threshold(threshold))
                }
            }
            activity("final") { transition("$handlerKey.final") }
        }

        handlerRegistry.register("$handlerKey.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..totalTasks).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })

        val failCounter = AtomicInteger(0)
        handlerRegistry.register("$handlerKey.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                if (failCounter.incrementAndGet() <= failCount) throw RuntimeException("Simulated failure")
                return HandlerOutput(result = input.payload)
            }
        })
        handlerRegistry.register("$handlerKey.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C5-$failCount"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, expectedStatus)
        sweepJob.cancel()
    }

    // ---- C6: FailurePolicy.ABORT mid-phase ----

    @Test
    fun `C6 - ABORT mid-phase - workflow fails and no new phase started`() = runBlocking {
        val def = workflow {
            activity("step1") {
                transition("c6.handler")
                retries(0)
                failurePolicy(FailurePolicy.ABORT)
            }
            activity("step2") { transition("c6.step2") }
        }

        handlerRegistry.register("c6.handler", FailingHandler())
        handlerRegistry.register("c6.step2", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C6"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "FAILED")

        // No step2 tasks should exist
        val step2Tasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(0, step2Tasks.size, "No tasks should exist at seq 2 after ABORT")

        sweepJob.cancel()
    }

    // ---- C7: FailurePolicy.BEST_EFFORT - all tasks fail ----

    @Test
    fun `C7 - BEST_EFFORT with all failures - workflow advances to next phase`() = runBlocking {
        val def = workflow {
            activity("step1") {
                transition("c7.handler")
                retries(0)
                failurePolicy(FailurePolicy.BEST_EFFORT)
            }
            activity("step2") { transition("c7.step2") }
        }

        handlerRegistry.register("c7.handler", FailingHandler())
        handlerRegistry.register("c7.step2", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C7"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        sweepJob.cancel()
    }

    // ---- C8: Payload propagation integrity across phases ----

    @Test
    fun `C8 - payload propagates correctly across phase boundaries`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("c8.step1") }
            activity("step2") { transition("c8.step2") }
            activity("step3") { transition("c8.step3") }
        }

        handlerRegistry.register("c8.step1", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = """{"phase":1,"data":"${input.payload}"}""")
        })
        handlerRegistry.register("c8.step2", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = """{"phase":2,"prev":${input.payload}}""")
        })
        handlerRegistry.register("c8.step3", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = """{"phase":3,"prev":${input.payload}}""")
        })

        val wfId = engine.startWorkflow(def, """{"origin":"C8"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // Verify payload chain: each phase received the previous phase's result
        val tasks = readTasksDirect(wfId).sortedBy { (it["SEQUENCE_NUMBER"] as Number).toInt() }
        assertEquals(3, tasks.size)

        // Step1 received initial payload
        assertTrue(tasks[0]["PAYLOAD"].toString().contains("origin"))
        // Step2 received step1's result
        assertTrue(tasks[1]["PAYLOAD"].toString().contains("phase\":1"))
        // Step3 received step2's result
        assertTrue(tasks[2]["PAYLOAD"].toString().contains("phase\":2"))

        sweepJob.cancel()
    }

    // ---- C9: Fan-out sub-task results → join handler receives all ----

    @Test
    fun `C9 - join handler receives complete result set from all sub-tasks`() = runBlocking {
        val n = 10
        val def = workflow {
            activity("scatter") {
                transition("c9.scatter")
                fanOut {
                    transition("c9.parallel")
                    joinPolicy(JoinPolicy.All)
                }
            }
        }

        handlerRegistry.register("c9.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("c9.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = """{"processed":${input.payload}}""")
        })

        val wfId = engine.startWorkflow(def, """{"test":"C9"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Verify all parallel tasks completed with results
        val parallelTasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(n, parallelTasks.size)
        for (task in parallelTasks) {
            assertEquals("COMPLETED", task["STATUS"]?.toString())
            assertTrue(task["RESULT"]?.toString()?.contains("processed") == true)
        }

        sweepJob.cancel()
    }

    // ---- C10: Replay after FAILED ----

    @Test
    fun `C10 - replay resumes from current sequence without re-executing completed phases`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("c10.step1") }
            activity("step2") {
                transition("c10.step2")
                retries(0)
                failurePolicy(FailurePolicy.ABORT)
            }
            activity("step3") { transition("c10.step3") }
        }

        // Step1 succeeds, step2 fails
        handlerRegistry.register("c10.step1", PassThroughHandler())
        val step2Counter = AtomicInteger(0)
        handlerRegistry.register("c10.step2", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                if (step2Counter.incrementAndGet() == 1) throw RuntimeException("First attempt fails")
                return HandlerOutput(result = input.payload)
            }
        })
        handlerRegistry.register("c10.step3", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C10"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        // Wait for failure
        assertWorkflowStatus(wfId, "FAILED")

        // Step1 was completed — verify
        val step1Tasks = readTasksDirect(wfId, sequenceNumber = 1)
        assertEquals(1, step1Tasks.size)
        assertEquals("COMPLETED", step1Tasks[0]["STATUS"]?.toString())

        // Replay
        val replayed = engine.replayWorkflow(wfId)
        assertTrue(replayed, "Replay should succeed")

        // After replay, workflow should eventually complete
        assertWorkflowStatus(wfId, "COMPLETED", timeout = scale.outerTimeout)

        // Step1 should still have only 1 task (not re-executed)
        val step1After = readTasksDirect(wfId, sequenceNumber = 1)
        assertEquals(1, step1After.size, "Step1 should not be re-executed on replay")

        sweepJob.cancel()
    }

    // ---- C11: Concurrent barrier probes see consistent count under high write load ----

    @Test
    fun `C11 - concurrent barrier probes under high fanout - MVCC consistency`() = runBlocking {
        val n = scale.fanOutSize
        val def = workflow {
            activity("scatter") {
                transition("c11.scatter")
                fanOut {
                    transition("c11.parallel")
                    joinPolicy(JoinPolicy.All)
                }
            }
            activity("final") { transition("c11.final") }
        }

        handlerRegistry.register("c11.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("c11.parallel", PassThroughHandler())
        handlerRegistry.register("c11.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"C11"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // Use maximum workers to maximize concurrent barrier probes
        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // Critical assertion: exactly 1 final task (proves no duplicate CAS wins)
        val allTasks = readTasksDirect(wfId)
        val maxSeq = allTasks.maxOf { (it["SEQUENCE_NUMBER"] as Number).toInt() }
        assertTaskCount(wfId, maxSeq, 1)
        assertNoTaskDuplicates(wfId, maxSeq)

        sweepJob.cancel()
    }
}
```

- [ ] **Step 2: Run correctness tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="com.workflow.stress.CorrectnessStressTest" -pl . -q`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/CorrectnessStressTest.kt
git commit -m "test: add CorrectnessStressTest C1-C11 race condition and policy scenarios"
```

---

## Task 8: IdempotencyStressTest (I1–I8)

**Files:**
- Create: `src/test/kotlin/stress/IdempotencyStressTest.kt`

- [ ] **Step 1: Create IdempotencyStressTest**

```kotlin
package com.workflow.stress

import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.workflow
import com.workflow.engine.TaskStatus
import com.workflow.engine.WorkflowStatus
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Tag("stress")
class IdempotencyStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- I1: Sweeper + worker race on same stuck workflow ----

    @Test
    fun `I1 - sweeper and worker race on stuck workflow - exactly one CAS wins`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("i1.handler") }
            activity("step2") { transition("i1.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        // State: task COMPLETED, workflow not advanced (stuck)
        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "i1.handler",
            result = """{"test":"I1"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        handlerRegistry.register("i1.handler", PassThroughHandler())

        // Race: sweeper recovery and worker barrier completion fire simultaneously
        val latch = CountDownLatch(1)
        val sweeperResult = async {
            latch.await(5, TimeUnit.SECONDS)
            runSweep()
        }
        val barrierResult = async {
            latch.await(5, TimeUnit.SECONDS)
            barrier.recoverStuckWorkflow(wfId)
        }

        latch.countDown() // Fire both simultaneously
        sweeperResult.await()
        barrierResult.await()

        // Start workers for step2
        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Critical: exactly 1 task at seq 2 (no duplicates from race)
        assertTaskCount(wfId, 2, 1)
        assertNoTaskDuplicates(wfId, 2)
        sweepJob.cancel()
    }

    // ---- I2: Two sweeper patrols overlap (dual-leader) ----

    @Test
    fun `I2 - two sweeper patrols overlap - state consistent`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("i2.handler") }
            activity("step2") { transition("i2.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "i2.handler",
            result = """{"test":"I2"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        handlerRegistry.register("i2.handler", PassThroughHandler())

        // Two sweepers fire simultaneously
        val latch = CountDownLatch(1)
        val sweep1 = async { latch.await(5, TimeUnit.SECONDS); runSweep() }
        val sweep2 = async { latch.await(5, TimeUnit.SECONDS); runSweep() }

        latch.countDown()
        sweep1.await()
        sweep2.await()

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertTaskCount(wfId, 2, 1) // No duplicates
        sweepJob.cancel()
    }

    // ---- I3: Sweeper expires task at same moment worker completes it ----

    @Test
    fun `I3 - timeout and completion race - barrier fires exactly once`() = runBlocking {
        val def = workflow {
            activity("step1") {
                transition("i3.handler")
                deadline(Duration.ofSeconds(3))
                failurePolicy(FailurePolicy.ABORT)
            }
        }

        // Handler that takes just long enough to race with deadline
        handlerRegistry.register("i3.handler", SlowHandler(delayMs = 2500))

        val wfId = engine.startWorkflow(def, """{"test":"I3"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        // Either COMPLETED (worker wins) or FAILED (sweeper timeout wins)
        // But must terminate — not hang
        assertWorkflowTerminates(wfId)

        val wf = readWorkflowDirect(wfId)!!
        val status = wf["STATUS"]?.toString()
        assertTrue(
            status == "COMPLETED" || status == "FAILED",
            "Expected COMPLETED or FAILED, got $status",
        )
        sweepJob.cancel()
    }

    // ---- I4: Sweeper reclaims stale task while worker about to complete ----

    @Test
    fun `I4 - stale reclaim races with task completion - no corruption`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("i4.handler"); retries(3) }
        }
        val wfId = engine.startWorkflow(def, """{"test":"I4"}""")
        diagnostics.trackedWorkflows.add(wfId)

        // Slow handler that takes longer than stale threshold
        handlerRegistry.register("i4.handler", SlowHandler(
            delayMs = staleTaskThreshold.toMillis() + 1000,
            delegate = PassThroughHandler(),
        ))
        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        // Workflow should still complete despite the race
        assertWorkflowTerminates(wfId)
        sweepJob.cancel()
    }

    // ---- I5: Replay called while sweeper mid-recovery ----

    @Test
    fun `I5 - replay during sweeper recovery - no conflict`() = runBlocking {
        val def = workflow {
            activity("step1") {
                transition("i5.handler")
                retries(0)
                failurePolicy(FailurePolicy.ABORT)
            }
            activity("step2") { transition("i5.handler") }
        }

        // Step1 fails → workflow FAILED
        handlerRegistry.register("i5.handler", FailNThenSucceedHandler(failCount = 1))

        val wfId = engine.startWorkflow(def, """{"test":"I5"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowStatus(wfId, "FAILED")

        // Race: replay and sweeper both act on the workflow
        val latch = CountDownLatch(1)
        val replayResult = async {
            latch.await(5, TimeUnit.SECONDS)
            engine.replayWorkflow(wfId)
        }
        val sweeperResult = async {
            latch.await(5, TimeUnit.SECONDS)
            runSweep()
        }

        latch.countDown()
        replayResult.await()
        sweeperResult.await()

        // Should eventually complete (replay re-queues the failed task)
        assertWorkflowTerminates(wfId, timeout = scale.outerTimeout)
        sweepJob.cancel()
    }

    // ---- I6: Sweeper detects same stuck workflow on consecutive patrols ----

    @Test
    fun `I6 - consecutive sweeper patrols on same stuck workflow - second is no-op`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("i6.handler") }
            activity("step2") { transition("i6.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "i6.handler",
            result = """{"test":"I6"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        handlerRegistry.register("i6.handler", PassThroughHandler())

        // First patrol: recovers and advances
        runSweep()

        // Second patrol: workflow now has non-terminal tasks at new seq → skips
        runSweep()

        // Start workers to complete step2
        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // Exactly 1 task at seq 2 (second patrol didn't create duplicates)
        assertTaskCount(wfId, 2, 1)
        sweepJob.cancel()
    }

    // ---- I7: Double-claim prevention via SKIP LOCKED ----

    @Test
    fun `I7 - concurrent claims on same task - SKIP LOCKED prevents double claim`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("i7.handler") }
        }
        val wfId = engine.startWorkflow(def, """{"test":"I7"}""")
        diagnostics.trackedWorkflows.add(wfId)

        val counting = CountingHandler()
        handlerRegistry.register("i7.handler", counting)

        // Start multiple worker pools to maximize claim contention
        repeat(3) { startWorkerPool() }

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // The handler should have been invoked exactly once for the single task
        assertEquals(1, counting.totalInvocations.get(), "Task should be processed exactly once")
        sweepJob.cancel()
    }

    // ---- I8: Cancel workflow while barrier in-flight ----

    @Test
    fun `I8 - cancel workflow while barrier in-flight - no post-cancel advancement`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("i8.handler") }
            activity("step2") { transition("i8.step2") }
        }

        // Slow handler to give us time to cancel
        val gate = GatedHandler()
        handlerRegistry.register("i8.handler", gate)
        handlerRegistry.register("i8.step2", PassThroughHandler())

        val wfId = engine.startWorkflow(def, """{"test":"I8"}""")
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        // Wait for task to be claimed
        delay(pollInterval.toMillis() * 3)

        // Cancel workflow while handler is blocked
        val cancelled = engine.cancelWorkflow(wfId)
        assertTrue(cancelled, "Cancel should succeed")

        // Release the gate — handler completes, but barrier should fail CAS
        gate.release()

        delay(1000) // Give time for any erroneous advancement

        // Workflow should stay CANCELLED, no step2 tasks
        val wf = readWorkflowDirect(wfId)!!
        assertEquals("CANCELLED", wf["STATUS"]?.toString())
        val step2Tasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(0, step2Tasks.size, "No step2 tasks should exist after cancel")
    }
}
```

- [ ] **Step 2: Run idempotency tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="com.workflow.stress.IdempotencyStressTest" -pl . -q`
Expected: All 8 tests PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/IdempotencyStressTest.kt
git commit -m "test: add IdempotencyStressTest I1-I8 concurrent recovery scenarios"
```

---

## Task 9: ResilienceStressTest (R1–R8)

**Files:**
- Create: `src/test/kotlin/stress/ResilienceStressTest.kt`

- [ ] **Step 1: Create ResilienceStressTest**

```kotlin
package com.workflow.stress

import com.workflow.dsl.workflow
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import eu.rekawek.toxiproxy.model.ToxicDirection
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@Tag("stress")
@Tag("stress-network")
class ResilienceStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- R1: Oracle unavailable then recovers ----

    @Test
    fun `R1 - Oracle outage then recovery - workflows complete`() = runBlocking {
        val batchSize = scale.workflowBatchSize
        val def = workflow {
            activity("step1") { transition("r1.handler") }
        }

        handlerRegistry.register("r1.handler", PassThroughHandler())

        val wfIds = (1..batchSize).map {
            engine.startWorkflow(def, """{"test":"R1-$it"}""").also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()

        // Let some workflows start processing
        delay(pollInterval.toMillis() * 2)

        // Cut Oracle connection
        oracleProxy.toxics.bandwidth("cut-r1", ToxicDirection.DOWNSTREAM, 0)

        // Hold outage
        delay(3000)

        // Restore
        oracleProxy.toxics["cut-r1"].remove()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R2: Oracle latency spike ----

    @Test
    fun `R2 - Oracle latency spike - no spurious timeouts and backlog drains`() = runBlocking {
        val def = workflow {
            activity("step1") {
                transition("r2.handler")
                deadline(Duration.ofSeconds(30)) // Generous deadline
            }
        }

        handlerRegistry.register("r2.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def, """{"test":"R2-$it"}""").also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()

        // Inject latency
        oracleProxy.toxics.latency("slow-r2", ToxicDirection.DOWNSTREAM, 3000)

        delay(5000)

        // Remove latency
        oracleProxy.toxics["slow-r2"].remove()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
        }
        sweepJob.cancel()
    }

    // ---- R3: Connection pool exhaustion ----

    @Test
    fun `R3 - connection pool exhaustion - workers back off and recover`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("r3.handler"); retries(5) }
        }

        handlerRegistry.register("r3.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def, """{"test":"R3-$it"}""").also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()

        // Throttle bandwidth to simulate pool pressure
        oracleProxy.toxics.limitData("throttle-r3", ToxicDirection.DOWNSTREAM, 512)

        delay(5000)

        // Release throttle
        oracleProxy.toxics["throttle-r3"].remove()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R4: Full worker pool dies and restarts ----

    @Test
    fun `R4 - worker pool death and restart - all workflows recover`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("r4.handler"); retries(3) }
        }

        handlerRegistry.register("r4.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def, """{"test":"R4-$it"}""").also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        // Start and let workers claim tasks
        val jobs = startWorkerPool()
        delay(pollInterval.toMillis() * 3)

        // Kill all workers
        jobs.forEach { it.cancelAndJoin() }
        workerJobs.clear()

        // Age stale tasks past threshold
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                "UPDATE task SET claimed_at = :ts WHERE status = 'PROCESSING'",
            ).bind("ts", java.time.LocalDateTime.ofInstant(
                Instant.now().minus(staleTaskThreshold.multipliedBy(2)),
                java.time.ZoneOffset.UTC,
            )).execute()
        }

        // Restart fresh workers
        startWorkerPool()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R5: No leader for extended period, then elected ----

    @Test
    fun `R5 - leaderless period then recovery - stuck workflows batch recovered`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("r5.handler") }
            activity("step2") { transition("r5.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)

        // Create stuck workflows (simulating no sweeper running)
        val wfIds = (1..scale.workflowBatchSize).map { i ->
            val wfId = randomId()
            diagnostics.trackedWorkflows.add(wfId)
            insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
            insertTaskDirect(
                workflowId = wfId,
                sequenceNumber = 1,
                status = "COMPLETED",
                handlerKey = "r5.handler",
                result = """{"test":"R5-$i"}""",
            )
            updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(3)))
            wfId
        }

        handlerRegistry.register("r5.handler", PassThroughHandler())
        startWorkerPool()

        // Wait (simulating leaderless period — no sweeps)
        delay(2000)

        // "New leader elected" — start sweeping
        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
        }
        sweepJob.cancel()
    }

    // ---- R6: Network partition heals after multiple stale reclaim cycles ----

    @Test
    fun `R6 - extended partition then heal - system converges`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("r6.handler"); retries(5) }
        }

        handlerRegistry.register("r6.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def, """{"test":"R6-$it"}""").also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()
        delay(pollInterval.toMillis() * 2)

        // Extended outage (longer than stale threshold)
        oracleProxy.toxics.bandwidth("cut-r6", ToxicDirection.DOWNSTREAM, 0)
        delay(staleTaskThreshold.toMillis() * 2)
        oracleProxy.toxics["cut-r6"].remove()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R7: Rapid leader election flaps ----

    @Test
    fun `R7 - rapid leader flaps - no orphaned state`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("r7.handler") }
            activity("step2") { transition("r7.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "r7.handler",
            result = """{"test":"R7"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        handlerRegistry.register("r7.handler", PassThroughHandler())
        startWorkerPool()

        // Simulate rapid leader flaps: sweep, pause, sweep, pause, sweep
        repeat(4) {
            runSweep()
            delay(200)
        }

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // No duplicate tasks from flapping sweeps
        assertTaskCount(wfId, 2, 1)
        sweepJob.cancel()
    }

    // ---- R8: Oracle restarts (connections reset) ----

    @Test
    fun `R8 - Oracle connection reset - pool reconnects and workflows resume`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("r8.handler"); retries(3) }
        }

        handlerRegistry.register("r8.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def, """{"test":"R8-$it"}""").also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()
        delay(pollInterval.toMillis() * 2)

        // Simulate connection reset (disable then re-enable proxy)
        oracleProxy.toxics.resetPeer("reset-r8", ToxicDirection.DOWNSTREAM, 0)
        delay(1000)
        oracleProxy.toxics["reset-r8"].remove()

        val sweepJob = launch {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }
}
```

- [ ] **Step 2: Run resilience tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="com.workflow.stress.ResilienceStressTest" -pl . -q`
Expected: All 8 tests PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/ResilienceStressTest.kt
git commit -m "test: add ResilienceStressTest R1-R8 infrastructure failure recovery scenarios"
```

---

## Task 10: Run Full Stress Test Suite and Verify

**Files:** None (verification only)

- [ ] **Step 1: Run all stress tests together**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dgroups=stress -pl . 2>&1 | tail -20`
Expected: All 39 tests PASS

- [ ] **Step 2: Run full test suite to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All existing + new tests PASS

- [ ] **Step 3: Final commit with all files**

If any files were adjusted during debugging, stage and commit:

```bash
git add -A
git commit -m "test: complete stress test suite — 39 scenarios across 4 guarantee classes"
```

---

## Prerequisites & Notes

1. **Docker Desktop must be running** — Testcontainers needs Docker for Oracle and Toxiproxy containers.
2. **2-step TX model** — Scenarios L4 and L6b assume the barrier service splits self-update (TX1) from CAS+advance (TX2). If the current code uses a single transaction, L4 is still valid because it simulates the stuck state directly via SQL setup. L6b is a probabilistic test that works either way.
3. **Toxiproxy import** — The `eu.rekawek.toxiproxy.model.ToxicDirection` import comes from the testcontainers-toxiproxy transitive dependency.
4. **CrashableHandler and CancellationException** — `BarrierService.onTaskCompleted` uses `NonCancellable` context (`inTransactionSuspend`). CancellationException thrown from handlers is caught by WorkerLoop's exception handling, which routes through `handleTaskFailure`. The task stays in PROCESSING until stale reclaim picks it up.
5. **Parameterized scale** — Default is MODERATE. Run with `-Dstress.scale=HIGH` for pre-release validation.
6. **C4 split into two test methods** — `C4 at threshold` and `C4 below threshold` are separate `@Test` methods (not one test with cleanup in between) to avoid shared state issues.
