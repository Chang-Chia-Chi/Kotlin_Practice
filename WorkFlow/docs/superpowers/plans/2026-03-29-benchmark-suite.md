# Standalone Benchmark Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone CLI benchmark tool that boots Oracle, runs parameterized workflow scenarios with phase-level timing, and persists reproducible JSON results.

**Architecture:** Standalone `fun main()` entry point in test sources, manually wires engine components with instrumented subclass wrappers (leveraging Kotlin all-open on `@ApplicationScoped`), runs a Cartesian-product matrix of scenarios at configurable scales, persists timestamped JSON results.

**Tech Stack:** Kotlin, JDBI, Oracle Testcontainers, HikariCP, Jackson, Micrometer (optional), exec-maven-plugin

**Spec:** `docs/superpowers/specs/2026-03-29-benchmark-suite-design.md`

---

## File Structure

```
src/test/kotlin/benchmark/
  PhaseTimer.kt                     -- Per-phase nanosecond recording + percentile summary
  BenchmarkConfig.kt                -- Scale profiles, matrix generation, CLI arg parsing
  BenchmarkScenarios.kt             -- Workflow definitions + handler factories per shape
  InstrumentedComponents.kt         -- Subclass wrappers for TaskRepo, WorkflowRepo, Barrier, InputResolver
  BenchmarkHarness.kt               -- Enhanced harness (batch + sustained modes, window bucketing)
  BenchmarkReporter.kt              -- JSON persistence, console table formatting, result data models
  MetricsSupport.kt                 -- Optional Micrometer wiring + Prometheus scrape endpoint
  BenchmarkMain.kt                  -- Entry point, Oracle boot, wiring, matrix orchestration

  PhaseTimerTest.kt                 -- Unit tests for PhaseTimer
  BenchmarkConfigTest.kt            -- Unit tests for matrix generation + config parsing
  BenchmarkHarnessTest.kt           -- Unit tests for batch/sustained result computation
  BenchmarkReporterTest.kt          -- Unit tests for JSON serialization + table formatting

benchmarks/
  .gitignore                        -- Ignore result files

pom.xml                             -- New <profile id="benchmark"> with exec-maven-plugin
```

---

### Task 1: PhaseTimer

**Files:**
- Create: `src/test/kotlin/benchmark/PhaseTimer.kt`
- Test: `src/test/kotlin/benchmark/PhaseTimerTest.kt`

- [ ] **Step 1: Write the failing test**

```kotlin
// src/test/kotlin/benchmark/PhaseTimerTest.kt
package com.workflow.benchmark

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PhaseTimerTest {

    @Test
    fun `records timing and computes percentiles`() {
        val timer = PhaseTimer()
        repeat(100) { timer.time("test.phase") { Thread.sleep(1) } }
        val summary = timer.summary()
        val phase = summary["test.phase"]!!
        assertEquals(100, phase.count)
        assertTrue(phase.meanMs > 0.0)
        assertTrue(phase.p50Ms > 0.0)
        assertTrue(phase.p95Ms >= phase.p50Ms)
        assertTrue(phase.p99Ms >= phase.p95Ms)
    }

    @Test
    fun `reset clears all recordings`() {
        val timer = PhaseTimer()
        timer.time("a") { Thread.sleep(1) }
        timer.time("b") { Thread.sleep(1) }
        timer.reset()
        assertTrue(timer.summary().isEmpty())
    }

    @Test
    fun `multiple phases tracked independently`() {
        val timer = PhaseTimer()
        repeat(10) { timer.time("fast") { } }
        repeat(5) { timer.time("slow") { Thread.sleep(2) } }
        val summary = timer.summary()
        assertEquals(10, summary["fast"]!!.count)
        assertEquals(5, summary["slow"]!!.count)
        assertTrue(summary["slow"]!!.meanMs > summary["fast"]!!.meanMs)
    }

    @Test
    fun `time returns the block result`() {
        val timer = PhaseTimer()
        val result = timer.time("phase") { 42 }
        assertEquals(42, result)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest="PhaseTimerTest" -pl .`
Expected: FAIL — class `PhaseTimer` not found

- [ ] **Step 3: Write implementation**

```kotlin
// src/test/kotlin/benchmark/PhaseTimer.kt
package com.workflow.benchmark

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

data class PhaseSummary(
    val count: Int,
    val meanMs: Double,
    val p50Ms: Double,
    val p95Ms: Double,
    val p99Ms: Double,
)

class PhaseTimer {
    private val recordings = ConcurrentHashMap<String, CopyOnWriteArrayList<Long>>()

    fun <T> time(phase: String, block: () -> T): T {
        val start = System.nanoTime()
        try {
            return block()
        } finally {
            recordings.getOrPut(phase) { CopyOnWriteArrayList() }
                .add(System.nanoTime() - start)
        }
    }

    fun summary(): Map<String, PhaseSummary> =
        recordings.mapValues { (_, nanos) ->
            val ms = nanos.map { it / 1_000_000.0 }.sorted()
            PhaseSummary(
                count = ms.size,
                meanMs = ms.average(),
                p50Ms = ms.percentile(50),
                p95Ms = ms.percentile(95),
                p99Ms = ms.percentile(99),
            )
        }

    fun reset() = recordings.clear()
}

private fun List<Double>.percentile(p: Int): Double {
    if (isEmpty()) return 0.0
    val idx = (p / 100.0 * (size - 1)).toInt().coerceIn(0, size - 1)
    return this[idx]
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest="PhaseTimerTest" -pl .`
Expected: 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/benchmark/PhaseTimer.kt src/test/kotlin/benchmark/PhaseTimerTest.kt
git commit -m "feat(benchmark): add PhaseTimer for per-phase nanosecond timing"
```

---

### Task 2: BenchmarkConfig

**Files:**
- Create: `src/test/kotlin/benchmark/BenchmarkConfig.kt`
- Test: `src/test/kotlin/benchmark/BenchmarkConfigTest.kt`

- [ ] **Step 1: Write the failing test**

```kotlin
// src/test/kotlin/benchmark/BenchmarkConfigTest.kt
package com.workflow.benchmark

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BenchmarkConfigTest {

    @Test
    fun `quick scale generates small matrix for single scenario`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "single")
        assertTrue(points.isNotEmpty())
        for (p in points) {
            assertEquals("single", p.scenarioName)
            assertEquals(1, p.tasksPerWorkflow)
            assertTrue(p.workflows in listOf(20, 50))
            assertTrue(p.workers in listOf(5, 10))
        }
    }

    @Test
    fun `quick fanout matrix includes fanOutFactor axis`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "fanout")
        assertTrue(points.isNotEmpty())
        val factors = points.map { it.fanOutFactor }.distinct().sorted()
        assertEquals(listOf(10, 50), factors)
        for (p in points) {
            assertEquals(1 + p.fanOutFactor + 1, p.tasksPerWorkflow)
        }
    }

    @Test
    fun `thorough scale has more combinations than quick`() {
        val quick = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "single")
        val thorough = BenchmarkConfig.matrixFor(BenchmarkScale.THOROUGH, "single")
        assertTrue(thorough.size > quick.size)
    }

    @Test
    fun `soak scale produces sustained mode points`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.SOAK, "single")
        assertTrue(points.all { it.isSustained })
        assertTrue(points.all { it.submissionRate > 0 })
        assertTrue(points.all { it.durationSeconds > 0 })
    }

    @Test
    fun `multistep tasksPerWorkflow equals stepCount`() {
        val points = BenchmarkConfig.matrixFor(BenchmarkScale.QUICK, "multistep")
        for (p in points) {
            assertEquals(p.stepCount, p.tasksPerWorkflow)
        }
    }

    @Test
    fun `parse defaults to quick scale with all scenarios`() {
        // Clear any system properties that might interfere
        val config = BenchmarkConfig.parseFrom(emptyMap())
        assertEquals(BenchmarkScale.QUICK, config.scale)
        assertEquals(setOf("single", "fanout", "multistep"), config.scenarios)
        assertEquals(false, config.metricsEnabled)
    }

    @Test
    fun `parse reads system properties`() {
        val props = mapOf(
            "bench.scale" to "thorough",
            "bench.scenarios" to "fanout,single",
            "bench.metrics" to "true",
            "bench.workers" to "32",
        )
        val config = BenchmarkConfig.parseFrom(props)
        assertEquals(BenchmarkScale.THOROUGH, config.scale)
        assertEquals(setOf("fanout", "single"), config.scenarios)
        assertEquals(true, config.metricsEnabled)
        assertEquals(32, config.workerOverride)
    }

    @Test
    fun `timeout per point varies by scale`() {
        assertTrue(BenchmarkConfig.timeoutForScale(BenchmarkScale.QUICK).seconds <= 60)
        assertTrue(BenchmarkConfig.timeoutForScale(BenchmarkScale.THOROUGH).seconds <= 120)
        assertTrue(BenchmarkConfig.timeoutForScale(BenchmarkScale.SOAK).seconds >= 180)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest="BenchmarkConfigTest" -pl .`
Expected: FAIL — class `BenchmarkConfig` not found

- [ ] **Step 3: Write implementation**

```kotlin
// src/test/kotlin/benchmark/BenchmarkConfig.kt
package com.workflow.benchmark

import java.time.Duration

enum class BenchmarkScale { QUICK, THOROUGH, SOAK }

data class MatrixPoint(
    val scenarioName: String,
    val workflows: Int,
    val workers: Int,
    val handlerLatencyMs: Int,
    val payloadSizeBytes: Int,
    val fanOutFactor: Int = 0,
    val stepCount: Int = 0,
    val submissionRate: Int = 0,
    val durationSeconds: Int = 0,
) {
    val isSustained: Boolean get() = submissionRate > 0

    val tasksPerWorkflow: Int
        get() = when (scenarioName) {
            "single" -> 1
            "fanout" -> 1 + fanOutFactor + 1
            "multistep" -> stepCount
            else -> 1
        }

    fun toParameterMap(): Map<String, Any> = buildMap {
        put("workflows", workflows)
        put("workers", workers)
        put("handlerLatencyMs", handlerLatencyMs)
        put("payloadSizeBytes", payloadSizeBytes)
        if (fanOutFactor > 0) put("fanOutFactor", fanOutFactor)
        if (stepCount > 0) put("stepCount", stepCount)
        if (submissionRate > 0) put("submissionRate", submissionRate)
        if (durationSeconds > 0) put("durationSeconds", durationSeconds)
    }
}

data class BenchmarkRunConfig(
    val scale: BenchmarkScale,
    val scenarios: Set<String>,
    val metricsEnabled: Boolean,
    val workerOverride: Int? = null,
    val fanOutOverride: Int? = null,
)

object BenchmarkConfig {

    fun parse(): BenchmarkRunConfig = parseFrom(System.getProperties().map {
        it.key.toString() to it.value.toString()
    }.toMap())

    fun parseFrom(props: Map<String, String>): BenchmarkRunConfig {
        val scale = props["bench.scale"]?.uppercase()
            ?.let { BenchmarkScale.valueOf(it) }
            ?: BenchmarkScale.QUICK
        val scenarios = props["bench.scenarios"]
            ?.split(",")?.map { it.trim() }?.toSet()
            ?: setOf("single", "fanout", "multistep")
        val metricsEnabled = props["bench.metrics"]?.toBoolean() ?: false
        val workerOverride = props["bench.workers"]?.toIntOrNull()
        val fanOutOverride = props["bench.fanout.factor"]?.toIntOrNull()
        return BenchmarkRunConfig(scale, scenarios, metricsEnabled, workerOverride, fanOutOverride)
    }

    fun matrixFor(scale: BenchmarkScale, scenario: String): List<MatrixPoint> =
        when (scale) {
            BenchmarkScale.QUICK -> quickMatrix(scenario)
            BenchmarkScale.THOROUGH -> thoroughMatrix(scenario)
            BenchmarkScale.SOAK -> soakMatrix(scenario)
        }

    fun timeoutForScale(scale: BenchmarkScale): Duration = when (scale) {
        BenchmarkScale.QUICK -> Duration.ofSeconds(60)
        BenchmarkScale.THOROUGH -> Duration.ofSeconds(120)
        BenchmarkScale.SOAK -> Duration.ofSeconds(180)
    }

    private fun quickMatrix(scenario: String): List<MatrixPoint> {
        val latencies = listOf(0)
        val payloads = listOf(100)
        return when (scenario) {
            "single" -> cartesian(
                workflows = listOf(20, 50), workers = listOf(5, 10),
                latencies = latencies, payloads = payloads,
            ) { wf, w, lat, pay -> MatrixPoint("single", wf, w, lat, pay) }

            "fanout" -> cartesian(
                workflows = listOf(5), workers = listOf(10),
                latencies = latencies, payloads = payloads,
                extra = listOf(10, 50),
            ) { wf, w, lat, pay, fo -> MatrixPoint("fanout", wf, w, lat, pay, fanOutFactor = fo) }

            "multistep" -> cartesian(
                workflows = listOf(10), workers = listOf(5),
                latencies = latencies, payloads = payloads,
                extra = listOf(3, 5),
            ) { wf, w, lat, pay, sc -> MatrixPoint("multistep", wf, w, lat, pay, stepCount = sc) }

            else -> emptyList()
        }
    }

    private fun thoroughMatrix(scenario: String): List<MatrixPoint> {
        val latencies = listOf(0, 10)
        val payloads = listOf(100, 1000)
        return when (scenario) {
            "single" -> cartesian(
                workflows = listOf(50, 100, 200), workers = listOf(10, 20),
                latencies = latencies, payloads = payloads,
            ) { wf, w, lat, pay -> MatrixPoint("single", wf, w, lat, pay) }

            "fanout" -> cartesian(
                workflows = listOf(5, 10), workers = listOf(10, 20),
                latencies = latencies, payloads = payloads,
                extra = listOf(50, 100, 500),
            ) { wf, w, lat, pay, fo -> MatrixPoint("fanout", wf, w, lat, pay, fanOutFactor = fo) }

            "multistep" -> cartesian(
                workflows = listOf(10, 20), workers = listOf(10, 20),
                latencies = latencies, payloads = payloads,
                extra = listOf(3, 5, 10),
            ) { wf, w, lat, pay, sc -> MatrixPoint("multistep", wf, w, lat, pay, stepCount = sc) }

            else -> emptyList()
        }
    }

    private fun soakMatrix(scenario: String): List<MatrixPoint> {
        val latencies = listOf(0, 10, 50)
        val payloads = listOf(100, 1000, 10000)
        val dur = 120
        return when (scenario) {
            "single" -> cartesian(
                workflows = listOf(0), workers = listOf(10, 20, 50),
                latencies = latencies, payloads = payloads,
            ) { _, w, lat, pay ->
                MatrixPoint("single", 0, w, lat, pay, submissionRate = 50, durationSeconds = dur)
            }

            "fanout" -> cartesian(
                workflows = listOf(0), workers = listOf(20, 50),
                latencies = latencies, payloads = payloads,
                extra = listOf(100, 500, 1000),
            ) { _, w, lat, pay, fo ->
                MatrixPoint("fanout", 0, w, lat, pay, fanOutFactor = fo, submissionRate = 5, durationSeconds = dur)
            }

            "multistep" -> cartesian(
                workflows = listOf(0), workers = listOf(10, 20, 50),
                latencies = latencies, payloads = payloads,
                extra = listOf(5, 10, 20),
            ) { _, w, lat, pay, sc ->
                MatrixPoint("multistep", 0, w, lat, pay, stepCount = sc, submissionRate = 10, durationSeconds = dur)
            }

            else -> emptyList()
        }
    }

    // Cartesian product helpers

    private fun cartesian(
        workflows: List<Int>, workers: List<Int>,
        latencies: List<Int>, payloads: List<Int>,
        build: (Int, Int, Int, Int) -> MatrixPoint,
    ): List<MatrixPoint> =
        workflows.flatMap { wf ->
            workers.flatMap { w ->
                latencies.flatMap { lat ->
                    payloads.map { pay -> build(wf, w, lat, pay) }
                }
            }
        }

    private fun cartesian(
        workflows: List<Int>, workers: List<Int>,
        latencies: List<Int>, payloads: List<Int>,
        extra: List<Int>,
        build: (Int, Int, Int, Int, Int) -> MatrixPoint,
    ): List<MatrixPoint> =
        workflows.flatMap { wf ->
            workers.flatMap { w ->
                latencies.flatMap { lat ->
                    payloads.flatMap { pay ->
                        extra.map { e -> build(wf, w, lat, pay, e) }
                    }
                }
            }
        }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest="BenchmarkConfigTest" -pl .`
Expected: 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/benchmark/BenchmarkConfig.kt src/test/kotlin/benchmark/BenchmarkConfigTest.kt
git commit -m "feat(benchmark): add BenchmarkConfig with scale profiles and matrix generation"
```

---

### Task 3: BenchmarkScenarios

**Files:**
- Create: `src/test/kotlin/benchmark/BenchmarkScenarios.kt`

**Context:**
- DSL imports: `com.workflow.dsl.workflow`, `com.workflow.dsl.JoinPolicy`
- Handler interface: `com.workflow.worker.TransitionHandler`, `HandlerInput`, `HandlerOutput`
- Registry: `com.workflow.worker.HandlerRegistry`

- [ ] **Step 1: Write the scenario definitions and handler factories**

```kotlin
// src/test/kotlin/benchmark/BenchmarkScenarios.kt
package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import com.workflow.dsl.workflow
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.HandlerRegistry
import com.workflow.worker.TransitionHandler

object BenchmarkScenarios {

    fun singleActivityDefinition(): WorkflowDefinition = workflow {
        activity("process") { transition("bench.single.process") }
    }

    fun fanOutDefinition(fanOutFactor: Int): WorkflowDefinition = workflow {
        activity("scatter") {
            transition("bench.fanout.scatter")
            fanOut("parallel")
        }
        activity("parallel") {
            transition("bench.fanout.parallel")
            joinPolicy(JoinPolicy.All)
        }
        activity("join") {
            transition("bench.fanout.join")
            inputs {
                "results" from "parallel.result"
            }
        }
    }

    fun multiStepDefinition(stepCount: Int): WorkflowDefinition = workflow {
        for (i in 1..stepCount) {
            activity("step-$i") { transition("bench.multistep.step") }
        }
    }

    fun registerHandlers(
        registry: HandlerRegistry,
        objectMapper: ObjectMapper,
        point: MatrixPoint,
    ) {
        val baseHandler = payloadHandler(point.payloadSizeBytes)
        val handler = if (point.handlerLatencyMs > 0) {
            latencyHandler(point.handlerLatencyMs.toLong(), baseHandler)
        } else {
            baseHandler
        }

        when (point.scenarioName) {
            "single" -> {
                registry.register("bench.single.process", handler)
            }
            "fanout" -> {
                registry.register("bench.fanout.scatter", scatterHandler(point.fanOutFactor, objectMapper))
                registry.register("bench.fanout.parallel", handler)
                registry.register("bench.fanout.join", handler)
            }
            "multistep" -> {
                registry.register("bench.multistep.step", handler)
            }
        }
    }

    fun definitionFor(point: MatrixPoint): WorkflowDefinition = when (point.scenarioName) {
        "single" -> singleActivityDefinition()
        "fanout" -> fanOutDefinition(point.fanOutFactor)
        "multistep" -> multiStepDefinition(point.stepCount)
        else -> throw IllegalArgumentException("Unknown scenario: ${point.scenarioName}")
    }

    private fun payloadHandler(sizeBytes: Int): TransitionHandler = object : TransitionHandler {
        private val payload = """{"data":"${"x".repeat((sizeBytes - 10).coerceAtLeast(0))}"}"""
        override suspend fun execute(input: HandlerInput): HandlerOutput =
            HandlerOutput(result = payload)
    }

    private fun latencyHandler(delayMs: Long, delegate: TransitionHandler): TransitionHandler =
        object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                Thread.sleep(delayMs)
                return delegate.execute(input)
            }
        }

    private fun scatterHandler(fanOutFactor: Int, objectMapper: ObjectMapper): TransitionHandler =
        object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                val items = (1..fanOutFactor).map { mapOf("item" to it) }
                return HandlerOutput(result = objectMapper.writeValueAsString(items))
            }
        }
}
```

- [ ] **Step 2: Commit**

```bash
git add src/test/kotlin/benchmark/BenchmarkScenarios.kt
git commit -m "feat(benchmark): add BenchmarkScenarios with workflow definitions and handler factories"
```

---

### Task 4: InstrumentedComponents

**Files:**
- Create: `src/test/kotlin/benchmark/InstrumentedComponents.kt`

**Context:**
- All engine classes are `@ApplicationScoped` and thus opened by Kotlin all-open plugin
- Subclass the real implementations, override target methods with `timer.time()` around `super` call
- BarrierService receives instrumented repos via constructor, so its internal calls to repo methods are automatically timed

- [ ] **Step 1: Write the instrumented wrappers**

```kotlin
// src/test/kotlin/benchmark/InstrumentedComponents.kt
package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.engine.BarrierService
import com.workflow.engine.InputResolver
import com.workflow.engine.PhaseStrategyRegistry
import com.workflow.engine.SequenceInfo
import com.workflow.engine.Task
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.engine.WorkflowRepository
import com.workflow.worker.HandlerInput
import com.workflow.worker.HandlerOutput
import com.workflow.worker.TransitionHandler
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.time.Instant

class InstrumentedTaskRepository(
    jdbi: Jdbi,
    private val timer: PhaseTimer,
) : TaskRepository(jdbi) {

    override suspend fun claimNext(workerId: String, limit: Int, queueName: String): List<Task> =
        timer.time("task.claim") { super.claimNext(workerId, limit, queueName) }

    override fun insertBatchWithHandle(handle: Handle, tasks: List<Task>) =
        timer.time("task.insert") { super.insertBatchWithHandle(handle, tasks) }

    override fun insertFanOutFromScatter(
        handle: Handle, workflowId: String, scatterSequence: Int,
        targetSeqInfo: SequenceInfo, now: Instant,
    ) = timer.time("task.fanout_insert") {
        super.insertFanOutFromScatter(handle, workflowId, scatterSequence, targetSeqInfo, now)
    }
}

class InstrumentedWorkflowRepository(
    jdbi: Jdbi,
    private val timer: PhaseTimer,
) : WorkflowRepository(jdbi) {

    override fun casAdvanceWithHandle(
        handle: Handle, id: String, expectedSequence: Int,
        nextSequence: Int, expectedVersion: Int,
    ): Boolean = timer.time("workflow.cas") {
        super.casAdvanceWithHandle(handle, id, expectedSequence, nextSequence, expectedVersion)
    }
}

class InstrumentedBarrierService(
    jdbi: Jdbi,
    workflowRepo: WorkflowRepository,
    taskRepo: TaskRepository,
    objectMapper: ObjectMapper,
    strategyRegistry: PhaseStrategyRegistry,
    private val timer: PhaseTimer,
) : BarrierService(jdbi, workflowRepo, taskRepo, objectMapper, strategyRegistry) {

    override suspend fun onTaskCompleted(
        taskId: String, workflowId: String, sequenceNumber: Int,
        status: TaskStatus, resultJson: String?,
        claimedBy: String?, claimedAt: Instant?,
    ) = timer.time("barrier.evaluate") {
        super.onTaskCompleted(taskId, workflowId, sequenceNumber, status, resultJson, claimedBy, claimedAt)
    }
}

class InstrumentedInputResolver(
    objectMapper: ObjectMapper,
    private val timer: PhaseTimer,
) : InputResolver(objectMapper) {

    override suspend fun resolve(
        inputs: Map<String, String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: suspend (Int) -> List<Task>,
    ): String? = timer.time("input.resolve") {
        super.resolve(inputs, sequenceMap, tasksBySequence)
    }
}

class TimedHandler(
    private val delegate: TransitionHandler,
    private val timer: PhaseTimer,
) : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput =
        timer.time("handler.execute") { delegate.execute(input) }
}
```

- [ ] **Step 2: Commit**

```bash
git add src/test/kotlin/benchmark/InstrumentedComponents.kt
git commit -m "feat(benchmark): add instrumented component wrappers with PhaseTimer"
```

---

### Task 5: BenchmarkHarness

**Files:**
- Create: `src/test/kotlin/benchmark/BenchmarkHarness.kt`
- Test: `src/test/kotlin/benchmark/BenchmarkHarnessTest.kt`

- [ ] **Step 1: Write the failing test**

```kotlin
// src/test/kotlin/benchmark/BenchmarkHarnessTest.kt
package com.workflow.benchmark

import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class BenchmarkHarnessTest {

    @Test
    fun `batch result computes throughput and latency`() {
        val harness = EnhancedBenchmarkHarness()
        val base = Instant.now()
        // Simulate: 3 workflows, each taking ~100ms, ~200ms, ~300ms
        harness.recordSubmissionAt("wf1", base)
        harness.recordSubmissionAt("wf2", base.plusMillis(10))
        harness.recordSubmissionAt("wf3", base.plusMillis(20))
        harness.recordCompletionAt("wf1", base.plusMillis(100))
        harness.recordCompletionAt("wf2", base.plusMillis(210))
        harness.recordCompletionAt("wf3", base.plusMillis(320))

        val breakdown = mapOf("task.claim" to PhaseSummary(3, 1.0, 1.0, 1.0, 1.0))
        val result = harness.batchResult("test", tasksPerWorkflow = 2, phaseBreakdown = breakdown)

        assertEquals(3, result.totalWorkflows)
        assertEquals(6, result.totalTasks)
        assertTrue(result.wallClockMs in 300..321)
        assertTrue(result.workflowsPerSec > 0.0)
        assertTrue(result.latency.p50Ms in 100..300)
        assertEquals(1, result.phaseBreakdown.size)
        assertNull(result.windows)
    }

    @Test
    fun `sustained result buckets completions into windows`() {
        val harness = EnhancedBenchmarkHarness()
        val base = Instant.now()

        // Window 0 (0-10s): 5 workflows
        for (i in 0 until 5) {
            harness.recordSubmissionAt("w0-$i", base.plusMillis(i * 100L))
            harness.recordCompletionAt("w0-$i", base.plusMillis(500 + i * 100L))
        }
        // Window 1 (10-20s): 3 workflows
        for (i in 0 until 3) {
            harness.recordSubmissionAt("w1-$i", base.plusMillis(10_000 + i * 100L))
            harness.recordCompletionAt("w1-$i", base.plusMillis(10_500 + i * 100L))
        }

        val inflight = listOf(
            WindowSample(base.plusMillis(10_000), 2),
            WindowSample(base.plusMillis(20_000), 0),
        )
        val result = harness.sustainedResult(
            "test", tasksPerWorkflow = 1, phaseBreakdown = emptyMap(),
            windowDurationMs = 10_000, inflightSamples = inflight,
        )

        assertEquals(8, result.totalWorkflows)
        assertTrue(result.windows!!.size >= 2)
        assertTrue(result.windows!![0].workflowsPerSec > 0.0)
    }

    @Test
    fun `inflight count tracks unfinished workflows`() {
        val harness = EnhancedBenchmarkHarness()
        harness.recordSubmission("a")
        harness.recordSubmission("b")
        assertEquals(2, harness.inflightCount())
        harness.recordCompletion("a")
        assertEquals(1, harness.inflightCount())
    }

    @Test
    fun `reset clears all state`() {
        val harness = EnhancedBenchmarkHarness()
        harness.recordSubmission("a")
        harness.recordCompletion("a")
        harness.reset()
        assertEquals(0, harness.inflightCount())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest="BenchmarkHarnessTest" -pl .`
Expected: FAIL — class `EnhancedBenchmarkHarness` not found

- [ ] **Step 3: Write implementation**

```kotlin
// src/test/kotlin/benchmark/BenchmarkHarness.kt
package com.workflow.benchmark

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

data class LatencyStats(val p50Ms: Long, val p95Ms: Long, val p99Ms: Long)

data class WindowSnapshot(
    val offsetSec: Int,
    val workflowsPerSec: Double,
    val inflightCount: Int,
    val latency: LatencyStats,
)

data class WindowSample(val timestamp: Instant, val inflightCount: Int)

data class ScenarioResult(
    val name: String,
    val parameters: Map<String, Any>,
    val totalWorkflows: Int,
    val totalTasks: Int,
    val wallClockMs: Long,
    val workflowsPerSec: Double,
    val tasksPerSec: Double,
    val latency: LatencyStats,
    val phaseBreakdown: Map<String, PhaseSummary>,
    val windows: List<WindowSnapshot>? = null,
)

class EnhancedBenchmarkHarness {
    private val submissions = ConcurrentHashMap<String, Instant>()
    private val completions = ConcurrentHashMap<String, Instant>()

    fun recordSubmission(workflowId: String) {
        submissions[workflowId] = Instant.now()
    }

    fun recordSubmissionAt(workflowId: String, at: Instant) {
        submissions[workflowId] = at
    }

    fun recordCompletion(workflowId: String) {
        completions[workflowId] = Instant.now()
    }

    fun recordCompletionAt(workflowId: String, at: Instant) {
        completions[workflowId] = at
    }

    fun inflightCount(): Int = submissions.size - completions.size

    fun submittedIds(): Set<String> = submissions.keys.toSet()

    fun completedIds(): Set<String> = completions.keys.toSet()

    fun batchResult(
        label: String,
        tasksPerWorkflow: Int,
        phaseBreakdown: Map<String, PhaseSummary>,
    ): ScenarioResult {
        val latencies = perWorkflowLatencies()
        val wallClock = wallClockMs()
        val total = submissions.size
        return ScenarioResult(
            name = label,
            parameters = emptyMap(),
            totalWorkflows = total,
            totalTasks = total * tasksPerWorkflow,
            wallClockMs = wallClock,
            workflowsPerSec = if (wallClock > 0) total * 1000.0 / wallClock else 0.0,
            tasksPerSec = if (wallClock > 0) total * tasksPerWorkflow * 1000.0 / wallClock else 0.0,
            latency = latencyStats(latencies),
            phaseBreakdown = phaseBreakdown,
        )
    }

    fun sustainedResult(
        label: String,
        tasksPerWorkflow: Int,
        phaseBreakdown: Map<String, PhaseSummary>,
        windowDurationMs: Long = 10_000,
        inflightSamples: List<WindowSample>,
    ): ScenarioResult {
        val runStart = submissions.values.minOrNull() ?: Instant.now()
        val runEnd = completions.values.maxOrNull() ?: Instant.now()
        val totalDurationMs = Duration.between(runStart, runEnd).toMillis().coerceAtLeast(1)
        val total = submissions.size

        // Bucket completions into windows
        val windows = mutableListOf<WindowSnapshot>()
        var windowStart = runStart
        var windowIndex = 0
        while (windowStart.isBefore(runEnd)) {
            val windowEnd = windowStart.plusMillis(windowDurationMs)
            val completedInWindow = completions.entries.filter {
                !it.value.isBefore(windowStart) && it.value.isBefore(windowEnd)
            }
            val windowLatencies = completedInWindow.mapNotNull { (wfId, endTime) ->
                submissions[wfId]?.let { Duration.between(it, endTime).toMillis() }
            }
            val inflight = inflightSamples.getOrNull(windowIndex)?.inflightCount ?: 0
            val wfPerSec = if (windowDurationMs > 0) {
                completedInWindow.size * 1000.0 / windowDurationMs
            } else 0.0

            windows.add(WindowSnapshot(
                offsetSec = (windowIndex * windowDurationMs / 1000).toInt(),
                workflowsPerSec = wfPerSec,
                inflightCount = inflight,
                latency = latencyStats(windowLatencies),
            ))
            windowStart = windowEnd
            windowIndex++
        }

        val allLatencies = perWorkflowLatencies()
        return ScenarioResult(
            name = label,
            parameters = emptyMap(),
            totalWorkflows = total,
            totalTasks = total * tasksPerWorkflow,
            wallClockMs = totalDurationMs,
            workflowsPerSec = if (totalDurationMs > 0) total * 1000.0 / totalDurationMs else 0.0,
            tasksPerSec = if (totalDurationMs > 0) total * tasksPerWorkflow * 1000.0 / totalDurationMs else 0.0,
            latency = latencyStats(allLatencies),
            phaseBreakdown = phaseBreakdown,
            windows = windows,
        )
    }

    fun reset() {
        submissions.clear()
        completions.clear()
    }

    private fun perWorkflowLatencies(): List<Long> =
        submissions.keys.mapNotNull { wfId ->
            val start = submissions[wfId] ?: return@mapNotNull null
            val end = completions[wfId] ?: return@mapNotNull null
            Duration.between(start, end).toMillis()
        }

    private fun wallClockMs(): Long {
        val start = submissions.values.minOrNull() ?: return 0
        val end = completions.values.maxOrNull() ?: return 0
        return Duration.between(start, end).toMillis()
    }
}

private fun latencyStats(latencies: List<Long>): LatencyStats {
    if (latencies.isEmpty()) return LatencyStats(0, 0, 0)
    val sorted = latencies.sorted()
    return LatencyStats(
        p50Ms = sorted.percentile(50),
        p95Ms = sorted.percentile(95),
        p99Ms = sorted.percentile(99),
    )
}

private fun List<Long>.percentile(p: Int): Long {
    if (isEmpty()) return 0
    val idx = (p / 100.0 * (size - 1)).toInt().coerceIn(0, size - 1)
    return this[idx]
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest="BenchmarkHarnessTest" -pl .`
Expected: 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/benchmark/BenchmarkHarness.kt src/test/kotlin/benchmark/BenchmarkHarnessTest.kt
git commit -m "feat(benchmark): add EnhancedBenchmarkHarness with batch and sustained modes"
```

---

### Task 6: BenchmarkReporter

**Files:**
- Create: `src/test/kotlin/benchmark/BenchmarkReporter.kt`
- Test: `src/test/kotlin/benchmark/BenchmarkReporterTest.kt`

- [ ] **Step 1: Write the failing test**

```kotlin
// src/test/kotlin/benchmark/BenchmarkReporterTest.kt
package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BenchmarkReporterTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())

    @Test
    fun `saveReport writes valid JSON with all fields`(@TempDir tempDir: Path) {
        val report = BenchmarkReport(
            timestamp = "2026-03-29T14:30:00",
            scale = "quick",
            gitCommit = "abc1234",
            environment = EnvironmentInfo("Windows 11", 8, 4096, "21.0.2"),
            scenarios = listOf(
                ScenarioResult(
                    name = "single",
                    parameters = mapOf("workflows" to 50, "workers" to 10),
                    totalWorkflows = 50, totalTasks = 50,
                    wallClockMs = 2000, workflowsPerSec = 25.0, tasksPerSec = 25.0,
                    latency = LatencyStats(20, 45, 78),
                    phaseBreakdown = mapOf(
                        "task.claim" to PhaseSummary(50, 2.1, 1.8, 4.2, 6.1),
                    ),
                ),
            ),
        )

        BenchmarkReporter.saveReport(report, tempDir, objectMapper)

        val files = tempDir.toFile().listFiles()!!
        assertEquals(1, files.size)
        assertTrue(files[0].name.startsWith("quick-"))
        assertTrue(files[0].name.endsWith(".json"))

        val parsed = objectMapper.readValue<BenchmarkReport>(files[0])
        assertEquals("abc1234", parsed.gitCommit)
        assertEquals(1, parsed.scenarios.size)
        assertEquals(50, parsed.scenarios[0].totalWorkflows)
    }

    @Test
    fun `formatScenarioLine produces compact one-liner`() {
        val result = ScenarioResult(
            name = "fanout", parameters = mapOf("workflows" to 10, "fanOutFactor" to 500, "workers" to 20),
            totalWorkflows = 10, totalTasks = 5020,
            wallClockMs = 8432, workflowsPerSec = 1.19, tasksPerSec = 595.0,
            latency = LatencyStats(780, 1200, 1450),
            phaseBreakdown = emptyMap(),
        )
        val line = BenchmarkReporter.formatScenarioLine(result)
        assertTrue(line.contains("fanout"))
        assertTrue(line.contains("1.19"))
        assertTrue(line.contains("595.0"))
    }

    @Test
    fun `formatComparisonTable handles multiple results`() {
        val results = listOf(
            ScenarioResult("single", mapOf("workflows" to 20, "workers" to 5), 20, 20, 1000, 20.0, 20.0, LatencyStats(10, 20, 30), emptyMap()),
            ScenarioResult("single", mapOf("workflows" to 50, "workers" to 10), 50, 50, 2000, 25.0, 25.0, LatencyStats(15, 35, 50), emptyMap()),
        )
        val table = BenchmarkReporter.formatComparisonTable(results)
        assertTrue(table.contains("single"))
        assertTrue(table.contains("20.0"))
        assertTrue(table.contains("25.0"))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest="BenchmarkReporterTest" -pl .`
Expected: FAIL — class `BenchmarkReporter` not found

- [ ] **Step 3: Write implementation**

```kotlin
// src/test/kotlin/benchmark/BenchmarkReporter.kt
package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

data class EnvironmentInfo(
    val os: String,
    val cpuCores: Int,
    val jvmMaxMemoryMb: Long,
    val javaVersion: String,
)

data class BenchmarkReport(
    val timestamp: String,
    val scale: String,
    val gitCommit: String,
    val environment: EnvironmentInfo,
    val scenarios: List<ScenarioResult>,
)

object BenchmarkReporter {

    fun saveReport(report: BenchmarkReport, outputDir: Path, objectMapper: ObjectMapper) {
        Files.createDirectories(outputDir)
        val ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss"))
        val file = outputDir.resolve("${report.scale}-$ts.json")
        objectMapper.copy()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .writeValue(file.toFile(), report)
        println("Results saved to $file")
    }

    fun formatScenarioLine(r: ScenarioResult): String {
        val params = r.parameters.entries.joinToString(" ") { "${it.key}=${it.value}" }
        return "[${r.name}] $params -> ${"%.2f".format(r.workflowsPerSec)} wf/s | " +
            "${"%.1f".format(r.tasksPerSec)} tasks/s | " +
            "p50=${r.latency.p50Ms}ms p95=${r.latency.p95Ms}ms p99=${r.latency.p99Ms}ms"
    }

    fun formatComparisonTable(results: List<ScenarioResult>): String {
        if (results.isEmpty()) return "(no results)"

        // Collect all parameter keys across results
        val paramKeys = results.flatMap { it.parameters.keys }.distinct().sorted()

        // Header
        val headers = listOf("scenario") + paramKeys + listOf("wf/s", "tasks/s", "p50", "p95", "p99")
        val rows = results.map { r ->
            listOf(r.name) +
                paramKeys.map { r.parameters[it]?.toString() ?: "-" } +
                listOf(
                    "%.2f".format(r.workflowsPerSec),
                    "%.1f".format(r.tasksPerSec),
                    "${r.latency.p50Ms}ms",
                    "${r.latency.p95Ms}ms",
                    "${r.latency.p99Ms}ms",
                )
        }

        // Column widths
        val widths = headers.indices.map { col ->
            maxOf(headers[col].length, rows.maxOf { it[col].length })
        }

        val sep = "+-" + widths.joinToString("-+-") { "-".repeat(it) } + "-+"
        val headerLine = "| " + headers.mapIndexed { i, h -> h.padEnd(widths[i]) }.joinToString(" | ") + " |"
        val dataLines = rows.map { row ->
            "| " + row.mapIndexed { i, v -> v.padEnd(widths[i]) }.joinToString(" | ") + " |"
        }

        return buildString {
            appendLine(sep)
            appendLine(headerLine)
            appendLine(sep)
            dataLines.forEach { appendLine(it) }
            appendLine(sep)
        }
    }

    fun captureEnvironment(): EnvironmentInfo = EnvironmentInfo(
        os = "${System.getProperty("os.name")} ${System.getProperty("os.version")}",
        cpuCores = Runtime.getRuntime().availableProcessors(),
        jvmMaxMemoryMb = Runtime.getRuntime().maxMemory() / (1024 * 1024),
        javaVersion = System.getProperty("java.version"),
    )

    fun captureGitCommit(): String = try {
        val process = ProcessBuilder("git", "rev-parse", "--short", "HEAD")
            .redirectErrorStream(true).start()
        process.inputStream.bufferedReader().readLine()?.trim() ?: "unknown"
    } catch (_: Exception) {
        "unknown"
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest="BenchmarkReporterTest" -pl .`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/benchmark/BenchmarkReporter.kt src/test/kotlin/benchmark/BenchmarkReporterTest.kt
git commit -m "feat(benchmark): add BenchmarkReporter with JSON persistence and table formatting"
```

---

### Task 7: MetricsSupport

**Files:**
- Create: `src/test/kotlin/benchmark/MetricsSupport.kt`

**Context:**
- `io.micrometer.core.instrument.simple.SimpleMeterRegistry` is available (used in StressTestBase)
- `io.micrometer.prometheus.PrometheusMeterRegistry` / `PrometheusConfig` for Prometheus export
- When disabled, return a `SimpleMeterRegistry` (no-op-like, lightweight)
- When enabled, expose Prometheus scrape endpoint on port 19090

- [ ] **Step 1: Write implementation**

```kotlin
// src/test/kotlin/benchmark/MetricsSupport.kt
package com.workflow.benchmark

import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import java.net.InetSocketAddress

class MetricsSupport private constructor(
    val registry: MeterRegistry,
    private val prometheusRegistry: PrometheusMeterRegistry?,
    private var server: HttpServer?,
) {
    companion object {
        fun create(enabled: Boolean): MetricsSupport {
            if (!enabled) {
                return MetricsSupport(SimpleMeterRegistry(), null, null)
            }
            val prometheus = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            val server = HttpServer.create(InetSocketAddress(19090), 0).apply {
                createContext("/metrics") { exchange ->
                    val body = prometheus.scrape().toByteArray()
                    exchange.sendResponseHeaders(200, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                }
                start()
            }
            println("Prometheus metrics available at http://localhost:19090/metrics")
            return MetricsSupport(prometheus, prometheus, server)
        }
    }

    fun printSummary() {
        println("\n=== Micrometer Metrics Summary ===")
        registry.meters
            .sortedBy { it.id.name }
            .forEach { meter ->
                when (meter) {
                    is Timer -> {
                        val snap = meter.takeSnapshot()
                        println("  ${meter.id.name}: count=${snap.count()} mean=${"%.2f".format(snap.mean() * 1000)}ms max=${"%.2f".format(snap.max() * 1000)}ms")
                    }
                    else -> println("  ${meter.id.name}: ${meter.measure().joinToString { "${it.statistic}=${it.value}" }}")
                }
            }
        println()
    }

    fun stop() {
        server?.stop(0)
        server = null
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add src/test/kotlin/benchmark/MetricsSupport.kt
git commit -m "feat(benchmark): add MetricsSupport with optional Prometheus scrape endpoint"
```

---

### Task 8: BenchmarkMain

**Files:**
- Create: `src/test/kotlin/benchmark/BenchmarkMain.kt`

**Context:**
- Entry point: `fun main()`
- Wiring follows the pattern in `StressTestBase.initInfrastructure()` but uses instrumented components
- Two execution modes: batch (submit-and-wait) and sustained (submit-at-rate)
- SIGINT handler for graceful partial-result persistence
- Warmup run (small, discarded) before real matrix
- Clean tables between matrix points

- [ ] **Step 1: Write the main entry point**

```kotlin
// src/test/kotlin/benchmark/BenchmarkMain.kt
package com.workflow.benchmark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.config.FrameworkConfig
import com.workflow.engine.OracleTestContainer
import com.workflow.engine.PhaseStrategyRegistry
import com.workflow.engine.Sweeper
import com.workflow.engine.WorkflowEngine
import com.workflow.worker.HandlerRegistry
import com.workflow.worker.WorkerLoop
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.jdbi.v3.core.Jdbi
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

fun main() {
    val config = BenchmarkConfig.parse()
    println("=== Benchmark Suite ===")
    println("Scale: ${config.scale} | Scenarios: ${config.scenarios} | Metrics: ${config.metricsEnabled}")

    // 1. Boot Oracle
    println("Starting Oracle container...")
    val directJdbi = OracleTestContainer.jdbi
    val oracle = OracleTestContainer.oracle
    println("Oracle ready: ${oracle.jdbcUrl}")

    // 2. Create pooled DataSource
    val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = oracle.jdbcUrl
        username = oracle.username
        password = oracle.password
        maximumPoolSize = 30
        minimumIdle = 5
        connectionTimeout = 10_000
    })
    val pooledJdbi = Jdbi.create(dataSource)

    // 3. Wire components
    val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())

    val timer = PhaseTimer()
    val workflowRepo = InstrumentedWorkflowRepository(pooledJdbi, timer)
    val taskRepo = InstrumentedTaskRepository(pooledJdbi, timer)
    val strategyRegistry = PhaseStrategyRegistry()
    val barrier = InstrumentedBarrierService(pooledJdbi, workflowRepo, taskRepo, objectMapper, strategyRegistry, timer)
    val engine = WorkflowEngine(pooledJdbi, workflowRepo, taskRepo, objectMapper)
    val inputResolver = InstrumentedInputResolver(objectMapper, timer)
    val handlerRegistry = HandlerRegistry()

    // 4. Metrics
    val metrics = MetricsSupport.create(config.metricsEnabled)

    // 5. Generate matrix
    val allPoints = config.scenarios.flatMap { scenario ->
        BenchmarkConfig.matrixFor(config.scale, scenario)
    }
    println("Matrix: ${allPoints.size} points across ${config.scenarios.size} scenario(s)\n")

    val results = mutableListOf<ScenarioResult>()
    val gitCommit = BenchmarkReporter.captureGitCommit()
    val timeout = BenchmarkConfig.timeoutForScale(config.scale)

    // SIGINT handler
    val shutdownRequested = java.util.concurrent.atomic.AtomicBoolean(false)
    Runtime.getRuntime().addShutdownHook(Thread {
        shutdownRequested.set(true)
        if (results.isNotEmpty()) {
            val report = buildReport(config, gitCommit, results, objectMapper)
            BenchmarkReporter.saveReport(report, Path.of("benchmarks/results"), objectMapper)
            println("Partial results saved (${results.size} scenarios)")
        }
        metrics.stop()
        dataSource.close()
    })

    // 6. Warmup (discarded)
    val warmupPoint = allPoints.firstOrNull()
    if (warmupPoint != null) {
        println("Warmup run (discarded)...")
        val smallWarmup = warmupPoint.copy(workflows = 5, submissionRate = 0, durationSeconds = 0)
        runScenario(smallWarmup, engine, handlerRegistry, barrier, taskRepo, inputResolver,
            workflowRepo, objectMapper, timer, metrics, directJdbi, timeout, config)
        cleanTables(directJdbi)
        timer.reset()
        println("Warmup complete.\n")
    }

    // 7. Run matrix
    for ((index, point) in allPoints.withIndex()) {
        if (shutdownRequested.get()) break

        val applyPoint = applyOverrides(point, config)
        println("[${index + 1}/${allPoints.size}] Running ${applyPoint.scenarioName} ${applyPoint.toParameterMap()}")

        cleanTables(directJdbi)
        timer.reset()

        val result = runScenario(applyPoint, engine, handlerRegistry, barrier, taskRepo,
            inputResolver, workflowRepo, objectMapper, timer, metrics, directJdbi, timeout, config)

        if (result != null) {
            results.add(result)
            println("  ${BenchmarkReporter.formatScenarioLine(result)}")
        } else {
            println("  TIMEOUT — scenario did not complete within ${timeout.seconds}s")
            dumpDiagnostics(directJdbi)
        }
        println()
    }

    // 8. Report
    if (results.isNotEmpty()) {
        val report = buildReport(config, gitCommit, results, objectMapper)
        BenchmarkReporter.saveReport(report, Path.of("benchmarks/results"), objectMapper)
        println("\n${BenchmarkReporter.formatComparisonTable(results)}")
    }

    if (config.metricsEnabled) {
        metrics.printSummary()
    }

    // 9. Shutdown
    metrics.stop()
    dataSource.close()
    println("Done.")
}

private fun runScenario(
    point: MatrixPoint,
    engine: WorkflowEngine,
    handlerRegistry: HandlerRegistry,
    barrier: InstrumentedBarrierService,
    taskRepo: InstrumentedTaskRepository,
    inputResolver: InstrumentedInputResolver,
    workflowRepo: InstrumentedWorkflowRepository,
    objectMapper: ObjectMapper,
    timer: PhaseTimer,
    metrics: MetricsSupport,
    directJdbi: Jdbi,
    timeout: Duration,
    config: BenchmarkRunConfig,
): ScenarioResult? = runBlocking(Dispatchers.Default) {
    val definition = BenchmarkScenarios.definitionFor(point)

    // Wrap handlers with TimedHandler
    val timedRegistry = HandlerRegistry()
    BenchmarkScenarios.registerHandlers(timedRegistry, objectMapper, point)
    wrapRegistryWithTiming(handlerRegistry, timedRegistry, timer)

    val testConfig = createTestConfig(point.workers)
    val workerScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val loop = WorkerLoop(testConfig, taskRepo, handlerRegistry, barrier, metrics.registry,
        inputResolver, workflowRepo, objectMapper)
    val workerJob = loop.start(workerScope)

    val sweeper = Sweeper(directJdbi, workflowRepo, taskRepo, barrier, testConfig)
    val sweepJob = launch(Dispatchers.IO) {
        while (isActive) {
            delay(1000)
            timer.time("sweeper.cycle") { sweeper.patrol() }
        }
    }

    val harness = EnhancedBenchmarkHarness()
    val result: ScenarioResult?

    try {
        if (point.isSustained) {
            result = runSustained(point, definition, engine, harness, timer, timeout, directJdbi)
        } else {
            result = runBatch(point, definition, engine, harness, timer, timeout, directJdbi)
        }
    } finally {
        sweepJob.cancel()
        workerJob.cancelAndJoin()
    }

    result
}

private suspend fun runBatch(
    point: MatrixPoint,
    definition: com.workflow.dsl.WorkflowDefinition,
    engine: WorkflowEngine,
    harness: EnhancedBenchmarkHarness,
    timer: PhaseTimer,
    timeout: Duration,
    directJdbi: Jdbi,
): ScenarioResult? {
    val wfIds = (1..point.workflows).map {
        val wfId = engine.startWorkflow(definition)
        harness.recordSubmission(wfId)
        wfId
    }

    val completed = awaitCompletions(wfIds.toSet(), harness, directJdbi, timeout)
    if (!completed) return null

    return harness.batchResult(
        label = point.scenarioName,
        tasksPerWorkflow = point.tasksPerWorkflow,
        phaseBreakdown = timer.summary(),
    ).copy(parameters = point.toParameterMap())
}

private suspend fun CoroutineScope.runSustained(
    point: MatrixPoint,
    definition: com.workflow.dsl.WorkflowDefinition,
    engine: WorkflowEngine,
    harness: EnhancedBenchmarkHarness,
    timer: PhaseTimer,
    timeout: Duration,
    directJdbi: Jdbi,
): ScenarioResult? {
    val intervalMs = 1000L / point.submissionRate.coerceAtLeast(1)
    val durationMs = point.durationSeconds * 1000L
    val runStart = Instant.now()
    val allIds = ConcurrentHashMap.newKeySet<String>()
    val inflightSamples = mutableListOf<WindowSample>()

    // Submitter
    val submitterJob = launch(Dispatchers.IO) {
        val end = runStart.plusMillis(durationMs)
        while (isActive && Instant.now().isBefore(end)) {
            val wfId = engine.startWorkflow(definition)
            harness.recordSubmission(wfId)
            allIds.add(wfId)
            delay(intervalMs)
        }
    }

    // Sampler (10-second windows)
    val samplerJob = launch(Dispatchers.IO) {
        while (isActive) {
            delay(10_000)
            inflightSamples.add(WindowSample(Instant.now(), harness.inflightCount()))
        }
    }

    // Completion poller (runs alongside submitter)
    val pollerJob = launch(Dispatchers.IO) {
        while (isActive) {
            delay(500)
            pollAndRecordCompletions(allIds, harness, directJdbi)
        }
    }

    // Wait for submission phase to end
    submitterJob.join()

    // Wait for all remaining to complete (grace period)
    val grace = Duration.ofSeconds(60)
    val completed = awaitCompletions(allIds, harness, directJdbi, grace)

    samplerJob.cancel()
    pollerJob.cancel()

    if (!completed && harness.completedIds().size < allIds.size / 2) return null

    return harness.sustainedResult(
        label = point.scenarioName,
        tasksPerWorkflow = point.tasksPerWorkflow,
        phaseBreakdown = timer.summary(),
        inflightSamples = inflightSamples,
    ).copy(parameters = point.toParameterMap())
}

private suspend fun awaitCompletions(
    wfIds: Set<String>,
    harness: EnhancedBenchmarkHarness,
    directJdbi: Jdbi,
    timeout: Duration,
): Boolean {
    val deadline = Instant.now().plus(timeout)
    while (harness.completedIds().size < wfIds.size && Instant.now().isBefore(deadline)) {
        pollAndRecordCompletions(wfIds, harness, directJdbi)
        delay(200)
    }
    return harness.completedIds().size >= wfIds.size
}

private fun pollAndRecordCompletions(
    wfIds: Set<String>,
    harness: EnhancedBenchmarkHarness,
    directJdbi: Jdbi,
) {
    val pending = wfIds - harness.completedIds()
    if (pending.isEmpty()) return
    directJdbi.useHandle<Exception> { handle ->
        pending.chunked(500).forEach { chunk ->
            val completed = handle.createQuery(
                "SELECT id FROM workflow WHERE id IN (<ids>) AND status != 'RUNNING'",
            ).bindList("ids", chunk)
                .mapTo(String::class.java)
                .list()
            for (wfId in completed) {
                harness.recordCompletion(wfId)
            }
        }
    }
}

private fun wrapRegistryWithTiming(
    target: HandlerRegistry,
    source: HandlerRegistry,
    timer: PhaseTimer,
) {
    // Re-register all handlers from source into target wrapped with TimedHandler
    for (key in listOf(
        "bench.single.process",
        "bench.fanout.scatter", "bench.fanout.parallel", "bench.fanout.join",
        "bench.multistep.step",
    )) {
        try {
            val handler = source.resolve(key)
            target.register(key, TimedHandler(handler, timer))
        } catch (_: Exception) {
            // Handler not registered for this scenario — skip
        }
    }
}

private fun cleanTables(directJdbi: Jdbi) {
    directJdbi.useHandle<Exception> { handle ->
        handle.execute("DELETE FROM task")
        handle.execute("DELETE FROM workflow")
    }
}

private fun applyOverrides(point: MatrixPoint, config: BenchmarkRunConfig): MatrixPoint {
    var p = point
    config.workerOverride?.let { p = p.copy(workers = it) }
    config.fanOutOverride?.let { if (p.fanOutFactor > 0) p = p.copy(fanOutFactor = it) }
    return p
}

private fun createTestConfig(workers: Int): FrameworkConfig = object : FrameworkConfig {
    override fun worker() = object : FrameworkConfig.WorkerConfig {
        override fun id() = "bench-worker"
        override fun pollInterval() = Duration.ofMillis(100)
        override fun concurrency() = workers
        override fun batchSize() = 1
    }
    override fun leaderElection() = object : FrameworkConfig.LeaderElectionConfig {
        override fun namespace() = "default"
        override fun leaseName() = "bench-lease"
        override fun leaseDuration() = Duration.ofSeconds(15)
        override fun renewDeadline() = Duration.ofSeconds(10)
        override fun retryPeriod() = Duration.ofSeconds(2)
        override fun healthThreshold() = Duration.ofSeconds(45)
    }
    override fun shutdown() = object : FrameworkConfig.ShutdownConfig {
        override fun globalTimeout() = Duration.ofSeconds(30)
        override fun leaderTeardownTimeout() = Duration.ofSeconds(5)
    }
    override fun sweeper() = object : FrameworkConfig.SweeperConfig {
        override fun interval() = Duration.ofSeconds(1)
        override fun gracePeriod() = Duration.ofSeconds(2)
        override fun staleTaskThreshold() = Duration.ofSeconds(3)
    }
}

private fun dumpDiagnostics(directJdbi: Jdbi) {
    directJdbi.useHandle<Exception> { h ->
        println("  --- Diagnostic Dump ---")
        val wfCounts = h.createQuery(
            "SELECT status, COUNT(*) AS cnt FROM workflow GROUP BY status ORDER BY status",
        ).mapToMap().list()
        println("  Workflows: ${wfCounts.joinToString { "${it["STATUS"]}=${it["CNT"]}" }}")

        val taskCounts = h.createQuery(
            "SELECT status, COUNT(*) AS cnt FROM task GROUP BY status ORDER BY status",
        ).mapToMap().list()
        println("  Tasks: ${taskCounts.joinToString { "${it["STATUS"]}=${it["CNT"]}" }}")

        val stuck = h.createQuery(
            "SELECT id, current_sequence, status FROM workflow WHERE status = 'RUNNING' FETCH FIRST 5 ROWS ONLY",
        ).mapToMap().list()
        if (stuck.isNotEmpty()) {
            println("  Stuck workflows (sample):")
            for (wf in stuck) {
                println("    ${wf["ID"]} seq=${wf["CURRENT_SEQUENCE"]} status=${wf["STATUS"]}")
            }
        }
        println("  ---")
    }
}

private fun buildReport(
    config: BenchmarkRunConfig,
    gitCommit: String,
    results: List<ScenarioResult>,
    objectMapper: ObjectMapper,
): BenchmarkReport = BenchmarkReport(
    timestamp = java.time.LocalDateTime.now().toString(),
    scale = config.scale.name.lowercase(),
    gitCommit = gitCommit,
    environment = BenchmarkReporter.captureEnvironment(),
    scenarios = results,
)
```

- [ ] **Step 2: Verify compilation**

Run: `mvn test-compile -pl .`
Expected: BUILD SUCCESS (no runtime test, just compilation check)

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/benchmark/BenchmarkMain.kt
git commit -m "feat(benchmark): add BenchmarkMain entry point with matrix orchestration"
```

---

### Task 9: Maven Profile + .gitignore

**Files:**
- Modify: `pom.xml` (add `<profiles>` section)
- Create: `benchmarks/.gitignore`

- [ ] **Step 1: Add benchmark profile to pom.xml**

Add the following `<profiles>` section after the closing `</build>` tag (before `</project>`):

```xml
    <profiles>
        <profile>
            <id>benchmark</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.codehaus.mojo</groupId>
                        <artifactId>exec-maven-plugin</artifactId>
                        <version>3.1.0</version>
                        <configuration>
                            <mainClass>com.workflow.benchmark.BenchmarkMainKt</mainClass>
                            <classpathScope>test</classpathScope>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
    </profiles>
```

- [ ] **Step 2: Create benchmarks/.gitignore**

```
# Benchmark result files (machine-specific, not committed)
results/
```

- [ ] **Step 3: Commit**

```bash
git add pom.xml benchmarks/.gitignore
git commit -m "build: add benchmark Maven profile and results .gitignore"
```

---

### Task 10: Integration Smoke Test

**Goal:** Verify the full pipeline end-to-end by running quick scale.

**Prerequisite:** Docker Desktop must be running.

- [ ] **Step 1: Compile test sources**

Run: `mvn test-compile -pl .`
Expected: BUILD SUCCESS

- [ ] **Step 2: Run the benchmark at quick scale (single scenario only)**

Run: `mvn test-compile exec:java -Pbenchmark -Dbench.scale=quick -Dbench.scenarios=single`
Expected output (approximate):
```
=== Benchmark Suite ===
Scale: QUICK | Scenarios: [single] | Metrics: false
Starting Oracle container...
Oracle ready: jdbc:oracle:thin:@localhost:XXXXX/testdb
Matrix: 4 points across 1 scenario(s)

Warmup run (discarded)...
Warmup complete.

[1/4] Running single {workflows=20, workers=5, ...}
  [single] workflows=20 workers=5 -> X.XX wf/s | X.X tasks/s | p50=XXms p95=XXms p99=XXms

[2/4] Running single {workflows=20, workers=10, ...}
  ...

Results saved to benchmarks/results/quick-2026-03-29TXXXXXXX.json
+----------+-----------+---------+--------+-------+-------+-------+
| scenario | workflows | workers | wf/s   | ...   | p50   | p95   |
...
Done.
```

- [ ] **Step 3: Verify the results file exists and is valid JSON**

Run: `ls benchmarks/results/` — verify a `.json` file exists
Run: `cat benchmarks/results/quick-*.json | python -m json.tool` — verify valid JSON with `timestamp`, `gitCommit`, `environment`, `scenarios` fields

- [ ] **Step 4: Run fan-out scenario**

Run: `mvn test-compile exec:java -Pbenchmark -Dbench.scale=quick -Dbench.scenarios=fanout`
Expected: Completes with fan-out results showing higher tasks/s than single scenario

- [ ] **Step 5: Run multistep scenario**

Run: `mvn test-compile exec:java -Pbenchmark -Dbench.scale=quick -Dbench.scenarios=multistep`
Expected: Completes with multi-step results

- [ ] **Step 6: Run all scenarios together**

Run: `mvn test-compile exec:java -Pbenchmark -Dbench.scale=quick`
Expected: Runs all 3 scenarios (single + fanout + multistep matrix), produces comparison table

- [ ] **Step 7: Verify existing tests still pass**

Run: `mvn test -Dtest="PhaseTimerTest,BenchmarkConfigTest,BenchmarkHarnessTest,BenchmarkReporterTest" -pl .`
Expected: All unit tests PASS

- [ ] **Step 8: Commit any fixes from smoke testing**

```bash
git add -A
git commit -m "fix(benchmark): smoke test fixes from integration verification"
```
