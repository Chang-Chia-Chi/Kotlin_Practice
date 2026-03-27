# Metrics & Health Probes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Micrometer metrics (in-flight gauge, claim counters, handler duration histogram) and SmallRye Health liveness checks (worker loop, leader heartbeat) to the workflow engine.

**Architecture:** Metrics are registered in bean lifecycle methods (`onStart` / `registerMetrics`). Handler timing uses a decorator (`MeteredTransitionHandler`) wrapping handlers at registration in `HandlerRegistry` — keeping `WorkerLoop.processTask` untouched. Health checks are separate `@Liveness` beans that read state from `WorkerLoop` and `LeaderElection`. The worker loop's liveness signal is `lastActivityTimestamp` (updated on both poll and task completion) instead of `lastPollTimestamp` to avoid false alarms under saturation.

**Tech Stack:** Kotlin, Micrometer (`io.micrometer.core.instrument`), SmallRye Health (`org.eclipse.microprofile.health`), Quarkus CDI

---

## File Map

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `src/main/kotlin/worker/MeteredTransitionHandler.kt` | Decorator: times `execute()`, records histogram |
| Create | `src/main/kotlin/worker/WorkerLoopHealthCheck.kt` | `@Liveness` bean: checks `lastActivityTimestamp` freshness |
| Create | `src/main/kotlin/leader/LeaderHealthCheck.kt` | `@Liveness` bean: checks leader heartbeat age |
| Create | `src/test/kotlin/worker/MeteredTransitionHandlerTest.kt` | Tests for decorator timing + exception rethrow |
| Create | `src/test/kotlin/worker/WorkerLoopHealthCheckTest.kt` | Tests for UP/DOWN based on activity age |
| Create | `src/test/kotlin/leader/LeaderHealthCheckTest.kt` | Tests for follower-UP, leader-fresh-UP, leader-stale-DOWN |
| Modify | `src/main/kotlin/worker/WorkerLoop.kt` | Inject `MeterRegistry`, register gauges/counters, rename to `lastActivityTimestamp` |
| Modify | `src/main/kotlin/worker/HandlerRegistry.kt` | Inject `MeterRegistry`, wrap handlers with `MeteredTransitionHandler` |
| Modify | `src/main/kotlin/config/FrameworkConfig.kt` | Add `healthThreshold()` to `LeaderElectionConfig` |
| Modify | `src/main/kotlin/leader/LeaderManager.kt` | Add heartbeat age gauge in `registerMetrics()` |
| Modify | `src/test/kotlin/worker/WorkerLoopTest.kt` | Add `MeterRegistry` to constructor, add metric assertion tests |
| Modify | `src/test/kotlin/worker/HandlerRegistryTest.kt` | Add `MeterRegistry` to constructor, verify wrapping behavior |

---

### Task 1: MeteredTransitionHandler — decorator for handler timing (R3.3)

**Files:**
- Create: `src/main/kotlin/worker/MeteredTransitionHandler.kt`
- Create: `src/test/kotlin/worker/MeteredTransitionHandlerTest.kt`

- [ ] **Step 1: Write the failing tests**

Create `src/test/kotlin/worker/MeteredTransitionHandlerTest.kt`:

```kotlin
package com.workflow.worker

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class MeteredTransitionHandlerTest {

    private lateinit var meterRegistry: SimpleMeterRegistry

    private val input = HandlerInput(
        taskId = "t1",
        workflowId = "wf1",
        sequenceNumber = 1,
        payload = null,
    )

    @BeforeEach
    fun setup() {
        meterRegistry = SimpleMeterRegistry()
    }

    @Test
    fun `records success timer and returns delegate output`() = runTest {
        val delegate = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput) = HandlerOutput(result = "ok")
        }
        val metered = MeteredTransitionHandler(delegate, "order.validate", meterRegistry)

        val output = metered.execute(input)

        assertEquals("ok", output.result)

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "order.validate")
            .tag("status", "success")
            .timer()
        assertNotNull(timer, "success timer should be registered")
        assertEquals(1, timer.count())
    }

    @Test
    fun `records failure timer and rethrows exception`() = runTest {
        val delegate = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                throw IllegalStateException("boom")
            }
        }
        val metered = MeteredTransitionHandler(delegate, "order.validate", meterRegistry)

        assertFailsWith<IllegalStateException>("boom") {
            metered.execute(input)
        }

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "order.validate")
            .tag("status", "failure")
            .timer()
        assertNotNull(timer, "failure timer should be registered")
        assertEquals(1, timer.count())
    }

    @Test
    fun `multiple executions accumulate in timer`() = runTest {
        val delegate = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput) = HandlerOutput(result = null)
        }
        val metered = MeteredTransitionHandler(delegate, "step.process", meterRegistry)

        repeat(3) { metered.execute(input) }

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "step.process")
            .tag("status", "success")
            .timer()
        assertNotNull(timer)
        assertEquals(3, timer.count())
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="MeteredTransitionHandlerTest" -Dsurefire.failIfNoSpecifiedTests=false`

Expected: Compilation failure — `MeteredTransitionHandler` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/kotlin/worker/MeteredTransitionHandler.kt`:

```kotlin
package com.workflow.worker

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

class MeteredTransitionHandler(
    private val delegate: TransitionHandler,
    private val handlerKey: String,
    private val meterRegistry: MeterRegistry,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val sample = Timer.start(meterRegistry)
        try {
            val output = delegate.execute(input)
            sample.stop(timer("success"))
            return output
        } catch (e: Exception) {
            sample.stop(timer("failure"))
            throw e
        }
    }

    private fun timer(status: String): Timer =
        Timer.builder("taskqueue_handler_duration_seconds")
            .tag("handler", handlerKey)
            .tag("status", status)
            .publishPercentileHistogram()
            .register(meterRegistry)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="MeteredTransitionHandlerTest"`

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/MeteredTransitionHandler.kt src/test/kotlin/worker/MeteredTransitionHandlerTest.kt
git commit -m "feat: add MeteredTransitionHandler decorator for handler duration histogram"
```

---

### Task 2: HandlerRegistry — inject MeterRegistry, wrap on register (R3.3)

**Files:**
- Modify: `src/main/kotlin/worker/HandlerRegistry.kt`
- Modify: `src/test/kotlin/worker/HandlerRegistryTest.kt`

- [ ] **Step 1: Write the failing test**

Add to `src/test/kotlin/worker/HandlerRegistryTest.kt`. Replace the file with:

```kotlin
package com.workflow.worker

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class HandlerRegistryTest {

    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var registry: HandlerRegistry

    @BeforeEach
    fun setup() {
        meterRegistry = SimpleMeterRegistry()
        registry = HandlerRegistry(meterRegistry)
    }

    @Test
    fun `register handler and resolve by key returns metered wrapper`() = runTest {
        val handler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = null)
        }

        registry.register("order.validate", handler)

        val resolved = registry.resolve("order.validate")
        assertIs<MeteredTransitionHandler>(resolved)
    }

    @Test
    fun `resolve unknown key throws IllegalStateException with key in message`() = runTest {
        val ex = assertFailsWith<IllegalStateException> {
            registry.resolve("nonexistent.key")
        }
        assertTrue(
            ex.message!!.contains("nonexistent.key"),
            "Exception message should contain the missing key, was: ${ex.message}",
        )
    }

    @Test
    fun `register second handler with same key overwrites first`() = runTest {
        val firstHandler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = "first")
        }
        val secondHandler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = "second")
        }

        registry.register("step.one", firstHandler)
        registry.register("step.one", secondHandler)

        val resolved = registry.resolve("step.one")
        val output = resolved.execute(
            HandlerInput(taskId = "t1", workflowId = "wf1", sequenceNumber = 1, payload = null),
        )
        assertEquals("second", output.result)
    }

    @Test
    fun `resolved handler records timer metric on execute`() = runTest {
        val handler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput =
                HandlerOutput(result = "done")
        }

        registry.register("order.ship", handler)
        registry.resolve("order.ship").execute(
            HandlerInput(taskId = "t1", workflowId = "wf1", sequenceNumber = 1, payload = null),
        )

        val timer = meterRegistry.find("taskqueue_handler_duration_seconds")
            .tag("handler", "order.ship")
            .tag("status", "success")
            .timer()
        assertEquals(1, timer?.count())
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="HandlerRegistryTest"`

Expected: Compilation failure — `HandlerRegistry` constructor doesn't accept `MeterRegistry`.

- [ ] **Step 3: Write minimal implementation**

Replace `src/main/kotlin/worker/HandlerRegistry.kt` with:

```kotlin
package com.workflow.worker

import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class HandlerRegistry(
    private val meterRegistry: MeterRegistry,
) {

    private val handlers = ConcurrentHashMap<String, TransitionHandler>()

    fun resolve(key: String): TransitionHandler =
        handlers[key] ?: throw IllegalStateException("No handler found for key: $key")

    fun register(key: String, handler: TransitionHandler) {
        handlers[key] = MeteredTransitionHandler(handler, key, meterRegistry)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="HandlerRegistryTest"`

Expected: 4 tests PASS.

- [ ] **Step 5: Fix WorkerLoopTest compilation**

`WorkerLoopTest` mocks `HandlerRegistry` so constructor change won't break it (Mockito creates the mock without calling the constructor). Run full worker tests to confirm:

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="WorkerLoopTest,HandlerRegistryTest,MeteredTransitionHandlerTest"`

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/worker/HandlerRegistry.kt src/test/kotlin/worker/HandlerRegistryTest.kt
git commit -m "feat: HandlerRegistry wraps handlers with MeteredTransitionHandler on register"
```

---

### Task 3: WorkerLoop — inject MeterRegistry, add gauges + claim counters, rename to lastActivityTimestamp (R3.1, R3.2, R3.4 prep)

**Files:**
- Modify: `src/main/kotlin/worker/WorkerLoop.kt`
- Modify: `src/test/kotlin/worker/WorkerLoopTest.kt`

- [ ] **Step 1: Write the failing tests**

Add a new `@Nested` class inside `WorkerLoopTest` (after the existing `@BeforeEach`). First, update the `setup()` to include `SimpleMeterRegistry` and pass it to WorkerLoop. Then add the nested test class.

In `WorkerLoopTest`, add import:
```kotlin
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
```

In the class body, add a field:
```kotlin
private lateinit var meterRegistry: SimpleMeterRegistry
```

In `setup()`, add before the `workerLoop =` line:
```kotlin
meterRegistry = SimpleMeterRegistry()
```

Change the `workerLoop =` line to:
```kotlin
workerLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService, meterRegistry)
```

Then add this new nested class at the end of `WorkerLoopTest`:

```kotlin
@Nested
inner class MetricsTest {

    @Test
    fun `registers in-flight tasks gauge that reflects actual count`() = runTest {
        val handler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerOutput {
                delay(5_000)
                return HandlerOutput(result = null)
            }
        }
        whenever(handlerRegistry.resolve(any())).thenReturn(handler)
        whenever(taskRepo.claimNext(any(), any())).thenReturn(listOf(aTask()))

        val job = workerLoop.start(this)
        advanceTimeBy(500)

        val gauge = meterRegistry.find("taskqueue_worker_in_flight_tasks").gauge()
        assertNotNull(gauge, "in-flight gauge should be registered")
        assertEquals(1.0, gauge.value())

        workerLoop.shutdown()
        job.join()
    }

    @Test
    fun `registers concurrency limit gauge from config`() = runTest {
        whenever(taskRepo.claimNext(any(), any())).thenReturn(emptyList())

        val job = workerLoop.start(this)
        advanceTimeBy(100)

        val gauge = meterRegistry.find("taskqueue_worker_concurrency_limit").gauge()
        assertNotNull(gauge, "concurrency limit gauge should be registered")
        assertEquals(concurrency.toDouble(), gauge.value())

        workerLoop.shutdown()
        job.join()
    }

    @Test
    fun `increments claim counter with empty outcome when no tasks`() = runTest {
        whenever(taskRepo.claimNext(any(), any())).thenReturn(emptyList())

        val job = workerLoop.start(this)
        advanceTimeBy(1_500)

        val counter = meterRegistry.find("taskqueue_claim_total")
            .tag("outcome", "empty")
            .counter()
        assertNotNull(counter, "empty outcome counter should be registered")
        assertTrue(counter.count() >= 1.0)

        workerLoop.shutdown()
        job.join()
    }

    @Test
    fun `increments claim counter with success outcome and claimed tasks counter`() = runTest {
        val handler = object : TransitionHandler {
            override suspend fun execute(input: HandlerInput) = HandlerOutput(result = null)
        }
        whenever(handlerRegistry.resolve(any())).thenReturn(handler)
        whenever(taskRepo.claimNext(any(), any()))
            .thenReturn(listOf(aTask()))
            .thenReturn(emptyList())

        val job = workerLoop.start(this)
        advanceTimeBy(1_500)

        val successCounter = meterRegistry.find("taskqueue_claim_total")
            .tag("outcome", "success")
            .counter()
        assertNotNull(successCounter, "success outcome counter should be registered")
        assertTrue(successCounter.count() >= 1.0)

        val claimedCounter = meterRegistry.find("taskqueue_claimed_tasks_total").counter()
        assertNotNull(claimedCounter, "claimed tasks counter should be registered")
        assertTrue(claimedCounter.count() >= 1.0)

        workerLoop.shutdown()
        job.join()
    }

    @Test
    fun `increments claim counter with error outcome on claimNext failure`() = runTest {
        whenever(taskRepo.claimNext(any(), any())).thenThrow(RuntimeException("db down"))

        val job = workerLoop.start(this)
        advanceTimeBy(1_500)

        val counter = meterRegistry.find("taskqueue_claim_total")
            .tag("outcome", "error")
            .counter()
        assertNotNull(counter, "error outcome counter should be registered")
        assertTrue(counter.count() >= 1.0)

        workerLoop.shutdown()
        job.join()
    }

    @Test
    fun `lastActivityTimestamp updates on poll`() = runTest {
        whenever(taskRepo.claimNext(any(), any())).thenReturn(emptyList())

        val before = workerLoop.lastActivityTimestamp
        val job = workerLoop.start(this)
        advanceTimeBy(1_500)

        assertTrue(
            workerLoop.lastActivityTimestamp >= before,
            "lastActivityTimestamp should update after poll",
        )

        workerLoop.shutdown()
        job.join()
    }

    private fun aTask() = Task(
        id = UUID.randomUUID().toString(),
        workflowId = "wf-1",
        sequenceNumber = 1,
        handlerKey = "test.handler",
        status = TaskStatus.CLAIMED,
        payloadJson = null,
        resultJson = null,
        retryCount = 0,
        maxRetries = 3,
        claimedBy = workerId,
        createdAt = Instant.now().truncatedTo(ChronoUnit.MICROS),
        updatedAt = Instant.now().truncatedTo(ChronoUnit.MICROS),
    )
}
```

Also add this import if not already present:
```kotlin
import kotlin.test.assertNotNull
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="WorkerLoopTest"`

Expected: Compilation failure — `WorkerLoop` constructor doesn't accept `MeterRegistry`.

- [ ] **Step 3: Implement WorkerLoop changes**

Modify `src/main/kotlin/worker/WorkerLoop.kt`:

**Add imports** (after existing imports):
```kotlin
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
```

**Add `meterRegistry` to constructor** (after `barrierService`):
```kotlin
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val taskRepo: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val barrierService: BarrierService,
    private val meterRegistry: MeterRegistry,
) : ShutdownParticipant {
```

**Rename `_lastPollTimestamp` → `_lastActivityTimestamp`** and rename the public property:
```kotlin
@Volatile
private var _lastActivityTimestamp: Instant = Instant.now()
val lastActivityTimestamp: Instant get() = _lastActivityTimestamp
```

**Add counter fields** (after `_lastActivityTimestamp`):
```kotlin
private lateinit var claimTotal: (String) -> Counter
private lateinit var claimedTasksTotal: Counter
```

**Register metrics in `start()`** before the `val job =` line, after `val pollInterval = ...`:
```kotlin
val podTag = Tags.of("pod", workerId)
meterRegistry.gauge(
    "taskqueue_worker_in_flight_tasks",
    podTag,
    _inFlightTasks,
) { it.get().toDouble() }
meterRegistry.gauge(
    "taskqueue_worker_concurrency_limit",
    podTag,
    concurrency,
) { it.toDouble() }
claimTotal = { outcome: String ->
    meterRegistry.counter("taskqueue_claim_total", "pod", workerId, "outcome", outcome)
}
claimedTasksTotal = meterRegistry.counter("taskqueue_claimed_tasks_total", "pod", workerId)
```

**Update `pollAndProcess`** — replace the current body with:
```kotlin
private suspend fun pollAndProcess(
    workerId: String,
    pollInterval: Duration,
    batchSize: Int,
) {
    val tasks =
        try {
            taskRepo.claimNext(workerId, batchSize)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Failed to claim tasks", e)
            claimTotal("error").increment()
            delay(pollInterval.toMillis())
            return
        }
    _lastActivityTimestamp = Instant.now()

    if (tasks.isEmpty()) {
        claimTotal("empty").increment()
        delay(pollInterval.toMillis())
        return
    }

    claimTotal("success").increment()
    claimedTasksTotal.increment(tasks.size.toDouble())

    for (task in tasks) {
        processTask(task)
    }
}
```

**Update `processTask` finally block** — add activity timestamp update:
```kotlin
} finally {
    _inFlightTasks.decrementAndGet()
    _lastActivityTimestamp = Instant.now()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="WorkerLoopTest"`

Expected: All tests PASS (existing + new metrics tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/WorkerLoop.kt src/test/kotlin/worker/WorkerLoopTest.kt
git commit -m "feat: add in-flight gauge, claim outcome counters, and lastActivityTimestamp to WorkerLoop"
```

---

### Task 4: WorkerLoopHealthCheck — liveness probe (R3.4)

**Files:**
- Create: `src/main/kotlin/worker/WorkerLoopHealthCheck.kt`
- Create: `src/test/kotlin/worker/WorkerLoopHealthCheckTest.kt`

- [ ] **Step 1: Write the failing tests**

Create `src/test/kotlin/worker/WorkerLoopHealthCheckTest.kt`:

```kotlin
package com.workflow.worker

import com.workflow.config.FrameworkConfig
import org.eclipse.microprofile.health.HealthCheckResponse
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals

class WorkerLoopHealthCheckTest {

    private val workerConfig = mock<FrameworkConfig.WorkerConfig>().also {
        whenever(it.pollInterval()).thenReturn(Duration.ofSeconds(1))
    }
    private val config = mock<FrameworkConfig>().also {
        whenever(it.worker()).thenReturn(workerConfig)
    }

    @Test
    fun `returns UP when last activity is recent`() {
        val workerLoop = mock<WorkerLoop>().also {
            whenever(it.lastActivityTimestamp).thenReturn(Instant.now())
        }
        val check = WorkerLoopHealthCheck(workerLoop, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
        assertEquals("worker-loop", response.name)
    }

    @Test
    fun `returns DOWN when last activity exceeds threshold`() {
        val staleTime = Instant.now().minus(Duration.ofSeconds(10))
        val workerLoop = mock<WorkerLoop>().also {
            whenever(it.lastActivityTimestamp).thenReturn(staleTime)
        }
        val check = WorkerLoopHealthCheck(workerLoop, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        assertEquals("worker-loop", response.name)
        assertEquals(
            true,
            response.data.isPresent,
            "DOWN response should include diagnostic data",
        )
    }

    @Test
    fun `threshold is 5x poll interval`() {
        // pollInterval = 1s, threshold = 5s, age = 4s -> UP
        val justUnder = Instant.now().minus(Duration.ofSeconds(4))
        val workerLoop = mock<WorkerLoop>().also {
            whenever(it.lastActivityTimestamp).thenReturn(justUnder)
        }
        val check = WorkerLoopHealthCheck(workerLoop, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="WorkerLoopHealthCheckTest" -Dsurefire.failIfNoSpecifiedTests=false`

Expected: Compilation failure — `WorkerLoopHealthCheck` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/kotlin/worker/WorkerLoopHealthCheck.kt`:

```kotlin
package com.workflow.worker

import com.workflow.config.FrameworkConfig
import jakarta.inject.Singleton
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import java.time.Duration
import java.time.Instant

@Liveness
@Singleton
class WorkerLoopHealthCheck(
    private val workerLoop: WorkerLoop,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val lastActivity = workerLoop.lastActivityTimestamp
        val threshold = config.worker().pollInterval().multipliedBy(5)
        val age = Duration.between(lastActivity, Instant.now())

        return if (age < threshold) {
            HealthCheckResponse.up("worker-loop")
        } else {
            HealthCheckResponse.named("worker-loop")
                .down()
                .withData("last_activity_age_seconds", age.seconds)
                .withData("threshold_seconds", threshold.seconds)
                .build()
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="WorkerLoopHealthCheckTest"`

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/WorkerLoopHealthCheck.kt src/test/kotlin/worker/WorkerLoopHealthCheckTest.kt
git commit -m "feat: add WorkerLoopHealthCheck liveness probe using lastActivityTimestamp"
```

---

### Task 5: FrameworkConfig — add healthThreshold to LeaderElectionConfig (R3.5 prep)

**Files:**
- Modify: `src/main/kotlin/config/FrameworkConfig.kt`

- [ ] **Step 1: Add the config field**

In `src/main/kotlin/config/FrameworkConfig.kt`, add to `LeaderElectionConfig` interface after `retryPeriod()`:

```kotlin
@WithDefault("PT45S")
fun healthThreshold(): Duration
```

The full `LeaderElectionConfig` becomes:

```kotlin
interface LeaderElectionConfig {
    @WithDefault("default")
    fun namespace(): String
    @WithDefault("workflow-leader")
    fun leaseName(): String
    @WithDefault("PT15S")
    fun leaseDuration(): Duration
    @WithDefault("PT10S")
    fun renewDeadline(): Duration
    @WithDefault("PT2S")
    fun retryPeriod(): Duration
    @WithDefault("PT45S")
    fun healthThreshold(): Duration
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`

Expected: BUILD SUCCESS. Existing tests that mock `LeaderElectionConfig` will still work — Mockito returns default values for unmocked methods (`Duration` defaults to `null`, but tests that don't use `healthThreshold()` won't call it).

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/config/FrameworkConfig.kt
git commit -m "feat: add healthThreshold config field for leader liveness check"
```

---

### Task 6: LeaderManager — add heartbeat age gauge (R3.5)

**Files:**
- Modify: `src/main/kotlin/leader/LeaderManager.kt`

- [ ] **Step 1: Write the failing test**

Add to `src/test/kotlin/leader/LeaderManagerTest.kt`, in an existing or new test method. Find the test class setup and add this test (the test class already uses `SimpleMeterRegistry`):

```kotlin
@Test
fun `registerMetrics exposes heartbeat age gauge`() {
    val meterRegistry = SimpleMeterRegistry()
    val manager = LeaderManager(config, kubernetesClient, meterRegistry, kubernetesDetector)
    manager.clock = fixedClock
    manager.scope = CoroutineScope(SupervisorJob())

    whenever(kubernetesDetector.isRunningInKubernetes()).thenReturn(false)
    manager.onStart(mock())

    val gauge = meterRegistry.find("leader_election_heartbeat_age_seconds").gauge()
    assertNotNull(gauge, "heartbeat age gauge should be registered")
    assertTrue(gauge.value() >= 0.0)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="LeaderManagerTest#registerMetrics exposes heartbeat age gauge"`

Expected: FAIL — gauge not found (returns null).

- [ ] **Step 3: Add heartbeat age gauge to registerMetrics**

In `src/main/kotlin/leader/LeaderManager.kt`, modify `registerMetrics()`:

```kotlin
private fun registerMetrics() {
    meterRegistry.gauge("leader_election_is_leader", this) { if (isActive) 1.0 else 0.0 }
    meterRegistry.gauge("leader_election_epoch", this) { token.toDouble() }
    meterRegistry.gauge("leader_election_heartbeat_age_seconds", this) {
        Duration.between(lastHeartbeat, Instant.now(clock)).toSeconds().toDouble()
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="LeaderManagerTest"`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/leader/LeaderManager.kt src/test/kotlin/leader/LeaderManagerTest.kt
git commit -m "feat: add leader_election_heartbeat_age_seconds gauge to LeaderManager"
```

---

### Task 7: LeaderHealthCheck — stale-leader liveness probe (R3.5)

**Files:**
- Create: `src/main/kotlin/leader/LeaderHealthCheck.kt`
- Create: `src/test/kotlin/leader/LeaderHealthCheckTest.kt`

- [ ] **Step 1: Write the failing tests**

Create `src/test/kotlin/leader/LeaderHealthCheckTest.kt`:

```kotlin
package com.workflow.leader

import com.workflow.config.FrameworkConfig
import org.eclipse.microprofile.health.HealthCheckResponse
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals

class LeaderHealthCheckTest {

    private val leaderElectionConfig = mock<FrameworkConfig.LeaderElectionConfig>().also {
        whenever(it.healthThreshold()).thenReturn(Duration.ofSeconds(45))
    }
    private val config = mock<FrameworkConfig>().also {
        whenever(it.leaderElection()).thenReturn(leaderElectionConfig)
    }

    @Test
    fun `follower always returns UP`() {
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(false)
        }
        val check = LeaderHealthCheck(leaderElection, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
        assertEquals("leader-election", response.name)
    }

    @Test
    fun `leader with fresh heartbeat returns UP`() {
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(true)
            whenever(it.lastHeartbeat).thenReturn(Instant.now())
        }
        val check = LeaderHealthCheck(leaderElection, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
    }

    @Test
    fun `leader with stale heartbeat returns DOWN`() {
        val staleTime = Instant.now().minus(Duration.ofSeconds(60))
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(true)
            whenever(it.lastHeartbeat).thenReturn(staleTime)
        }
        val check = LeaderHealthCheck(leaderElection, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        assertEquals("leader-election", response.name)
        assertEquals(
            true,
            response.data.isPresent,
            "DOWN response should include diagnostic data",
        )
    }

    @Test
    fun `leader at exact threshold boundary returns UP`() {
        // age = 44s, threshold = 45s -> UP
        val justUnder = Instant.now().minus(Duration.ofSeconds(44))
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(true)
            whenever(it.lastHeartbeat).thenReturn(justUnder)
        }
        val check = LeaderHealthCheck(leaderElection, config)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="LeaderHealthCheckTest" -Dsurefire.failIfNoSpecifiedTests=false`

Expected: Compilation failure — `LeaderHealthCheck` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/kotlin/leader/LeaderHealthCheck.kt`:

```kotlin
package com.workflow.leader

import com.workflow.config.FrameworkConfig
import jakarta.inject.Singleton
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import java.time.Duration
import java.time.Instant

@Liveness
@Singleton
class LeaderHealthCheck(
    private val leaderElection: LeaderElection,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        if (!leaderElection.isActive) {
            return HealthCheckResponse.up("leader-election")
        }

        val age = Duration.between(leaderElection.lastHeartbeat, Instant.now())
        val threshold = config.leaderElection().healthThreshold()

        return if (age < threshold) {
            HealthCheckResponse.up("leader-election")
        } else {
            HealthCheckResponse.named("leader-election")
                .down()
                .withData("heartbeat_age_seconds", age.seconds)
                .withData("threshold_seconds", threshold.seconds)
                .build()
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="LeaderHealthCheckTest"`

Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/leader/LeaderHealthCheck.kt src/test/kotlin/leader/LeaderHealthCheckTest.kt
git commit -m "feat: add LeaderHealthCheck liveness probe with configurable threshold"
```

---

### Task 8: Full test suite verification

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`

Expected: BUILD SUCCESS — all existing and new tests pass.

- [ ] **Step 2: Run coverage check**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`

Expected: Coverage thresholds met.

- [ ] **Step 3: Verify no compilation warnings**

Scan the build output for any deprecation warnings or unused imports introduced by the changes.
