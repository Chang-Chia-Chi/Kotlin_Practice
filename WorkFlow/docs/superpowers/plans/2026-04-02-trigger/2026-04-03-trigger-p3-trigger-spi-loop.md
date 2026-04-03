# Trigger P3: TriggerDriver SPI & TriggerLoop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the `TriggerDriver` SPI interface, `TriggerResult` sealed interface, `TriggerLoopConfig`, and the `TriggerLoop` component with leader gating, sweep logic, settlement, deadline enforcement, shutdown, and metrics.

**Architecture:** `TriggerLoop` is a leader-gated component that periodically loads DEFERRED tasks, dispatches them to `TriggerDriver` instances (CDI beans), polls for results, and settles completed/failed tasks through the phase gate. Follows the same patterns as `WorkflowWatchdog` (leader gating) and `WorkerLoop` (shutdown participant).

**Tech Stack:** Kotlin, Quarkus CDI, Micrometer, Mockito

**Depends on:** P1 (foundation types) must be complete.

---

### Task 1: Create `TriggerResult` sealed interface

**Files:**
- Create: `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerResult.kt`

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerResultTest.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TriggerResultTest {

    @Test
    fun `Succeeded carries taskId and result`() {
        val r: TriggerResult = TriggerResult.Succeeded("t-1", """{"key":"value"}""")
        assertEquals("t-1", r.taskId)
        assertTrue(r is TriggerResult.Succeeded)
        assertEquals("""{"key":"value"}""", (r as TriggerResult.Succeeded).result)
    }

    @Test
    fun `Succeeded with null result`() {
        val r = TriggerResult.Succeeded("t-2", null)
        assertNull(r.result)
    }

    @Test
    fun `Failed carries taskId and reason`() {
        val r: TriggerResult = TriggerResult.Failed("t-3", "Job exited with code 1")
        assertEquals("t-3", r.taskId)
        assertEquals("Job exited with code 1", (r as TriggerResult.Failed).reason)
    }

    @Test
    fun `exhaustive when on TriggerResult`() {
        val results: List<TriggerResult> = listOf(
            TriggerResult.Succeeded("t-1", "ok"),
            TriggerResult.Failed("t-2", "err"),
        )
        for (r in results) {
            val label = when (r) {
                is TriggerResult.Succeeded -> "succeeded"
                is TriggerResult.Failed -> "failed"
            }
            assertTrue(label.isNotEmpty())
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerResultTest" -pl WorkFlow`
Expected: FAIL — `TriggerResult` does not exist.

- [ ] **Step 3: Create TriggerResult.kt**

Create `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerResult.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

sealed interface TriggerResult {
    val taskId: String
    data class Succeeded(override val taskId: String, val result: String?) : TriggerResult
    data class Failed(override val taskId: String, val reason: String) : TriggerResult
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerResultTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: add TriggerResult sealed interface
```

---

### Task 2: Create `TriggerDriver` SPI interface

**Files:**
- Create: `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerDriver.kt`

- [ ] **Step 1: Create TriggerDriver.kt**

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

/**
 * SPI for monitoring external task completion.
 *
 * ## Lifecycle contract
 * - [start] is called each sweep cycle with the **full** set of DEFERRED tasks
 *   for this driver's [type]. The driver diffs internally — add new tasks,
 *   remove already-resolved ones.
 * - [poll] returns results since last call. Must be non-blocking.
 * - [cancel] is best-effort cleanup (e.g., delete K8s Job, cancel SQL query).
 * - [close] is called on shutdown for resource cleanup.
 */
interface TriggerDriver {
    fun type(): String
    suspend fun start(tasks: List<DeferredTaskRef>)
    suspend fun poll(): List<TriggerResult>
    suspend fun cancel(taskId: String)
    suspend fun close()
}
```

- [ ] **Step 2: Run compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
feat: add TriggerDriver SPI interface
```

---

### Task 3: Create `TriggerLoopConfig`

**Files:**
- Create: `src/main/kotlin/worker/config/TriggerLoopConfig.kt`
- Modify: `src/main/resources/application.properties`
- Modify: `src/test/resources/application.properties`

- [ ] **Step 1: Create TriggerLoopConfig.kt**

```kotlin
package com.workflow.worker.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

@ConfigMapping(prefix = "framework.trigger")
interface TriggerLoopConfig {
    @WithDefault("PT5S")
    fun sweepInterval(): Duration

    @WithDefault("5")
    fun sqlMaxConcurrent(): Int
}
```

- [ ] **Step 2: Add config to application.properties**

Add to `src/main/resources/application.properties`:

```properties
# =============================================================================
# Trigger Loop
# =============================================================================
framework.trigger.sweep-interval=PT5S
framework.trigger.sql-max-concurrent=5
```

- [ ] **Step 3: Add config to test application.properties**

Add to `src/test/resources/application.properties`:

```properties
framework.trigger.sweep-interval=PT5S
framework.trigger.sql-max-concurrent=2
```

- [ ] **Step 4: Run config validation test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="FrameworkConfigTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: add TriggerLoopConfig with sweep interval and SQL concurrency settings
```

---

### Task 4: Create `TriggerLoop` component

**Files:**
- Create: `src/main/kotlin/worker/usecase/service/trigger/TriggerLoop.kt`

- [ ] **Step 1: Create TriggerLoop.kt**

```kotlin
package com.workflow.worker.usecase.service.trigger

import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.infrastructure.shutdown.ShutdownParticipant
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

const val SHUTDOWN_ORDER_TRIGGER = 5

@ApplicationScoped
class TriggerLoop(
    private val taskRepo: TaskRepository,
    private val driverBeans: Instance<TriggerDriver>,
    private val phaseGate: DefaultPhaseGate,
    private val leaderGuard: LeaderGuard,
    private val meterRegistry: MeterRegistry,
    private val triggerLoopConfig: TriggerLoopConfig,
    private val shutdownConfig: ShutdownConfig,
) : ShutdownParticipant {

    private val log = LoggerFactory.getLogger(TriggerLoop::class.java)
    private val _running = AtomicBoolean(false)

    @Volatile
    private var activeJob: Job? = null

    private lateinit var drivers: Map<String, TriggerDriver>
    private lateinit var pollCounter: Counter
    private lateinit var sweepTimer: Timer

    private fun settledCounter(type: String, outcome: String): Counter =
        meterRegistry.counter("trigger_settled_total", "type", type, "outcome", outcome)

    fun onStart(@Observes ev: StartupEvent) {
        drivers = driverBeans.associateBy { it.type() }
        pollCounter = meterRegistry.counter("trigger_poll_total")
        sweepTimer = Timer.builder("trigger_sweep_duration_seconds")
            .publishPercentileHistogram()
            .register(meterRegistry)

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO.limitedParallelism(1))
        start(scope)
    }

    fun start(scope: CoroutineScope): Job {
        if (drivers.isEmpty()) {
            if (!::drivers.isInitialized) {
                drivers = driverBeans.associateBy { it.type() }
            }
        }
        if (!::pollCounter.isInitialized) {
            pollCounter = meterRegistry.counter("trigger_poll_total")
            sweepTimer = Timer.builder("trigger_sweep_duration_seconds")
                .publishPercentileHistogram()
                .register(meterRegistry)
        }

        _running.set(true)
        val interval = triggerLoopConfig.sweepInterval()

        val job = scope.launch {
            while (isActive && _running.get()) {
                try {
                    sweep()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.error("Trigger sweep failed", e)
                }
                delay(interval.toMillis())
            }
        }
        activeJob = job
        log.info("TriggerLoop started: sweepInterval={}, drivers={}", interval, drivers.keys)
        return job
    }

    internal suspend fun sweep() {
        if (!leaderGuard.isLeader) return

        val sample = Timer.start(meterRegistry)
        pollCounter.increment()

        val deferred = taskRepo.findDeferred()

        meterRegistry.gauge("trigger_deferred_tasks", deferred.size.toDouble())

        val grouped = deferred.groupBy { it.triggerType }

        // Dispatch to drivers
        for ((type, tasks) in grouped) {
            val driver = drivers[type]
            if (driver == null) {
                log.warn("No TriggerDriver registered for type '{}', {} tasks orphaned", type, tasks.size)
                continue
            }
            try {
                driver.start(tasks)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("TriggerDriver '{}' start() failed", type, e)
            }
        }

        // Poll all drivers for results
        for ((type, driver) in drivers) {
            val results = try {
                driver.poll()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("TriggerDriver '{}' poll() failed", type, e)
                continue
            }
            for (result in results) {
                settleResult(type, result)
            }
        }

        // Check deadlines
        val now = Instant.now()
        for (task in deferred) {
            if (task.deadlineAt != null && now.isAfter(task.deadlineAt)) {
                expireTask(task)
            }
        }

        sample.stop(sweepTimer)
    }

    private suspend fun settleResult(triggerType: String, result: TriggerResult) {
        try {
            when (result) {
                is TriggerResult.Succeeded -> {
                    phaseGate.onTaskCompleted(
                        taskId = result.taskId,
                        status = TaskStatus.COMPLETED,
                        resultJson = result.result,
                    )
                    settledCounter(triggerType, "succeeded").increment()
                    log.info("Trigger settled task {} as COMPLETED (type={})", result.taskId, triggerType)
                }
                is TriggerResult.Failed -> {
                    handleTriggerFailure(result.taskId, triggerType, result.reason)
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Failed to settle trigger result for task {}", result.taskId, e)
        }
    }

    private suspend fun handleTriggerFailure(taskId: String, triggerType: String, reason: String) {
        val tasks = taskRepo.findDeferred().filter { it.taskId == taskId }
        val task = tasks.firstOrNull()

        if (task != null && task.retryCount < task.maxRetries) {
            taskRepo.resetForRetry(taskId, task.retryCount + 1)
            settledCounter(triggerType, "retried").increment()
            log.info("Trigger task {} failed ({}), retrying ({}/{})", taskId, reason, task.retryCount + 1, task.maxRetries)
        } else {
            phaseGate.onTaskCompleted(
                taskId = taskId,
                status = TaskStatus.FAILED,
                resultJson = null,
            )
            settledCounter(triggerType, "failed").increment()
            log.warn("Trigger task {} failed permanently ({})", taskId, reason)
        }
    }

    private suspend fun expireTask(task: DeferredTaskRef) {
        try {
            val driver = drivers[task.triggerType]
            if (driver != null) {
                try {
                    driver.cancel(task.taskId)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.warn("Failed to cancel trigger for expired task {}", task.taskId, e)
                }
            }
            phaseGate.onTaskCompleted(
                taskId = task.taskId,
                status = TaskStatus.TIMED_OUT,
                resultJson = null,
            )
            log.warn("DEFERRED task {} expired (deadline={})", task.taskId, task.deadlineAt)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Failed to expire DEFERRED task {}", task.taskId, e)
        }
    }

    override val shutdownOrder: Int = SHUTDOWN_ORDER_TRIGGER

    override val shutdownTimeout: Duration get() = shutdownConfig.globalTimeout()

    override suspend fun shutdown() {
        log.info("TriggerLoop shutting down")
        _running.set(false)
        withTimeoutOrNull(shutdownTimeout.toMillis()) {
            activeJob?.join()
        }
        activeJob?.cancelAndJoin()
        for ((type, driver) in drivers) {
            try {
                driver.close()
            } catch (e: Exception) {
                log.warn("TriggerDriver '{}' close() failed", type, e)
            }
        }
        log.info("TriggerLoop shutdown complete")
    }
}
```

Note: The `phaseGate.onTaskCompleted()` call in `settleResult` and `expireTask` uses a simplified signature. Check the actual `DefaultPhaseGate.onTaskCompleted()` signature and pass all required parameters. If `workflowId` and `sequenceNumber` are needed, load them from the deferred task list or the DB. The `findDeferred()` returns `DeferredTaskRef` which has `workflowId` and `sequenceNumber` — maintain a local map during the sweep for lookups.

**Revised approach:** maintain a `Map<String, DeferredTaskRef>` during sweep to look up task metadata when settling:

In `sweep()`, after loading `deferred`:
```kotlin
val taskIndex = deferred.associateBy { it.taskId }
```

Pass `taskIndex` to `settleResult()` and `expireTask()`, and use it to get `workflowId`/`sequenceNumber`.

- [ ] **Step 2: Run compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
feat: add TriggerLoop component with sweep, settlement, and shutdown
```

---

### Task 5: Test TriggerLoop

**Files:**
- Create: `src/test/kotlin/worker/usecase/service/trigger/TriggerLoopTest.kt`

- [ ] **Step 1: Write TriggerLoopTest with mock driver**

```kotlin
package com.workflow.worker.usecase.service.trigger

import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

@OptIn(ExperimentalCoroutinesApi::class)
class TriggerLoopTest {

    private lateinit var taskRepo: TaskRepository
    private lateinit var phaseGate: DefaultPhaseGate
    private lateinit var leaderGuard: LeaderGuard
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var config: TriggerLoopConfig
    private lateinit var shutdownConfig: ShutdownConfig
    private lateinit var mockDriver: TriggerDriver
    private lateinit var driverBeans: Instance<TriggerDriver>
    private lateinit var triggerLoop: TriggerLoop

    @BeforeEach
    fun setUp() {
        taskRepo = mock()
        phaseGate = mock()
        leaderGuard = mock { on { isLeader } doReturn true }
        meterRegistry = SimpleMeterRegistry()
        config = mock {
            on { sweepInterval() } doReturn Duration.ofSeconds(5)
            on { sqlMaxConcurrent() } doReturn 2
        }
        shutdownConfig = mock { on { globalTimeout() } doReturn Duration.ofSeconds(30) }
        mockDriver = mock { on { type() } doReturn "test-driver" }
        driverBeans = mock { on { iterator() } doReturn mutableListOf(mockDriver).iterator() }

        triggerLoop = TriggerLoop(taskRepo, driverBeans, phaseGate, leaderGuard, meterRegistry, config, shutdownConfig)
    }

    private fun makeDeferredRef(
        taskId: String = "t-1",
        triggerType: String = "test-driver",
        deadlineAt: Instant? = Instant.now().plusSeconds(3600),
        retryCount: Int = 0,
        maxRetries: Int = 3,
    ) = DeferredTaskRef(
        taskId = taskId,
        workflowId = "wf-1",
        sequenceNumber = 1,
        triggerType = triggerType,
        triggerMeta = "{}",
        deadlineAt = deadlineAt,
        retryCount = retryCount,
        maxRetries = maxRetries,
    )

    @Test
    fun `sweep dispatches DEFERRED tasks to matching driver`() = runTest {
        val ref = makeDeferredRef()
        taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
        mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

        triggerLoop.sweep()

        verify(mockDriver).start(eq(listOf(ref)))
    }

    @Test
    fun `sweep skips when not leader`() = runTest {
        whenever(leaderGuard.isLeader).thenReturn(false)
        taskRepo.stub { onBlocking { findDeferred() } doReturn emptyList() }

        triggerLoop.sweep()

        verify(taskRepo, never()).findDeferred()
    }

    @Test
    fun `Succeeded result settles task as COMPLETED`() = runTest {
        val ref = makeDeferredRef()
        taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
        mockDriver.stub { onBlocking { poll() } doReturn listOf(TriggerResult.Succeeded("t-1", """{"ok":true}""")) }

        triggerLoop.sweep()

        verify(phaseGate).onTaskCompleted(
            taskId = eq("t-1"),
            workflowId = eq("wf-1"),
            sequenceNumber = eq(1),
            status = eq(TaskStatus.COMPLETED),
            resultJson = eq("""{"ok":true}"""),
        )
    }

    @Test
    fun `Failed result with retries remaining calls resetForRetry`() = runTest {
        val ref = makeDeferredRef(retryCount = 0, maxRetries = 3)
        taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
        mockDriver.stub { onBlocking { poll() } doReturn listOf(TriggerResult.Failed("t-1", "Job failed")) }

        triggerLoop.sweep()

        verify(taskRepo).resetForRetry(eq("t-1"), eq(1))
        verify(phaseGate, never()).onTaskCompleted(any(), any(), any(), any(), any())
    }

    @Test
    fun `Failed result with retries exhausted settles as FAILED`() = runTest {
        val ref = makeDeferredRef(retryCount = 3, maxRetries = 3)
        taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
        mockDriver.stub { onBlocking { poll() } doReturn listOf(TriggerResult.Failed("t-1", "Job failed")) }

        triggerLoop.sweep()

        verify(phaseGate).onTaskCompleted(
            taskId = eq("t-1"),
            workflowId = eq("wf-1"),
            sequenceNumber = eq(1),
            status = eq(TaskStatus.FAILED),
            resultJson = eq(null),
        )
    }

    @Test
    fun `expired DEFERRED task is cancelled and timed out`() = runTest {
        val ref = makeDeferredRef(deadlineAt = Instant.now().minusSeconds(60))
        taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
        mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

        triggerLoop.sweep()

        verify(mockDriver).cancel(eq("t-1"))
        verify(phaseGate).onTaskCompleted(
            taskId = eq("t-1"),
            workflowId = eq("wf-1"),
            sequenceNumber = eq(1),
            status = eq(TaskStatus.TIMED_OUT),
            resultJson = eq(null),
        )
    }

    @Test
    fun `shutdown calls close on all drivers`() = runTest {
        triggerLoop.shutdown()
        verify(mockDriver).close()
    }
}
```

Note: Adjust `phaseGate.onTaskCompleted()` verify calls to match the actual method signature. If it requires additional parameters like `claimedBy`/`claimedAt`, pass `any()` for those.

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerLoopTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
test: add TriggerLoop unit tests for sweep, settlement, deadline, and shutdown
```
