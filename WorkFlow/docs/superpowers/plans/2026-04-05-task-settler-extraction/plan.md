# Extract TaskSettler: Unify Retry/Settlement Logic

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a `TaskSettler` service that centralises the retry-or-fail decision and `PhaseGate.onTaskCompleted()` settlement. Both `WorkerLoop` and `TriggerLoop` delegate here so the business rule lives in one place.

**Motivation:** Code review identified that the retry logic (`retryCount < maxRetries` -> `resetForRetry()`, else -> `FAILED` via `phaseGate`) is duplicated across `TriggerLoop.handleTriggerFailure()` and `WorkerLoop.handleTaskFailure()`. This is a business rule that should live in one place to avoid divergence bugs.

**What NOT to extract:** The coroutine lifecycle (start/shutdown) and metrics initialisation are intentionally different between the two loops (timer-based sweep vs flow pipeline) and should remain separate.

**Tech Stack:** Kotlin, Quarkus CDI, Mockito

---

## New Type

```kotlin
// src/main/kotlin/worker/usecase/service/TaskSettler.kt

sealed interface RetryOutcome {
    data object Retried : RetryOutcome
    data object Failed : RetryOutcome
}

@ApplicationScoped
class TaskSettler(
    private val taskRepo: TaskRepository,
    private val phaseGate: PhaseGate,
) {
    suspend fun settle(
        taskId: String, workflowId: String, sequenceNumber: Int,
        status: TaskStatus, resultJson: String?,
        claimedBy: String? = null, claimedAt: Instant? = null,
    )

    suspend fun retryOrFail(
        taskId: String, workflowId: String, sequenceNumber: Int,
        retryCount: Int, maxRetries: Int,
        claimedBy: String? = null, claimedAt: Instant? = null,
    ): RetryOutcome
}
```

**Error contract:** Both methods propagate exceptions (CancellationException-safe via `suspendCatching`). Callers are responsible for their own error boundaries. `retryOrFail` internally catches `resetForRetry` failures and falls through to settling as FAILED — but if `phaseGate.onTaskCompleted` itself throws, that propagates to the caller.

---

## Pre-existing Files (already created)

`TaskSettler.kt` was already written at `src/main/kotlin/worker/usecase/service/TaskSettler.kt` before this spec. Verify it matches the contract above before proceeding.

---

### Task 1: Create TaskSettlerTest

**Files:**
- Create: `src/test/kotlin/worker/usecase/service/TaskSettlerTest.kt`

- [ ] **Step 1: Write tests for `settle()` delegation**

```kotlin
@Test
fun `settle delegates to phaseGate onTaskCompleted`() = runTest {
    settler.settle("t-1", "wf-1", 1, TaskStatus.COMPLETED, """{"ok":true}""", "worker-1", someInstant)
    verify(phaseGate).onTaskCompleted("t-1", "wf-1", 1, TaskStatus.COMPLETED, """{"ok":true}""", "worker-1", someInstant)
}

@Test
fun `settle with null claimedBy and claimedAt defaults`() = runTest {
    settler.settle("t-1", "wf-1", 1, TaskStatus.TIMED_OUT, null)
    verify(phaseGate).onTaskCompleted("t-1", "wf-1", 1, TaskStatus.TIMED_OUT, null, null, null)
}

@Test
fun `settle propagates phaseGate exception`() = runTest {
    whenever(phaseGate.onTaskCompleted(...)).thenThrow(RuntimeException("db error"))
    assertThrows<RuntimeException> { settler.settle(...) }
}
```

- [ ] **Step 2: Write tests for `retryOrFail()` — retry path**

```kotlin
@Test
fun `retryOrFail with retries remaining - resets and returns Retried`() = runTest {
    val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 3)
    verify(taskRepo).resetForRetry("t-1", 1)
    verifyNoInteractions(phaseGate)
    assertEquals(RetryOutcome.Retried, outcome)
}

@Test
fun `retryOrFail at boundary (retryCount = maxRetries - 1) - still retries`() = runTest {
    val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 2, maxRetries = 3)
    verify(taskRepo).resetForRetry("t-1", 3)
    assertEquals(RetryOutcome.Retried, outcome)
}
```

- [ ] **Step 3: Write tests for `retryOrFail()` — exhausted path**

```kotlin
@Test
fun `retryOrFail with retries exhausted - settles FAILED and returns Failed`() = runTest {
    val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 3, maxRetries = 3)
    verify(taskRepo, never()).resetForRetry(any(), any())
    verify(phaseGate).onTaskCompleted("t-1", "wf-1", 1, TaskStatus.FAILED, null, null, null)
    assertEquals(RetryOutcome.Failed, outcome)
}

@Test
fun `retryOrFail with zero maxRetries - settles FAILED immediately`() = runTest {
    val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 0)
    verify(taskRepo, never()).resetForRetry(any(), any())
    assertEquals(RetryOutcome.Failed, outcome)
}
```

- [ ] **Step 4: Write tests for `retryOrFail()` — resetForRetry failure fallback**

```kotlin
@Test
fun `retryOrFail when resetForRetry throws - falls through to FAILED`() = runTest {
    whenever(taskRepo.resetForRetry("t-1", 1)).thenThrow(RuntimeException("DB error"))
    val outcome = settler.retryOrFail("t-1", "wf-1", 1, retryCount = 0, maxRetries = 3)
    verify(taskRepo).resetForRetry("t-1", 1)
    verify(phaseGate).onTaskCompleted("t-1", "wf-1", 1, TaskStatus.FAILED, null, null, null)
    assertEquals(RetryOutcome.Failed, outcome)
}

@Test
fun `retryOrFail when resetForRetry throws AND phaseGate throws - propagates phaseGate exception`() = runTest {
    whenever(taskRepo.resetForRetry("t-1", 1)).thenThrow(RuntimeException("DB error"))
    whenever(phaseGate.onTaskCompleted(...)).thenThrow(RuntimeException("phaseGate error"))
    assertThrows<RuntimeException> { settler.retryOrFail("t-1", "wf-1", 1, 0, 3) }
}
```

- [ ] **Step 5: Write tests for `retryOrFail()` with claimedBy/claimedAt passthrough**

```kotlin
@Test
fun `retryOrFail passes claimedBy and claimedAt to settle on failure`() = runTest {
    val instant = Instant.now()
    settler.retryOrFail("t-1", "wf-1", 1, 3, 3, "worker-1", instant)
    verify(phaseGate).onTaskCompleted("t-1", "wf-1", 1, TaskStatus.FAILED, null, "worker-1", instant)
}
```

---

### Task 2: Refactor TriggerLoop to use TaskSettler

**Files:**
- Modify: `src/main/kotlin/worker/usecase/service/trigger/TriggerLoop.kt`

**Constructor change:**
```diff
 class TriggerLoop(
     private val taskRepo: TaskRepository,
     private val driverBeans: Instance<TriggerDriver>,
-    private val phaseGate: PhaseGate,
+    private val taskSettler: TaskSettler,
     private val leaderGuard: LeaderGuard,
     private val meterRegistry: MeterRegistry,
     private val triggerLoopConfig: TriggerLoopConfig,
     private val shutdownConfig: ShutdownConfig,
 ) : ShutdownParticipant {
```

- [ ] **Step 1: Fix double-init — consolidate initialization into `start()` only**

Remove the initialization block from `onStart()`. Keep only the scope creation and `start()` call:

```kotlin
fun onStart(@Observes ev: StartupEvent) {
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO.limitedParallelism(1))
    start(scope)
}
```

`start()` already has the `!::drivers.isInitialized` guard — that becomes the single init path for both production and tests.

- [ ] **Step 2: Replace `settleResult` success path with `taskSettler.settle()`**

Before:
```kotlin
is TriggerResult.Succeeded -> {
    phaseGate.onTaskCompleted(
        taskId = result.taskId, workflowId = task.workflowId,
        sequenceNumber = task.sequenceNumber, status = TaskStatus.COMPLETED,
        resultJson = result.result, claimedBy = null, claimedAt = null,
    )
    settledCounter(triggerType, "succeeded").increment()
}
```

After:
```kotlin
is TriggerResult.Succeeded -> {
    taskSettler.settle(
        taskId = result.taskId, workflowId = task.workflowId,
        sequenceNumber = task.sequenceNumber, status = TaskStatus.COMPLETED,
        resultJson = result.result,
    )
    settledCounter(triggerType, "succeeded").increment()
}
```

- [ ] **Step 3: Replace `handleTriggerFailure` with `taskSettler.retryOrFail()` + metrics**

Delete the entire `handleTriggerFailure` method. In `settleResult`, replace the `Failed` branch:

```kotlin
is TriggerResult.Failed -> {
    val outcome = taskSettler.retryOrFail(
        taskId = result.taskId, workflowId = task.workflowId,
        sequenceNumber = task.sequenceNumber,
        retryCount = task.retryCount, maxRetries = task.maxRetries,
    )
    when (outcome) {
        RetryOutcome.Retried -> {
            settledCounter(triggerType, "retried").increment()
            log.info("Trigger task {} failed ({}), retrying ({}/{})",
                result.taskId, result.reason, task.retryCount + 1, task.maxRetries)
        }
        RetryOutcome.Failed -> {
            settledCounter(triggerType, "failed").increment()
            log.warn("Trigger task {} failed permanently ({})", result.taskId, result.reason)
        }
    }
}
```

- [ ] **Step 4: Replace `expireTask` phaseGate call with `taskSettler.settle()`**

Before:
```kotlin
phaseGate.onTaskCompleted(
    taskId = task.taskId, ..., status = TaskStatus.TIMED_OUT, ...
)
```

After:
```kotlin
taskSettler.settle(
    taskId = task.taskId, workflowId = task.workflowId,
    sequenceNumber = task.sequenceNumber, status = TaskStatus.TIMED_OUT,
    resultJson = null,
)
```

- [ ] **Step 5: Remove `phaseGate` import and field**

Clean up unused imports: `PhaseGate`, `TaskStatus` (if no longer directly referenced — but `TaskStatus.COMPLETED`/`TIMED_OUT` are still used in settle calls, so keep `TaskStatus`).

---

### Task 3: Refactor WorkerLoop to use TaskSettler

**Files:**
- Modify: `src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt`

**Constructor change:**
```diff
 class WorkerLoop(
     private val workerLoopConfig: WorkerLoopConfig,
     private val shutdownConfig: ShutdownConfig,
     private val taskRepo: TaskRepository,
     private val handlerRegistry: HandlerRegistry,
-    private val phaseGate: PhaseGate,
+    private val taskSettler: TaskSettler,
     private val meterRegistry: MeterRegistry,
     private val activityInputResolver: ActivityInputResolver,
     private val workflowRepo: WorkflowRepository,
     private val objectMapper: ObjectMapper,
     private val notifier: WorkerNotifier,
 ) : ShutdownParticipant {
```

- [ ] **Step 1: Replace success path in `executeAndReport` with `taskSettler.settle()`**

Before:
```kotlin
is HandlerResult.Completed -> {
    try {
        phaseGate.onTaskCompleted(
            taskId = task.id, ..., status = TaskStatus.COMPLETED,
            resultJson = result.result, claimedBy = task.claimedBy, claimedAt = task.claimedAt,
        )
    } catch (e: CancellationException) { throw e }
    catch (e: Exception) {
        handleTaskFailure(task, e)
    }
}
```

After:
```kotlin
is HandlerResult.Completed -> {
    try {
        taskSettler.settle(
            taskId = task.id, workflowId = task.workflowId,
            sequenceNumber = task.sequenceNumber, status = TaskStatus.COMPLETED,
            resultJson = result.result, claimedBy = task.claimedBy, claimedAt = task.claimedAt,
        )
    } catch (e: CancellationException) { throw e }
    catch (e: Exception) {
        log.error("Barrier failed for COMPLETED task {}, falling through to failure path", task.id, e)
        handleTaskFailure(task, e)
    }
}
```

- [ ] **Step 2: Replace `handleTaskFailure` body with `taskSettler.retryOrFail()`**

Before (retry decision + `reportTaskCompleted` fallback):
```kotlin
private suspend fun handleTaskFailure(task: Task, cause: Exception) {
    log.warn(...)
    if (task.retryCount < task.maxRetries) {
        suspendCatching { taskRepo.resetForRetry(task.id, task.retryCount + 1) }
            .onFailure { e ->
                log.error(...)
                reportTaskCompleted(task, TaskStatus.FAILED, null)
            }
    } else {
        reportTaskCompleted(task, TaskStatus.FAILED, null)
    }
}
```

After:
```kotlin
private suspend fun handleTaskFailure(task: Task, cause: Exception) {
    log.warn(
        "Task {} (handler={}) failed (retry {}/{}): {}",
        task.id, task.handlerKey, task.retryCount, task.maxRetries, cause.message, cause,
    )
    suspendCatching {
        taskSettler.retryOrFail(
            taskId = task.id, workflowId = task.workflowId,
            sequenceNumber = task.sequenceNumber,
            retryCount = task.retryCount, maxRetries = task.maxRetries,
            claimedBy = task.claimedBy, claimedAt = task.claimedAt,
        )
    }.onFailure { e ->
        log.error("Failed to handle task {} failure through settler", task.id, e)
    }
}
```

- [ ] **Step 3: Delete `reportTaskCompleted` method**

It is fully replaced by `taskSettler.settle()` / `taskSettler.retryOrFail()`.

- [ ] **Step 4: Remove `phaseGate` import and field**

Clean up: remove `PhaseGate` import. Keep `TaskStatus` (still used in `TaskStatus.COMPLETED`).

---

### Task 4: Update TriggerLoopTest

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/trigger/TriggerLoopTest.kt`

- [ ] **Step 1: Wire `TaskSettler` in test setup**

In `setUp()`, create a real `TaskSettler` backed by the existing mocks:

```kotlin
private lateinit var taskSettler: TaskSettler

@BeforeEach
fun setUp() {
    // ... existing mock setup ...
    taskSettler = TaskSettler(taskRepo, phaseGate)

    triggerLoop = TriggerLoop(
        taskRepo, driverBeans, taskSettler, leaderGuard,
        meterRegistry, config, shutdownConfig,
    )
    initLoop(triggerLoop)
}
```

All existing `verify(phaseGate)` and `verify(taskRepo).resetForRetry()` assertions remain valid because the real `TaskSettler` delegates to the same mocks.

- [ ] **Step 2: Update multi-driver tests**

Any test that constructs a `TriggerLoop` directly (e.g., `multiple tasks grouped by type`, `driver start() throws`) must also pass `taskSettler` instead of `phaseGate`.

- [ ] **Step 3: Verify all tests pass**

No behavioural changes expected. Existing assertions verify the same end-to-end path through the mocked `phaseGate`/`taskRepo`.

---

### Task 5: Update WorkerLoopTest

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/execution/WorkerLoopTest.kt`

- [ ] **Step 1: Wire `TaskSettler` in test setup**

```kotlin
private lateinit var taskSettler: TaskSettler

@BeforeEach
fun setup() {
    // ... existing mock setup ...
    taskSettler = TaskSettler(taskRepo, phaseGate)

    workerLoop = WorkerLoop(
        workerConfig, shutdownConfig, taskRepo, handlerRegistry,
        taskSettler, meterRegistry, activityInputResolver,
        workflowRepo, objectMapper, notifier,
    )
}
```

- [ ] **Step 2: Verify all tests pass**

Same principle: real `TaskSettler` delegates to the same mocked `phaseGate`/`taskRepo`, so all `verify(phaseGate).onTaskCompleted(...)` and `verify(taskRepo).resetForRetry(...)` assertions still work.

---

### Task 6: Run full test suite

- [ ] **Step 1:** Run `mvn test` — all tests green.
- [ ] **Step 2:** Run coverage check: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`.

---

## Summary of Changes

| File | Action |
|------|--------|
| `worker/usecase/service/TaskSettler.kt` | **Already created** — verify matches spec |
| `worker/usecase/service/trigger/TriggerLoop.kt` | Replace `phaseGate` with `taskSettler`, delete `handleTriggerFailure`, fix double-init |
| `worker/usecase/service/execution/WorkerLoop.kt` | Replace `phaseGate` with `taskSettler`, delete `reportTaskCompleted`, simplify `handleTaskFailure` |
| `test/.../TaskSettlerTest.kt` | **New** — isolated retry logic tests |
| `test/.../TriggerLoopTest.kt` | Wire `TaskSettler(mockTaskRepo, mockPhaseGate)` into setup |
| `test/.../WorkerLoopTest.kt` | Wire `TaskSettler(mockTaskRepo, mockPhaseGate)` into setup |

**Lines removed (est):** ~60 (duplicated retry logic + `reportTaskCompleted`)
**Lines added (est):** ~90 (`TaskSettler` class already exists + `TaskSettlerTest`)
**Net complexity:** Reduced — business rule in one place, loops focus on their execution model.
