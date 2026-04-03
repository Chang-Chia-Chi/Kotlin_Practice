# Trigger P2: WorkerLoop Defer Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the `HandlerResult.Defer` path into `WorkerLoop`, migrate `TransitionHandler.execute()` return type from `HandlerOutput` to `HandlerResult`, update `MeteredTransitionHandler`, and migrate all existing handlers.

**Architecture:** `WorkerLoop.processTask()` gets a `when` branch on `HandlerResult`. The `Defer` branch calls `taskRepo.defer()`. Existing handlers return `HandlerResult.Completed` instead of `HandlerOutput`. `HandlerOutput` is removed.

**Tech Stack:** Kotlin, Mockito

**Depends on:** P1 (foundation types) must be complete.

---

### Task 1: Change `TransitionHandler.execute()` return type

**Files:**
- Modify: `src/main/kotlin/worker/usecase/port/inbound/execution/TransitionHandler.kt`
- Remove: `HandlerOutput` class from `TransitionHandler.kt` (it's in the same file)

- [ ] **Step 1: Update TransitionHandler interface**

In `src/main/kotlin/worker/usecase/port/inbound/execution/TransitionHandler.kt`:

Change the return type of `execute()` from `HandlerOutput` to `HandlerResult`:

```kotlin
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult

interface TransitionHandler {
    fun key(): String = this::class.simpleName!!
    suspend fun execute(input: HandlerInput): HandlerResult
}
```

Keep `HandlerInput` as-is. Remove the `HandlerOutput` data class. Keep `HandlerInput` data class in this file.

- [ ] **Step 2: Attempt to compile — expect failures**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: Compilation errors in all handlers and WorkerLoop that reference `HandlerOutput`.

- [ ] **Step 3: Commit**

```
refactor: change TransitionHandler.execute() return type to HandlerResult
```

Note: this is an intentionally breaking commit. The next tasks fix all callers.

---

### Task 2: Update `MeteredTransitionHandler`

**Files:**
- Modify: `src/main/kotlin/worker/usecase/service/execution/MeteredTransitionHandler.kt`
- Modify: `src/test/kotlin/worker/usecase/service/execution/MeteredTransitionHandlerTest.kt`

- [ ] **Step 1: Update MeteredTransitionHandler**

In `src/main/kotlin/worker/usecase/service/execution/MeteredTransitionHandler.kt`:

Change the return type and import:

```kotlin
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult

class MeteredTransitionHandler(
    private val delegate: TransitionHandler,
    private val handlerKey: String,
    private val meterRegistry: MeterRegistry,
) : TransitionHandler {

    override suspend fun execute(input: HandlerInput): HandlerResult {
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

Remove the `HandlerOutput` import.

- [ ] **Step 2: Update MeteredTransitionHandlerTest**

In `MeteredTransitionHandlerTest.kt`, replace all `HandlerOutput(...)` with `HandlerResult.Completed(...)`. Update imports: remove `HandlerOutput`, add `HandlerResult`.

- [ ] **Step 3: Run MeteredTransitionHandlerTest**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="MeteredTransitionHandlerTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 4: Commit**

```
refactor: update MeteredTransitionHandler to use HandlerResult
```

---

### Task 3: Migrate existing handlers to `HandlerResult.Completed`

**Files:**
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchSimulationHandler.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchScatterHandler.kt`
- Modify: `src/main/kotlin/dispatch/usecase/service/handler/DispatchJoinHandler.kt`
- Modify: `src/test/kotlin/dispatch/usecase/service/handler/DispatchHandlersTest.kt`

- [ ] **Step 1: Update DispatchSimulationHandler**

Replace `HandlerOutput(...)` with `HandlerResult.Completed(...)`. Update imports.

```kotlin
return HandlerResult.Completed(
    objectMapper.writeValueAsString(
        mapOf("configId" to configId, "batchToken" to batchToken),
    ),
)
```

- [ ] **Step 2: Update DispatchScatterHandler**

Same pattern — replace `HandlerOutput(...)` with `HandlerResult.Completed(...)`.

- [ ] **Step 3: Update DispatchJoinHandler**

Same pattern — replace `HandlerOutput(...)` with `HandlerResult.Completed(...)`.

- [ ] **Step 4: Update DispatchHandlersTest**

Replace all `HandlerOutput(...)` references with `HandlerResult.Completed(...)`. Update imports.

- [ ] **Step 5: Run handler tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchHandlersTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 6: Commit**

```
refactor: migrate all existing handlers to HandlerResult.Completed
```

---

### Task 4: Wire defer path into `WorkerLoop.processTask()`

**Files:**
- Modify: `src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt`

- [ ] **Step 1: Update processTask()**

In `WorkerLoop.processTask()`, replace the direct `phaseGate.onTaskCompleted()` call after `handler.execute(input)` with a `when` branch:

```kotlin
val result = handler.execute(input)

when (result) {
    is HandlerResult.Completed -> {
        try {
            phaseGate.onTaskCompleted(
                taskId = task.id,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                status = TaskStatus.COMPLETED,
                resultJson = result.result,
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
    is HandlerResult.Defer -> {
        val deferred = taskRepo.defer(
            taskId = task.id,
            triggerType = result.triggerType,
            triggerMeta = result.triggerMeta,
        )
        if (deferred) {
            log.info("Task {} deferred to trigger type={}", task.id, result.triggerType)
        } else {
            log.warn("Task {} defer failed (status was not PROCESSING), treating as failure", task.id)
            handleTaskFailure(task, IllegalStateException("Defer failed: task not in PROCESSING state"))
        }
    }
}
```

Add `import com.workflow.worker.usecase.port.inbound.execution.HandlerResult`.
Remove `import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput`.

- [ ] **Step 2: Run compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: PASS — all callers now use `HandlerResult`.

- [ ] **Step 3: Commit**

```
feat: wire HandlerResult.Defer path into WorkerLoop.processTask()
```

---

### Task 5: Test defer path in WorkerLoop

**Files:**
- Modify: `src/test/kotlin/worker/usecase/service/execution/WorkerLoopTest.kt`

- [ ] **Step 1: Update all existing test mocks to use HandlerResult.Completed**

Throughout `WorkerLoopTest.kt`, replace all `HandlerOutput(...)` with `HandlerResult.Completed(...)`. Update imports.

- [ ] **Step 2: Write test for defer path**

Add a new test in `WorkerLoopTest.kt`:

```kotlin
@Test
fun `handler returning Defer calls taskRepo defer and frees worker slot`() = runTest {
    val task = makeTask()
    taskRepo.stub { onBlocking { claimNext(any(), any(), any()) } doReturn listOf(task) }

    val deferHandler = object : TransitionHandler {
        override fun key(): String = "defer-handler"
        override suspend fun execute(input: HandlerInput): HandlerResult =
            HandlerResult.Defer(triggerType = "k8s-job", triggerMeta = """{"jobName":"j1","namespace":"ns"}""")
    }
    handlerRegistry.register("defer-handler", deferHandler)
    taskRepo.stub { onBlocking { defer(any(), any(), any()) } doReturn true }

    val job = workerLoop.start(this)
    advanceTimeBy(pollInterval.toMillis() + 100)

    verify(taskRepo).defer(eq(task.id), eq("k8s-job"), eq("""{"jobName":"j1","namespace":"ns"}"""))
    verify(phaseGate, never()).onTaskCompleted(any(), any(), any(), any(), any(), any(), any())

    workerLoop.shutdown()
}
```

- [ ] **Step 3: Write test for defer failure (not PROCESSING)**

```kotlin
@Test
fun `handler returning Defer when task not PROCESSING falls through to failure`() = runTest {
    val task = makeTask()
    taskRepo.stub { onBlocking { claimNext(any(), any(), any()) } doReturn listOf(task) }

    val deferHandler = object : TransitionHandler {
        override fun key(): String = "defer-handler"
        override suspend fun execute(input: HandlerInput): HandlerResult =
            HandlerResult.Defer(triggerType = "sql-exec", triggerMeta = "{}")
    }
    handlerRegistry.register("defer-handler", deferHandler)
    taskRepo.stub { onBlocking { defer(any(), any(), any()) } doReturn false }

    val job = workerLoop.start(this)
    advanceTimeBy(pollInterval.toMillis() + 100)

    verify(taskRepo).defer(eq(task.id), any(), any())
    // Falls through to handleTaskFailure which calls resetForRetry or reportTaskFailed
    verify(taskRepo).resetForRetry(eq(task.id), any())

    workerLoop.shutdown()
}
```

- [ ] **Step 4: Run WorkerLoopTest**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: PASS — no regressions.

- [ ] **Step 6: Commit**

```
test: add WorkerLoop defer path tests and migrate existing tests to HandlerResult
```

---

### Task 6: Delete `HandlerOutput`

**Files:**
- Modify: `src/main/kotlin/worker/usecase/port/inbound/execution/TransitionHandler.kt`

- [ ] **Step 1: Verify no remaining references to HandlerOutput**

Search the entire codebase for `HandlerOutput`. If any remain, fix them first.

- [ ] **Step 2: Remove HandlerOutput data class**

If `HandlerOutput` is still defined in `TransitionHandler.kt`, remove it. It should already be gone from Task 1 of this plan, but verify.

- [ ] **Step 3: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: PASS

- [ ] **Step 4: Commit**

```
refactor: remove unused HandlerOutput class
```
