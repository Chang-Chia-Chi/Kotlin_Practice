# Session 8: Structured Logging & Handler Contract — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add JSON structured logging with coroutine-safe MDC propagation and document the TransitionHandler idempotency contract.

**Architecture:** Layered MDC via `kotlinx-coroutines-slf4j` MDCContext — worker_id at poll level, task fields at processTask level. Quarkus `quarkus-logging-json` serializes MDC entries as top-level JSON fields automatically.

**Tech Stack:** Kotlin Coroutines, kotlinx-coroutines-slf4j (MDCContext), quarkus-logging-json, SLF4J MDC

**Spec:** `docs/superpowers/specs/2026-03-27-structured-logging-handler-contract-design.md`

**Maven:** `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `pom.xml` | Modify | Add `quarkus-logging-json` + `kotlinx-coroutines-slf4j` deps |
| `src/main/resources/application.properties` | Modify | JSON logging config |
| `src/test/resources/application.properties` | Modify | Disable JSON for test readability |
| `src/main/kotlin/worker/WorkerLoop.kt` | Modify | Layered MDC in `pollAndProcess` + `processTask`, fix `handleTaskFailure` |
| `src/main/kotlin/worker/TransitionHandler.kt` | Modify | KDoc on interface + data classes |
| `src/test/kotlin/worker/WorkerLoopTest.kt` | Modify | MDC propagation + isolation tests |

---

### Task 1: Add dependencies and logging configuration

**Files:**
- Modify: `pom.xml:112-116` (after `quarkus-micrometer-registry-prometheus`)
- Modify: `pom.xml:99-104` (after `kotlinx-coroutines-core`)
- Modify: `src/main/resources/application.properties`
- Modify: `src/test/resources/application.properties`

- [ ] **Step 1: Add `quarkus-logging-json` dependency to `pom.xml`**

Insert after the `quarkus-micrometer-registry-prometheus` dependency (line 116):

```xml
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-logging-json</artifactId>
        </dependency>
```

- [ ] **Step 2: Add `kotlinx-coroutines-slf4j` dependency to `pom.xml`**

Insert after the `kotlinx-coroutines-core` dependency (line 104):

```xml
        <dependency>
            <groupId>org.jetbrains.kotlinx</groupId>
            <artifactId>kotlinx-coroutines-slf4j</artifactId>
            <version>${kotlinx-coroutines.version}</version>
        </dependency>
```

- [ ] **Step 3: Add JSON logging config to main `application.properties`**

Append a new logging section at the end of the file:

```properties

# =============================================================================
# Logging
# =============================================================================
quarkus.log.console.json=true
quarkus.log.console.json.additional-field.service.value=workflow-engine
```

- [ ] **Step 4: Disable JSON logging in test `application.properties`**

Append at the end of the file:

```properties

# Keep plaintext logs for test readability
quarkus.log.console.json=false
```

- [ ] **Step 5: Verify project compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/resources/application.properties src/test/resources/application.properties
git commit -m "chore: add quarkus-logging-json and kotlinx-coroutines-slf4j dependencies

Enable JSON structured logging in production config. Disable in test
config for readability. Add coroutine-safe MDC propagation library."
```

---

### Task 2: Document TransitionHandler idempotency contract (R3.8)

**Files:**
- Modify: `src/main/kotlin/worker/TransitionHandler.kt`

- [ ] **Step 1: Add KDoc to `TransitionHandler`, `HandlerInput`, and `HandlerOutput`**

Replace the entire file content:

```kotlin
package com.workflow.worker

/**
 * Handler for task execution within the workflow engine.
 *
 * ## Delivery Guarantee
 *
 * The engine provides **at-least-once** delivery. A handler may be invoked
 * multiple times for the same logical task due to:
 * - Retry on failure (up to `maxRetries` per task)
 * - Stale task reclaim by the sweeper (visibility timeout expiry)
 *
 * ## Idempotency Requirement
 *
 * Handlers **must be idempotent**. Use [HandlerInput.taskId] as the
 * idempotency key when interacting with external systems. For example,
 * pass `taskId` as the idempotency key to payment APIs, message
 * deduplication headers, or database upsert conditions.
 *
 * ## Cancellation
 *
 * Long-running handlers should periodically check `isActive` or call
 * `yield()` to cooperate with graceful shutdown. On pod termination,
 * in-flight handlers receive [kotlinx.coroutines.CancellationException]
 * after the drain window expires. Tasks whose handlers are cancelled
 * remain in PROCESSING state and will be reclaimed by the sweeper.
 *
 * ## Shutdown Awareness
 *
 * Handlers can call [com.workflow.shutdown.isShuttingDown] from their
 * coroutine context to detect that the pod is draining. Use this to
 * skip optional work or checkpoint progress.
 */
interface TransitionHandler {
    suspend fun execute(input: HandlerInput): HandlerOutput
}

/**
 * Input provided to a [TransitionHandler] for task execution.
 *
 * @property taskId Unique task identifier — use as idempotency key for external calls.
 * @property workflowId Parent workflow identifier.
 * @property sequenceNumber Position in the workflow DAG.
 * @property payload JSON payload from the previous step's output (or initial workflow input).
 */
data class HandlerInput(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val payload: String?,
)

/**
 * Output returned by a [TransitionHandler] after task execution.
 *
 * @property result JSON output passed to the next step or stored as the final workflow result.
 *                  Return `null` if the handler produces no output.
 */
data class HandlerOutput(
    val result: String?,
)
```

- [ ] **Step 2: Verify project compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/worker/TransitionHandler.kt
git commit -m "docs: add idempotency contract KDoc to TransitionHandler

Document at-least-once delivery guarantee, idempotency requirement,
cancellation behavior, and shutdown awareness for handler authors."
```

---

### Task 3: Write failing MDC propagation tests

**Files:**
- Modify: `src/test/kotlin/worker/WorkerLoopTest.kt`

- [ ] **Step 1: Add MDC import to `WorkerLoopTest.kt`**

Add after the existing import block (after line 40, before `class WorkerLoopTest`):

```kotlin
import org.slf4j.MDC
```

- [ ] **Step 2: Add MDC propagation test section**

Add the following nested class inside `WorkerLoopTest`, after the `CancellationExceptionPropagation` section (before the closing `}` of `WorkerLoopTest`):

```kotlin
    // ── Q. MDC Context Propagation (R3.7) ────────────────────────────────

    @Nested
    inner class MdcContextPropagation {

        @Test
        fun `MDC contains worker and task fields during handler execution`() = runTest {
            val task = makeTask(
                id = "task-42",
                workflowId = "wf-7",
                handlerKey = "order.validate",
                retryCount = 1,
            )
            val capturedMdc = mutableMapOf<String, String?>()
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    capturedMdc["worker_id"] = MDC.get("worker_id")
                    capturedMdc["task_id"] = MDC.get("task_id")
                    capturedMdc["workflow_id"] = MDC.get("workflow_id")
                    capturedMdc["handler_key"] = MDC.get("handler_key")
                    capturedMdc["attempt"] = MDC.get("attempt")
                    return HandlerOutput(null)
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            assertEquals(workerId, capturedMdc["worker_id"])
            assertEquals("task-42", capturedMdc["task_id"])
            assertEquals("wf-7", capturedMdc["workflow_id"])
            assertEquals("order.validate", capturedMdc["handler_key"])
            assertEquals("1", capturedMdc["attempt"])
        }

        @Test
        fun `MDC task fields do not leak between sequential tasks`() = runTest {
            val task1 = makeTask(id = "t1", workflowId = "wf-1", handlerKey = "step.one")
            val task2 = makeTask(id = "t2", workflowId = "wf-2", handlerKey = "step.two")
            val capturedMdcTask2 = mutableMapOf<String, String?>()

            val handler1 = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    return HandlerOutput(null)
                }
            }
            val handler2 = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    capturedMdcTask2["task_id"] = MDC.get("task_id")
                    capturedMdcTask2["workflow_id"] = MDC.get("workflow_id")
                    capturedMdcTask2["handler_key"] = MDC.get("handler_key")
                    return HandlerOutput(null)
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task1))
                .thenReturn(listOf(task2))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("step.one")).thenReturn(handler1)
            whenever(handlerRegistry.resolve("step.two")).thenReturn(handler2)

            startAndAdvance(this, ticks = 4)

            assertEquals("t2", capturedMdcTask2["task_id"])
            assertEquals("wf-2", capturedMdcTask2["workflow_id"])
            assertEquals("step.two", capturedMdcTask2["handler_key"])
        }

        @Test
        fun `MDC context persists through failure handling path`() = runTest {
            val task = makeTask(
                id = "fail-task",
                workflowId = "wf-fail",
                handlerKey = "order.fail",
                retryCount = 2,
                maxRetries = 3,
            )
            var mdcDuringRetry = emptyMap<String, String?>()

            val handler = mock<TransitionHandler>()
            whenever(handler.execute(any())).thenThrow(RuntimeException("boom"))

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            doAnswer {
                mdcDuringRetry = mapOf(
                    "worker_id" to MDC.get("worker_id"),
                    "task_id" to MDC.get("task_id"),
                    "workflow_id" to MDC.get("workflow_id"),
                    "handler_key" to MDC.get("handler_key"),
                    "attempt" to MDC.get("attempt"),
                )
                Unit
            }.whenever(taskRepo).resetForRetry(eq("fail-task"), eq(3))

            startAndAdvance(this)

            assertEquals(workerId, mdcDuringRetry["worker_id"])
            assertEquals("fail-task", mdcDuringRetry["task_id"])
            assertEquals("wf-fail", mdcDuringRetry["workflow_id"])
            assertEquals("order.fail", mdcDuringRetry["handler_key"])
            assertEquals("2", mdcDuringRetry["attempt"])
        }
    }
```

- [ ] **Step 3: Run the new tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest$MdcContextPropagation" -pl .`
Expected: 3 tests FAIL (MDC.get returns null because WorkerLoop doesn't set MDC yet)

---

### Task 4: Implement layered MDC in WorkerLoop and fix handleTaskFailure logging (R3.7)

**Files:**
- Modify: `src/main/kotlin/worker/WorkerLoop.kt`

- [ ] **Step 1: Add MDC imports to `WorkerLoop.kt`**

Add after the existing `import org.slf4j.LoggerFactory` (line 26):

```kotlin
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.MDC
```

- [ ] **Step 2: Wrap `pollAndProcess` body with worker-level MDCContext**

Replace the `pollAndProcess` method (lines 137-162) with:

```kotlin
    private suspend fun pollAndProcess(
        workerId: String,
        pollInterval: Duration,
        batchSize: Int,
    ) = withContext(MDCContext(mapOf("worker_id" to workerId))) {
        val tasks =
            try {
                taskRepo.claimNext(workerId, batchSize)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Failed to claim tasks", e)
                delay(pollInterval.toMillis())
                return@withContext
            }
        _lastPollTimestamp = Instant.now()

        if (tasks.isEmpty()) {
            delay(pollInterval.toMillis())
            return@withContext
        }

        for (task in tasks) {
            processTask(task)
        }
    }
```

- [ ] **Step 3: Wrap `processTask` body with task-level MDCContext**

Replace the `processTask` method (lines 164-200) with:

```kotlin
    private suspend fun processTask(task: Task) {
        val taskMdc = MDC.getCopyOfContextMap().orEmpty() + mapOf(
            "task_id" to task.id,
            "handler_key" to task.handlerKey,
            "workflow_id" to task.workflowId,
            "attempt" to task.retryCount.toString(),
        )
        withContext(MDCContext(taskMdc)) {
            _inFlightTasks.incrementAndGet()
            try {
                val handler = handlerRegistry.resolve(task.handlerKey)
                val input =
                    HandlerInput(
                        taskId = task.id,
                        workflowId = task.workflowId,
                        sequenceNumber = task.sequenceNumber,
                        payload = task.payloadJson,
                    )
                val output = handler.execute(input)

                try {
                    barrierService.onTaskCompleted(
                        taskId = task.id,
                        workflowId = task.workflowId,
                        sequenceNumber = task.sequenceNumber,
                        status = TaskStatus.COMPLETED,
                        resultJson = output.result,
                        claimedBy = task.claimedBy,
                        claimedAt = task.claimedAt,
                    )
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.error("Barrier failed for COMPLETED task {}, falling through to failure path", task.id, e)
                    handleTaskFailure(task, e)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                handleTaskFailure(task, e)
            } finally {
                _inFlightTasks.decrementAndGet()
            }
        }
    }
```

- [ ] **Step 4: Fix `handleTaskFailure` logging — add handler key and stack trace**

Replace the `log.warn` call in `handleTaskFailure` (lines 206-211):

```kotlin
        log.warn(
            "Task {} failed (retry {}/{}): {}",
            task.id,
            task.retryCount,
            task.maxRetries,
            cause.message,
        )
```

with:

```kotlin
        log.warn(
            "Task {} (handler={}) failed (retry {}/{}): {}",
            task.id,
            task.handlerKey,
            task.retryCount,
            task.maxRetries,
            cause.message,
            cause,
        )
```

- [ ] **Step 5: Run the MDC tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest$MdcContextPropagation" -pl .`
Expected: 3 tests PASS

- [ ] **Step 6: Run the full WorkerLoopTest suite to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest" -pl .`
Expected: All tests PASS (existing tests should not be affected by MDC wrapping)

- [ ] **Step 7: Commit**

```bash
git add src/main/kotlin/worker/WorkerLoop.kt src/test/kotlin/worker/WorkerLoopTest.kt
git commit -m "feat: add layered MDC context propagation to WorkerLoop

Worker-level MDC (worker_id) set in pollAndProcess, task-level MDC
(task_id, handler_key, workflow_id, attempt) set in processTask.
Uses MDCContext from kotlinx-coroutines-slf4j for coroutine-safe
propagation. Fix handleTaskFailure to include handler key and
full stack trace in log output."
```

---

### Task 5: Full verification

- [ ] **Step 1: Run the entire test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test`
Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 2: Run JaCoCo coverage check**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`
Expected: All packages meet thresholds
