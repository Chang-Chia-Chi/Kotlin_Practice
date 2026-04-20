# Session 8 — Structured Logging & Handler Contract

**Tier:** 3 (operational readiness)
**Prerequisites:** Session 7 (metrics foundation)
**Estimated scope:** Logging config + MDC context + documentation + tests

---

## Items

### R3.6 — Enable JSON structured logging

**Problem:** No `quarkus.log.console.json=true` in config. Logs are plaintext — not parseable by log aggregation systems (Loki, CloudWatch, Stackdriver).

**Files to modify:**
- `pom.xml` — add dependency (if not present):
  ```xml
  <dependency>
      <groupId>io.quarkus</groupId>
      <artifactId>quarkus-logging-json</artifactId>
  </dependency>
  ```
- `src/main/resources/application.properties`:
  ```properties
  quarkus.log.console.json=true
  quarkus.log.console.json.additional-field.service.value=workflow-engine
  ```
- `src/test/resources/application.properties` — keep JSON disabled for test readability:
  ```properties
  quarkus.log.console.json=false
  ```

---

### R3.7 — Add MDC context for task processing

**Problem:** Log lines lack `task_id`, `handler_type`, `workflow_id`, and `attempt_number`. An operator cannot correlate logs across pods for a single task execution.

**Files to modify:**
- `src/main/kotlin/worker/WorkerLoop.kt` — `processTask` method

**Fix:** Wrap handler execution in MDC context:
```kotlin
private suspend fun processTask(task: Task) {
    MDC.put("task_id", task.id.toString())
    MDC.put("handler_key", task.handlerKey)
    MDC.put("workflow_id", task.workflowId.toString())
    MDC.put("attempt", task.retryCount.toString())
    try {
        // ... existing handler resolution and execution
    } finally {
        MDC.remove("task_id")
        MDC.remove("handler_key")
        MDC.remove("workflow_id")
        MDC.remove("attempt")
    }
}
```

Note: MDC is thread-local. With coroutines on `Dispatchers.IO`, each suspension point may resume on a different thread. Use `kotlinx-coroutines-slf4j` MDC context element:
```kotlin
// Add dependency: kotlinx-coroutines-slf4j
withContext(MDCContext()) {
    // MDC values propagate across suspension points
}
```

Or use Quarkus's built-in MDC propagation for reactive contexts if available.

**Also fix `handleTaskFailure` logging (from Pass 3 finding):**
```kotlin
// Change:
log.warn("Task {} failed (retry {}/{}): {}", task.id, task.retryCount, task.maxRetries, cause.message)

// To:
log.warn(
    "Task {} (handler={}) failed (retry {}/{}): {}",
    task.id,
    task.handlerKey,
    task.retryCount,
    task.maxRetries,
    cause.message,
    cause,  // SLF4J appends full stack trace when last arg is Throwable
)
```

---

### R3.8 — Document idempotency contract on `TransitionHandler`

**Problem:** `TransitionHandler` has no documentation. Handler authors have no indication the engine is at-least-once. With the zombie handler scenario (Pass 2) and retry semantics, handlers that perform external side effects will silently introduce double-execution bugs.

**Files to modify:**
- `src/main/kotlin/worker/TransitionHandler.kt`

**Fix:**
```kotlin
/**
 * Handler for task execution within the workflow engine.
 *
 * ## Delivery Guarantee
 *
 * The engine provides **at-least-once** delivery. A handler may be invoked
 * multiple times for the same logical task due to:
 * - Retry on failure (up to [maxRetries] per task)
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
 * Long-running handlers should periodically check [isActive] or call
 * [yield] to cooperate with graceful shutdown. On pod termination,
 * in-flight handlers receive [CancellationException] after the drain
 * window expires. Tasks whose handlers are cancelled remain in
 * PROCESSING state and will be reclaimed by the sweeper.
 *
 * ## Shutdown Awareness
 *
 * Handlers can check [ShutdownSignal.isShuttingDown] from their
 * coroutine context to detect that the pod is draining. Use this to
 * skip optional work or checkpoint progress.
 */
interface TransitionHandler {
    suspend fun execute(input: HandlerInput): HandlerOutput
}
```

Also add KDoc to `HandlerInput`:
```kotlin
/**
 * @property taskId Unique task identifier — use as idempotency key for external calls.
 * @property workflowId Parent workflow identifier.
 * @property sequenceNumber Position in the workflow DAG.
 * @property payload JSON payload from the previous step's output (or initial input).
 */
data class HandlerInput(...)
```

---

## Verification

1. `mvn test` passes
2. Start `mvn quarkus:dev`, submit a workflow, verify JSON log output contains `task_id`, `handler_key`, `workflow_id` fields
3. Trigger a handler failure, verify stack trace appears in structured log
4. Verify `TransitionHandler` KDoc renders correctly in IDE
