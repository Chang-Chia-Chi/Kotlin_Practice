# Session 8 — Structured Logging & Handler Contract

**Tier:** 3 (operational readiness)
**Prerequisites:** Session 7 (metrics foundation)
**Date:** 2026-03-27

---

## R3.6 — Enable JSON Structured Logging

**Problem:** Logs are plaintext — not parseable by log aggregation systems (Loki, CloudWatch, Stackdriver).

**Solution:**

- Add `quarkus-logging-json` dependency to `pom.xml`
- Configure main `application.properties`:
  ```properties
  quarkus.log.console.json=true
  quarkus.log.console.json.additional-field.service.value=workflow-engine
  ```
- Keep JSON disabled in test `application.properties` for readability:
  ```properties
  quarkus.log.console.json=false
  ```

Quarkus's JSON formatter automatically serializes MDC entries as top-level JSON fields, which is what makes R3.7 valuable. The `service` additional field tags every log line for multi-service log aggregation.

---

## R3.7 — Layered MDC Context for Task Processing

**Problem:** Log lines lack `task_id`, `handler_key`, `workflow_id`, and `attempt`. Operators cannot correlate logs across pods for a single task execution.

**Approach:** MDC + `MDCContext()` from `kotlinx-coroutines-slf4j`. Layered scoping mirrors the data lifecycle.

### Dependencies

- Add `kotlinx-coroutines-slf4j` to `pom.xml` (provides `MDCContext()`)

### Layer 1 — Poll level (`pollAndProcess`)

Set `worker_id` in MDC, wrap the poll body in `withContext(MDCContext())`. All logs from claim, iteration, and child calls inherit it.

### Layer 2 — Task level (`processTask`)

Merge the inherited MDC (has `worker_id`) with task-specific fields, wrap handler execution in a new `withContext(MDCContext(mergedMap))`:

| Field | Source | Scope |
|---|---|---|
| `worker_id` | `workerId` parameter | Poll-level, inherited |
| `task_id` | `task.id` | Task-level |
| `handler_key` | `task.handlerKey` | Task-level |
| `workflow_id` | `task.workflowId` | Task-level |
| `attempt` | `task.retryCount` | Task-level |

When `processTask` returns, the outer `MDCContext` restores the worker-only state — task fields don't leak between iterations.

### Example JSON output

```json
{
  "timestamp": "2026-03-27T10:15:30.123Z",
  "level": "INFO",
  "loggerName": "worker.WorkerLoop",
  "message": "Task completed",
  "worker_id": "pod-abc-0",
  "task_id": "42",
  "handler_key": "order.validate",
  "workflow_id": "7",
  "attempt": "0"
}
```

### Fix `handleTaskFailure` logging

- Add `task.handlerKey` to the warn message format
- Pass `cause` as the final SLF4J argument so the full stack trace appears in structured output

Since `handleTaskFailure` is called inside the task-level `MDCContext`, it inherits all fields automatically — the explicit format args are for message template readability.

---

## R3.8 — Document Idempotency Contract on TransitionHandler

**Problem:** `TransitionHandler` has no documentation. Handler authors have no indication the engine provides at-least-once delivery.

**Solution:** Add KDoc to `TransitionHandler`, `HandlerInput`, and `HandlerOutput`.

### TransitionHandler KDoc

- **Delivery guarantee:** at-least-once (retry on failure + sweeper reclaim on visibility timeout expiry)
- **Idempotency requirement:** handlers must be idempotent; use `taskId` as idempotency key for external system calls
- **Cancellation:** check `isActive` / `yield()` for cooperative shutdown; cancelled tasks stay in PROCESSING state and get reclaimed by the sweeper
- **Shutdown awareness:** reference `ShutdownSignal.isShuttingDown` from coroutine context to detect pod drain and skip optional work

### HandlerInput KDoc

- `taskId` — unique task identifier, explicitly called out as idempotency key
- `workflowId` — parent workflow identifier
- `sequenceNumber` — position in the workflow DAG
- `payload` — JSON from previous step's output or initial workflow input

### HandlerOutput KDoc

- `result` — JSON output passed to next step or stored as final workflow result

This is pure documentation — no behavioral changes.

---

## Files Modified

| File | Change |
|---|---|
| `pom.xml` | Add `quarkus-logging-json`, `kotlinx-coroutines-slf4j` |
| `src/main/resources/application.properties` | Add JSON logging config |
| `src/test/resources/application.properties` | Disable JSON logging for tests |
| `src/main/kotlin/worker/WorkerLoop.kt` | Layered MDC in `pollAndProcess` + `processTask`, fix `handleTaskFailure` logging |
| `src/main/kotlin/worker/TransitionHandler.kt` | KDoc on interface, `HandlerInput`, `HandlerOutput` |

## Verification

1. `mvn test` passes
2. Start `mvn quarkus:dev`, submit a workflow, verify JSON log output contains `task_id`, `handler_key`, `workflow_id`, `worker_id` fields
3. Trigger a handler failure, verify stack trace appears in structured log with all MDC fields
4. Verify `TransitionHandler` KDoc renders correctly in IDE
