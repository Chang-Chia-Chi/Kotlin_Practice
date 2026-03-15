# Handler Execution Pipeline — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0  
**Depends on:** Event Bus

---

## 1. Problem Statement

The framework's current execution model is:

```
claim task → resolve handler → handle(payload) → report result
```

Every cross-cutting concern — metrics, tracing, timeout enforcement, circuit breaking, error classification, structured logging — must be implemented by each handler individually or bolted on ad hoc. This produces:

- **Inconsistent observability.** Some handlers emit metrics, others don't. Latency tracking is absent or hand-rolled.
- **No timeout enforcement.** A handler that blocks forever (hung JDBC connection, unresponsive downstream) holds a bulkhead slot permanently. The pod appears healthy but throughput degrades silently.
- **No circuit breaking.** When a downstream system (SFTP, MinIO, Trino) goes down, the queue drains tasks through the retry/dead-letter cycle at full speed. This turns a temporary outage into a permanent backlog of dead-lettered tasks that need manual replay.
- **Duplicated boilerplate.** Every handler wraps its body in try-catch, timing, logging. The patterns are identical but each implementation drifts.

The pipeline formalizes these concerns as composable middleware that wraps every handler invocation uniformly.

---

## 2. Goals & Non-Goals

### Goals

- **Uniform cross-cutting behavior.** Every handler gets metrics, tracing, timeout, and error classification without opt-in.
- **Composable middleware.** Middlewares are independent, ordered, and stackable. Adding a new concern means adding a new middleware — not modifying existing ones.
- **Per-handler configuration.** Timeouts, circuit breaker thresholds, and retry classification can be tuned per handler via annotation or configuration.
- **Extensible by developers.** Project-specific middleware (tenant isolation, audit logging, feature flags) can be added by implementing the middleware interface and letting CDI discover it.

### Non-Goals

- Replacing CDI interceptors for non-task concerns. The pipeline is task-execution-specific — it doesn't affect REST endpoints or scheduled methods.
- Dynamic middleware reordering at runtime. The chain is built at startup and cached.
- Middleware per-queue (rather than per-handler). Queue-level concerns are handled by the claim loop, not the pipeline.

---

## 3. Core Concept: The Middleware Chain

A middleware is a wrapper around handler execution. Each middleware can inspect the task context, decide whether to proceed, transform the result, or short-circuit execution entirely.

Middlewares are ordered by a numeric priority. Lower numbers execute first (outermost layer). The handler itself is the innermost element — it doesn't implement the middleware interface.

```
                    ┌─ Metrics (10) ────────────────────────────────┐
                    │  ┌─ Tracing (20) ─────────────────────────┐   │
                    │  │  ┌─ CircuitBreaker (30) ────────────┐  │   │
                    │  │  │  ┌─ Timeout (40) ─────────────┐  │  │   │
                    │  │  │  │  ┌─ ErrorClassifier (50) ┐  │  │  │   │
                    │  │  │  │  │                       │  │  │  │   │
  Task context ────►│  │  │  │  │   Handler.handle()    │  │  │  │   │
                    │  │  │  │  │                       │  │  │  │   │
  TaskResult   ◄────│  │  │  │  └───────────────────────┘  │  │  │   │
                    │  │  │  └──────────────────────────────┘  │  │   │
                    │  │  └────────────────────────────────────┘  │   │
                    │  └──────────────────────────────────────────┘   │
                    └────────────────────────────────────────────────┘
```

The chain is constructed once per handler at startup (resolve all middlewares, sort by order, link into a chain) and reused for every invocation. No per-invocation allocation.

---

## 4. Task Execution Context

Every middleware receives an immutable snapshot of the task being executed. This context is constructed by the worker loop after claiming a task and passed through the entire chain.

| Field | Purpose |
|-------|---------|
| taskId | UUID of the task |
| handler | Routing key (e.g., `"dispatch.map"`) |
| queue | Queue name |
| groupId | Job correlation (null for standalone tasks) |
| payload | Raw JSON string (opaque to the pipeline) |
| metadata | Raw JSON string (opaque to the pipeline) |
| retryCount | Current attempt number |
| maxRetries | Configured max attempts |
| claimedAt | When this pod claimed the task |
| taskContext | Runtime context carrying `isShuttingDown` and future extensibility |

The context is read-only. Middlewares cannot modify it — they can only use it to make decisions (e.g., which circuit breaker to apply, what timeout to use).

---

## 5. Standard Middlewares

### 5.1 Metrics Middleware (order = 10)

The outermost layer. Wraps the entire chain to capture end-to-end timing and outcome.

**Behavior:**

- Starts a timer before calling the next middleware.
- After the chain returns a result, records the duration and result type.
- Increments an in-flight gauge on entry, decrements on exit.

**Metrics emitted:**

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `taskqueue.handler.duration` | Timer/Histogram | handler, queue, result | Latency distribution per handler |
| `taskqueue.handler.executions` | Counter | handler, result | Throughput and success/failure rates |
| `taskqueue.handler.inflight` | Gauge | handler | Currently executing tasks |
| `taskqueue.handler.exceptions` | Counter | handler, exception_class | Exception type breakdown |

**Events fired:** `TaskCompleted` (via the event bus) after recording metrics. This is the authoritative source of task completion events for the entire framework.

### 5.2 Tracing Middleware (order = 20)

Creates an OpenTelemetry span per task execution for distributed tracing.

**Behavior:**

- Creates a new span named `"task.execute {handler}"`.
- Attaches task attributes: taskId, handler, queue, retryCount, groupId.
- On success, sets span status to OK. On failure, sets status to ERROR and records the exception.
- Ends the span after the chain returns.

**Why order 20:** Inside metrics so that the metrics timer includes tracing overhead (negligible, but consistent). Outside circuit breaker so that rejected tasks still get a span (useful for debugging breaker behavior).

### 5.3 Circuit Breaker Middleware (order = 30)

Prevents a downstream outage from draining the queue into dead-letter.

**Behavior:**

- Checks the circuit breaker state for this handler.
- If the breaker is **OPEN**: does NOT proceed to the next middleware. Returns a `Retry` result with a delay equal to the breaker's wait duration. Critically, this retry does NOT consume a retry attempt — the breaker opening is a system-level concern, not a handler failure.
- If the breaker is **CLOSED** or **HALF_OPEN**: proceeds to the next middleware. Records success or failure on the breaker based on the result.
- When the breaker transitions state, fires `CircuitBreakerStateChanged` via the event bus. The claim loop observes this event and adds the handler to a suppressed set, preventing it from claiming tasks for that handler (avoiding wasted SKIP LOCKED cycles).

**Configuration per handler:**

| Setting | Default | Purpose |
|---------|---------|---------|
| Failure rate threshold | 50% | Open breaker when failure rate exceeds this |
| Sliding window size | 20 | Number of recent executions to evaluate |
| Wait duration in open state | 30s | How long to keep the breaker open before probing |
| Permitted calls in half-open | 5 | How many probe calls to allow before deciding |

Handlers without circuit breaker configuration have no breaker — the middleware passes through transparently.

**Interaction with the claim loop:** When a breaker opens, the claim loop adds `AND handler NOT IN (:suppressedHandlers)` to the SKIP LOCKED query. When the breaker transitions to HALF_OPEN, the handler is removed from the suppressed set. This is coordinated via `CircuitBreakerStateChanged` events, not direct method calls.

### 5.4 Timeout Middleware (order = 40)

Enforces a hard execution deadline per handler.

**Behavior:**

- Resolves the timeout for this handler (annotation-based per handler, falling back to a global default).
- Wraps the remaining chain in a coroutine timeout.
- If the deadline expires, cancels the handler's coroutine and returns a `Failure` result with a timeout error message.

**Configuration:**

| Setting | Default | Purpose |
|---------|---------|---------|
| Global default timeout | 2 minutes | Applied to handlers without explicit timeout |
| Per-handler timeout | Annotation-based | Overrides the global default |

Examples of per-handler tuning: `email.send` might have a 30-second timeout. `report.generate` (Trino query + Parquet export) might have a 5-minute timeout. `dispatch.map` might use the default.

**Interaction with graceful shutdown:** The timeout middleware respects `taskContext.isShuttingDown`. If the pod is draining and the timeout fires, the result is `Retry(delay=0, consumeRetry=false)` instead of `Failure` — the task should be re-enqueued immediately for another pod, not penalized.

### 5.5 Error Classifier Middleware (order = 50)

The innermost middleware. Catches exceptions from the handler and translates them into structured `TaskResult` values.

**Behavior:**

- Wraps the handler invocation in a try-catch.
- Classifies exceptions into three categories:

| Category | Examples | Result | Retry consumed? |
|----------|----------|--------|-----------------|
| **Transient** | SQLTransientException, ConnectException, SocketTimeoutException | Retry with exponential backoff + jitter | Yes |
| **Permanent** | IllegalArgumentException, JsonProcessingException, NullPointerException | Dead-letter immediately (skip remaining retries) | N/A — dead-lettered |
| **Unknown** | Everything else | Failure (normal retry/dead-letter cycle) | Yes |

**Exponential backoff formula:** `min(baseMs × 2^retryCount, maxMs) ± 25% jitter`. Base: 1 second. Max: 60 seconds.

**Per-handler customization:** Handlers can declare which exception types are transient and which are permanent via annotation. This overrides the default classification. Example: an SFTP handler might declare `JSchException` as transient and `FileNotFoundException` as permanent.

**Why innermost:** The error classifier must be the last thing between the pipeline and the handler. If it were outer, it would catch exceptions from other middlewares (timeout, circuit breaker), conflating infrastructure errors with handler errors.

---

## 6. End-to-End Execution Flow

After the worker loop claims a task via SKIP LOCKED:

1. **Resolve handler** from the CDI registry using the task's `handler` string.
2. **Retrieve the cached pipeline chain** for this handler (built once at startup).
3. **Construct TaskExecutionContext** from the claimed task row.
4. **Execute the chain.** The middlewares nest as described in §3.
5. **Process the TaskResult:**

| Result | Framework action |
|--------|-----------------|
| Success | UPDATE task SET status = 'COMPLETED'. Fire TaskCompleted event. |
| Failure | UPDATE task SET status = 'FAILED', retry_count++. If retry_count ≥ max_retries → DEAD_LETTER. Fire TaskCompleted event. |
| Retry(delay, consumeRetry) | UPDATE task SET status = 'PENDING', scheduled_at = now + delay. If consumeRetry, increment retry_count. Fire TaskCompleted event. |
| DeadLetter(reason) | UPDATE task SET status = 'DEAD_LETTER'. Fire TaskDeadLettered event. |

---

## 7. Custom Middleware

Developers add project-specific middleware by implementing the middleware interface, giving it an `@ApplicationScoped` annotation, and choosing an `order` value. CDI discovers it automatically. The pipeline builder includes it in the chain at the correct position.

Examples of custom middleware:

- **Tenant isolation** (order 25) — extracts a tenant ID from the payload and sets a thread-local tenant context for downstream database routing.
- **Audit logging** (order 15) — writes a before/after audit record for regulated workloads.
- **Feature flags** (order 35) — checks a feature flag service before proceeding; returns `Retry` if the feature is disabled.

---

## 8. Handler Contract Change: TaskContext

The pipeline introduces a `TaskContext` parameter to the handler interface. The handler signature changes from `handle(payload)` to `handle(payload, ctx)`.

`TaskContext` carries runtime state that the framework communicates to handlers:

| Field | Purpose |
|-------|---------|
| isShuttingDown | True if the pod is in drain phase. Cooperative handlers can exit early and return Retry(delay=0). |

This is a breaking change to the handler SPI. To ease migration, `TaskContext` can be introduced with a default implementation on the interface so existing handlers compile without changes, even if they ignore the context.

Future extensibility: `TaskContext` may carry a progress callback (for heartbeat-aware long tasks), a cancellation token (for cooperative timeout), or a deadline instant (for time-aware handlers).

---

## 9. Middleware Ordering Invariants

The order values are not arbitrary. They encode semantic dependencies:

- **Metrics (10) must be outermost** — it measures the total wall-clock time including all middleware overhead. If metrics were inside circuit breaker, breaker rejections wouldn't be timed.
- **Tracing (20) must be outside circuit breaker** — rejected tasks should still produce a span for debuggability.
- **Circuit breaker (30) must be outside timeout** — if the handler times out, that's a failure the breaker should count. But if the breaker is open, the timeout shouldn't start at all.
- **Timeout (40) must be outside error classifier** — a timeout produces a `TimeoutCancellationException` that the error classifier doesn't need to see (the timeout middleware handles it directly).
- **Error classifier (50) must be innermost** — it only classifies handler exceptions, not middleware exceptions.

Custom middlewares choose their order based on where in this chain they belong. The framework should document the semantic boundaries (10–20: observation, 20–40: resilience, 40–50: error handling) to guide developers.

---

## 10. Observability Summary

The pipeline is the single source of truth for task execution observability. No handler needs to emit its own metrics or fire its own events. The pipeline does it uniformly:

| What | How | Where |
|------|-----|-------|
| Latency per handler | Metrics middleware timer | Prometheus histogram |
| Throughput per handler | Metrics middleware counter | Prometheus counter |
| Error rate per handler | Metrics middleware counter (by result type) | Prometheus counter |
| In-flight tasks | Metrics middleware gauge | Prometheus gauge |
| Distributed trace | Tracing middleware span | OpenTelemetry collector |
| Circuit breaker state | CircuitBreakerStateChanged event | Health probes, Prometheus gauge |
| Task completion | TaskCompleted event | Dead letter processor, MR counter incrementer |
| Dead-letter | TaskDeadLettered event | Alerting, dead letter processor |

---

## 11. Testing Strategy

| Test | Validates |
|------|-----------|
| Execute a handler through the full pipeline, verify metrics are recorded | Metrics middleware wiring |
| Simulate downstream timeout, verify handler coroutine is cancelled and result is Failure | Timeout middleware |
| Fail 50% of calls, verify circuit breaker opens and subsequent calls return Retry without calling handler | Circuit breaker middleware |
| Throw SQLTransientException, verify result is Retry with backoff | Error classifier (transient) |
| Throw IllegalArgumentException, verify result is DeadLetter | Error classifier (permanent) |
| Register a custom middleware at order 25, verify it executes between tracing and circuit breaker | Custom middleware ordering |
| Open circuit breaker, verify claim loop suppresses handler | Event bus integration (CircuitBreakerStateChanged → claim loop) |
| Set isShuttingDown=true, trigger timeout, verify result is Retry(delay=0) not Failure | Shutdown-aware timeout |
