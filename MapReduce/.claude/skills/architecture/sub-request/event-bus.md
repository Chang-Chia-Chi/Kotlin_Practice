# Event Bus (Internal Lifecycle Events) — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0

---

## 1. Problem Statement

Framework subsystems need to react to each other's state changes. Today these interactions are either implicit (a callback buried inside a single bean) or missing entirely:

- Leader election fires `onStopLeading()`, but only one bean knows about it. The shutdown coordinator, orchestration loops, health probes, and metrics all need to know — and today they don't.
- A task is dead-lettered. The metrics layer needs to count it. The alerting layer needs to evaluate thresholds. The dead letter processor needs to index it. But the task queue just flips a status column and moves on.
- A circuit breaker opens for a handler. The claim loop should stop claiming tasks for that handler to avoid wasting SKIP LOCKED cycles on tasks that will be immediately re-enqueued. But the claim loop doesn't know the circuit breaker exists.

Without a formal event mechanism, every new cross-component interaction becomes a direct method call, a new constructor parameter, and eventually a dependency cycle. Adding a new consumer of leadership events means modifying the LeaderManager bean — violating open/closed.

---

## 2. Goals & Non-Goals

### Goals

- **Decoupled communication.** Producers fire events without knowing who consumes them. Consumers observe events without importing producer types.
- **Type-safe.** Each event is a distinct type. Observers declare the exact event type they care about. No string-based routing, no casting.
- **Zero infrastructure.** In-process only. No external message broker, no persistence, no serialization.
- **Testable.** Fire events in unit tests, verify observers react correctly.

### Non-Goals

- Durable event delivery. If a consumer is not running when an event fires, the event is lost. These are lifecycle signals, not business messages.
- Cross-pod event propagation. Events are pod-local. For cross-pod communication, the framework uses the Oracle task table.
- Event sourcing or replay. Events are fire-and-forget notifications.
- Ordering guarantees across different event types.

---

## 3. Design: CDI Events

The event bus is Quarkus CDI events (`jakarta.enterprise.event.Event<T>`). No custom framework needed — CDI provides type-safe publish/subscribe with auto-discovery of observers.

Why CDI events over a hand-rolled pub/sub:

- Already built into Quarkus. No new dependencies.
- Observers are discovered automatically via CDI bean scanning.
- `@ObservesAsync` is available for consumers that should not block the producer.
- `@Priority` controls observer ordering when it matters.
- Qualifiers allow filtering (e.g., observe events only for a specific handler).

---

## 4. Event Catalog

Every event is an immutable data class. Events carry enough context for any consumer to act without querying back to the source. No event should require the consumer to make a follow-up database call to be useful.

### 4.1 Lifecycle Events

**LeadershipAcquired** — Fired when this pod wins the K8s Lease election.

| Field | Type | Purpose |
|-------|------|---------|
| epoch | Long | The fencing token (Lease resourceVersion) |
| podId | String | Identity of this pod |
| acquiredAt | Instant | Timestamp |

**LeadershipLost** — Fired when this pod loses the K8s Lease (renewal failed, shutdown, or stepped down).

| Field | Type | Purpose |
|-------|------|---------|
| lastEpoch | Long | The epoch this pod held |
| podId | String | Identity of this pod |
| lostAt | Instant | Timestamp |

**ShutdownStateChanged** — Fired when the shutdown coordinator transitions between phases (RUNNING → DRAINING → RELEASING → TERMINATED).

| Field | Type | Purpose |
|-------|------|---------|
| previousState | ShutdownState | State before transition |
| newState | ShutdownState | State after transition |
| inFlightTasks | Int | Tasks still executing |
| drainDeadline | Instant? | When drain timeout expires (null if not draining) |

### 4.2 Task Events

**TaskClaimed** — Fired after a task is successfully claimed by this pod's worker loop.

| Field | Type | Purpose |
|-------|------|---------|
| taskId | UUID | Task identifier |
| handler | String | Routing key |
| queue | String | Queue name |
| groupId | UUID? | Job correlation (null for standalone) |
| claimedAt | Instant | Timestamp |

**TaskCompleted** — Fired after a task finishes execution (any outcome).

| Field | Type | Purpose |
|-------|------|---------|
| taskId | UUID | Task identifier |
| handler | String | Routing key |
| queue | String | Queue name |
| groupId | UUID? | Job correlation |
| result | TaskResultType | SUCCESS, FAILED, RETRY, DEAD_LETTERED |
| durationMs | Long | Execution time |
| retryCount | Int | How many attempts so far |
| errorMessage | String? | Last error (null on success) |

**TaskDeadLettered** — Fired when a task is moved to DEAD_LETTER status. This is a subset of TaskCompleted but broken out as a separate event because it has distinct consumers (alerting, dead letter processor) that don't care about successful completions.

| Field | Type | Purpose |
|-------|------|---------|
| taskId | UUID | Task identifier |
| handler | String | Routing key |
| queue | String | Queue name |
| groupId | UUID? | Job correlation |
| retryCount | Int | Attempts exhausted |
| lastError | String | Final error message |
| createdAt | Instant | When the task was originally enqueued |
| deadLetteredAt | Instant | When it was dead-lettered |

**TaskReclaimed** — Fired when the stale task reaper reclaims a task from a dead pod.

| Field | Type | Purpose |
|-------|------|---------|
| taskId | UUID | Task identifier |
| handler | String | Routing key |
| previousClaimedBy | String | Pod that held the task |
| retryCount | Int | Updated retry count |
| reclaimedAt | Instant | Timestamp |

### 4.3 Resilience Events

**CircuitBreakerStateChanged** — Fired when a handler's circuit breaker transitions state.

| Field | Type | Purpose |
|-------|------|---------|
| name | String | Circuit breaker name (typically the handler string) |
| previousState | CBState | CLOSED, OPEN, or HALF_OPEN |
| newState | CBState | CLOSED, OPEN, or HALF_OPEN |
| failureRate | Double | Current failure rate at time of transition |
| changedAt | Instant | Timestamp |

### 4.4 Map-Reduce Events (Layer 2)

**JobStateChanged** — Fired when a map-reduce job transitions state.

| Field | Type | Purpose |
|-------|------|---------|
| jobId | UUID | Job identifier |
| jobType | String | Definition routing key |
| previousStatus | JobStatus | State before transition |
| newStatus | JobStatus | State after transition |
| completedTasks | Int | Map tasks completed so far |
| failedTasks | Int | Map tasks dead-lettered so far |
| totalTasks | Int | Total map tasks |
| changedAt | Instant | Timestamp |

---

## 5. Producer–Consumer Matrix

This is the central coordination map of the framework. It shows who produces each event and who consumes it.

| Event | Producers | Consumers |
|-------|-----------|-----------|
| LeadershipAcquired | Leader Election | Orchestration loops (start), Health probes, Metrics |
| LeadershipLost | Leader Election | Orchestration loops (stop), Shutdown coordinator (informational), Health probes, Metrics |
| ShutdownStateChanged | Shutdown Coordinator | Worker loop, Health probes, Leader election (informational) |
| TaskClaimed | Worker loop | Metrics (in-flight gauge) |
| TaskCompleted | Worker loop | Metrics (latency, throughput), Map-reduce counter incrementer |
| TaskDeadLettered | Worker loop | Dead Letter Processor (alerting), Metrics (dead-letter counter) |
| TaskReclaimed | Stale Task Reaper | Metrics (reclaim counter), Alerting |
| CircuitBreakerStateChanged | Handler execution pipeline | Claim loop (handler suppression), Health probes |
| JobStateChanged | Map-reduce orchestrator | Metrics, Alerting (job failure) |

---

## 6. Synchronous vs. Asynchronous Delivery

CDI supports both `@Observes` (synchronous) and `@ObservesAsync` (asynchronous). The choice depends on whether the consumer's reaction is essential to the producer's correctness.

**Synchronous (`@Observes`)** — the consumer runs on the producer's thread, blocking the producer until the observer returns. Use when:

- The consumer's reaction must complete before the producer continues.
- Failure in the consumer should propagate to the producer.

Examples: Orchestration loops stopping on `LeadershipLost` (must stop before lease is released). Shutdown coordinator reacting to state changes.

**Asynchronous (`@ObservesAsync`)** — the consumer runs on a managed executor thread pool, decoupled from the producer. Use when:

- The consumer is a side effect (metrics, alerting, logging to external systems).
- Consumer failure should not affect the producer.

Examples: Metrics recording on `TaskCompleted`. Alerting on `TaskDeadLettered`. Dead letter indexing.

---

## 7. Error Isolation

A critical design concern: a failing synchronous observer propagates its exception to the producer. A bug in the metrics observer should not crash the task completion path.

**Rule:** All non-essential synchronous observers must wrap their body in a try-catch that logs the error and continues. Essential observers (e.g., orchestration loop shutdown on `LeadershipLost`) should propagate failures because their reaction is critical to system safety.

The framework should provide a utility for safe observation that catches and logs exceptions. Observers opt into this by convention.

**For asynchronous observers**, exceptions are isolated by default — they don't propagate to the producer.

---

## 8. Ordering

CDI synchronous observers execute in undefined order across beans. Within a single bean, method declaration order applies.

If ordering matters between two consumers of the same event (e.g., orchestration loops must stop before the lease is released), use `@Priority` on the observer method. Lower priority values execute first.

In practice, ordering is rarely needed because the shutdown coordinator already controls the phase sequence explicitly — it doesn't rely on event ordering for its critical path.

---

## 9. Testing Strategy

| Test | Validates |
|------|-----------|
| Fire `LeadershipAcquired`, verify orchestration loops started | Event wiring correctness |
| Fire `LeadershipLost`, verify orchestration loops cancelled | Shutdown coordination |
| Fire `TaskDeadLettered`, verify alert evaluator receives it | Dead letter alerting chain |
| Fire `CircuitBreakerStateChanged(OPEN)`, verify handler added to suppressed set | Claim loop circuit breaker integration |
| Fire event with failing observer, verify producer is not affected | Error isolation |
| Fire `TaskCompleted(SUCCESS)`, verify metrics counter incremented | Metrics integration |

---

## 10. Anti-Patterns

**Using the event bus for request/response.** Events are one-way notifications. If the producer needs a return value, use a direct method call.

**Putting business logic in event observers.** Observers should be thin — update a counter, start/stop a loop, log. Heavy processing belongs in the component itself, triggered by the event.

**Creating event storms.** Don't fire an event from within an observer of another event in a way that could cascade. If A→B→C→A, you have an infinite loop. The event catalog is designed to avoid this — lifecycle events flow downward (leader → orchestration → tasks), not upward.

**Relying on event delivery for correctness.** Events are optimizations and coordination signals. The database (fencing, task table, job counters) is the source of truth. If an event is lost (pod crash, observer exception), the system must converge to the correct state via its next polling cycle or recovery mechanism.
