# Session 7 — Metrics & Health Probes Design

**Tier:** 3 (operational readiness)
**Prerequisites:** Session 1-2 (drain window, DEAD_LETTER status)

---

## R3.1 — In-flight tasks gauge + concurrency limit gauge

**Problem:** `WorkerLoop._inFlightTasks` tracks active handler count but is never exposed as a metric. Operators cannot monitor bulkhead utilization.

**Change:** Inject `MeterRegistry` into `WorkerLoop`, register two gauges in `onStart`:

- `taskqueue_worker_in_flight_tasks{pod}` — gauge from `_inFlightTasks` AtomicInteger
- `taskqueue_worker_concurrency_limit{pod}` — gauge from `config.worker().concurrency()`

**Utilization ratio:** `taskqueue_worker_in_flight_tasks / taskqueue_worker_concurrency_limit`

**Files:** `WorkerLoop.kt`

---

## R3.2 — Claim outcome counter

**Problem:** No metric exists for claim behavior. Operators cannot distinguish empty queues from SKIP LOCKED contention from DB errors.

**Change:** Single counter with outcome tag in `pollAndProcess`:

- `taskqueue_claim_total{pod, outcome}` where outcome is `success`, `empty`, or `error`
- `taskqueue_claimed_tasks_total{pod}` — incremented by `tasks.size` for throughput

**Signals:**
- Idle queue: high `empty`, zero `success`
- Contention: high `success` rate but low tasks-per-claim
- DB issues: rising `error` rate

**Files:** `WorkerLoop.kt`

---

## R3.3 — Handler duration histogram via decorator

**Problem:** No per-handler timing exists. Operators cannot identify slow handlers or set latency SLOs.

**Change:** Decorator pattern to keep `WorkerLoop.processTask` clean.

**New class:** `MeteredTransitionHandler(delegate, handlerKey, meterRegistry)` implementing `TransitionHandler`
- Wraps `delegate.execute(input)` with `Timer.start()` / `sample.stop()`
- Tags: `handler=handlerKey`, `status=success|failure`
- Uses `publishPercentileHistogram()` for Prometheus histogram buckets
- On exception: records with `status=failure`, rethrows

**Modified:** `HandlerRegistry` — inject `MeterRegistry`, wrap handlers at registration time:
```kotlin
fun register(key: String, handler: TransitionHandler) {
    handlers[key] = MeteredTransitionHandler(handler, key, meterRegistry)
}
```

`WorkerLoop.processTask` stays untouched. Timing is transparent.

**Metric:** `taskqueue_handler_duration_seconds{handler, status}`

**Files:** New `MeteredTransitionHandler.kt`, modified `HandlerRegistry.kt`

---

## R3.4 — Worker loop liveness check (revised)

**Problem:** Original plan used `_lastPollTimestamp` with a static threshold, which false-alarms when all concurrency slots are busy (saturated workers don't poll — that's healthy).

**Change:** Replace `_lastPollTimestamp` with `_lastActivityTimestamp`, updated in two places:
1. `pollAndProcess` after `claimNext` returns (existing location)
2. `processTask` finally-block after decrementing `_inFlightTasks`

This ensures the timestamp stays fresh whenever the worker is doing anything (polling or completing tasks). It only goes stale when nothing is happening — truly stuck.

Rename public property from `lastPollTimestamp` to `lastActivityTimestamp`.

**New class:** `WorkerLoopHealthCheck` with `@Liveness @Singleton`
- Threshold: `pollInterval * 5` (static, works correctly now)
- If `age < threshold` -> UP
- If `age >= threshold` -> DOWN with `last_activity_age_seconds`, `threshold_seconds`

**Why static threshold works now:**
- Idle: polls every interval -> timestamp fresh
- Saturated: tasks completing -> timestamp fresh
- Truly stuck (all handlers hung, no polls): timestamp stale -> correctly DOWN

**Files:** Modified `WorkerLoop.kt`, new `WorkerLoopHealthCheck.kt`

---

## R3.5 — Stale-leader health check + heartbeat age gauge

**Problem:** No mechanism detects stale leadership. If the K8s API becomes unreachable, `isActive` remains true until `onStoppedLeading` fires. The sweeper continues with a stale lease.

**New gauge in `LeaderManager.registerMetrics()`:**
- `leader_election_heartbeat_age_seconds` — computed from `Duration.between(lastHeartbeat, Instant.now(clock))`

**Existing metrics (no changes needed):**
- `leader_election_is_leader` — already registered
- `leader_election_epoch` — already registered

**New config field** in `FrameworkConfig.LeaderElectionConfig`:
```kotlin
@WithDefault("PT45S")
fun healthThreshold(): Duration
```
Default PT45S (3x default leaseDuration). Tunable by operators independently of lease timing.

**New class:** `LeaderHealthCheck` with `@Liveness @Singleton`
- If `!isActive` -> UP (followers are always healthy)
- If active and `heartbeat age < healthThreshold` -> UP
- If active and `heartbeat age >= healthThreshold` -> DOWN with `heartbeat_age_seconds`

**Files:** Modified `LeaderManager.kt`, modified `FrameworkConfig.kt`, new `LeaderHealthCheck.kt`

---

## Testing Strategy

- **R3.1:** Unit test verifying gauges are registered and reflect `_inFlightTasks` / concurrency values
- **R3.2:** Unit test verifying counter increments for each outcome (success, empty, error)
- **R3.3:** Unit test for `MeteredTransitionHandler` — verify timer records on success and failure, verify exception is rethrown
- **R3.4:** Unit test for `WorkerLoopHealthCheck` — mock fresh timestamp (UP), mock stale timestamp (DOWN)
- **R3.5:** Unit test for `LeaderHealthCheck` — follower always UP, leader with fresh heartbeat UP, leader with stale heartbeat DOWN

All tests use `SimpleMeterRegistry` for metric assertions. Health check tests mock `WorkerLoop`/`LeaderElection` and `FrameworkConfig`.

---

## Summary of files

| Action | File |
|--------|------|
| Modify | `WorkerLoop.kt` — inject MeterRegistry, register gauges/counters, rename to lastActivityTimestamp |
| Modify | `HandlerRegistry.kt` — inject MeterRegistry, wrap handlers with MeteredTransitionHandler |
| Modify | `LeaderManager.kt` — add heartbeat age gauge |
| Modify | `FrameworkConfig.kt` — add healthThreshold field |
| Create | `MeteredTransitionHandler.kt` — decorator for handler timing |
| Create | `WorkerLoopHealthCheck.kt` — liveness check for worker loop |
| Create | `LeaderHealthCheck.kt` — liveness check for stale leader |
