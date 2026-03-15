# Graceful Shutdown — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0

---

## 1. Problem Statement

When a pod terminates — whether from a rolling deployment, scale-down, node drain, or OOM kill — the system must ensure that:

1. No task is silently lost or left in `CLAIMED` limbo for the duration of the stale timeout.
2. No map-reduce job is left in a half-transitioned state with no leader to drive it forward.
3. In-flight work gets a reasonable window to complete rather than being killed mid-execution.
4. The Kubernetes scheduler sees the pod as "done" only after cleanup is truly finished.

Without explicit shutdown coordination, every pod restart creates a window of unavailability equal to the stale-task reaper's timeout. For a 60-second stale timeout and a 4-pod rolling restart with 30-second rollout intervals, that's up to 4 minutes of tasks stuck in `CLAIMED` — visible as a throughput cliff in monitoring.

This document specifies the shutdown protocol for both logical roles: **worker** (every pod) and **leader** (the lease holder). Since every leader is also a worker, a leader shutdown is a superset: leader-specific teardown runs first, then worker teardown.

---

## 2. Goals & Non-Goals

### Goals

- **Zero stale tasks from planned shutdowns.** Every task either completes within the drain window or is explicitly released back to `PENDING` before the pod exits.
- **Leader handoff without job stalls.** A shutting-down leader releases its Kubernetes Lease early, allowing another pod to acquire leadership before the drain window expires.
- **Bounded shutdown time.** The entire shutdown sequence completes within Kubernetes' `terminationGracePeriodSeconds`. No hanging pods.
- **Observable.** Operators can see shutdown state, drain progress, and release counts in logs and metrics.
- **No special client-side handling.** Callers of the config API and task enqueuers are unaffected — Kubernetes Service routing handles it via readiness probe removal.

### Non-Goals

- Checkpointing partially completed work within a handler. Handlers are expected to be idempotent; re-execution after release is the recovery path.
- Migrating in-flight tasks to a specific peer pod. Released tasks go back to the queue and are claimed by whoever is fastest.
- Graceful shutdown for unplanned kills (SIGKILL, OOM). These are inherently ungraceful — the stale-task reaper handles recovery.

---

## 3. Signal Flow & Kubernetes Integration

### 3.1 Termination Sequence

When Kubernetes decides to terminate a pod, the following happens in order:

```
K8s sends SIGTERM
       │
       ▼
┌──────────────────────────────────────────────────────────────────┐
│  Pod                                                             │
│                                                                  │
│  1. preStop hook fires (if configured)                           │
│  2. Quarkus CDI @Shutdown observers fire                         │
│  3. Readiness probe starts returning 503                         │
│     → K8s removes pod from Service endpoints                     │
│     → No new config API traffic routed here                      │
│  4. Shutdown coordinator runs (this design)                      │
│  5. Pod process exits                                            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
       │
       ▼ (if pod hasn't exited within terminationGracePeriodSeconds)
K8s sends SIGKILL — hard kill, no further cleanup possible
```

### 3.2 Kubernetes Configuration

```yaml
spec:
  terminationGracePeriodSeconds: 90   # hard ceiling for entire shutdown
  containers:
    - name: app
      lifecycle:
        preStop:
          exec:
            command: ["sh", "-c", "sleep 3"]   # allow endpoint removal to propagate
      readinessProbe:
        httpGet:
          path: /q/health/ready
          port: 8080
        periodSeconds: 5
        failureThreshold: 1     # one failure = immediate removal from Service
      livenessProbe:
        httpGet:
          path: /q/health/live
          port: 8080
        periodSeconds: 10
        failureThreshold: 3
```

**Why `terminationGracePeriodSeconds: 90`?** The budget is:

| Phase | Duration | Cumulative |
|-------|----------|------------|
| `preStop` sleep (endpoint propagation) | 3 s | 3 s |
| Leader teardown (lease release + loop drain) | up to 5 s | 8 s |
| Worker drain (in-flight task completion) | up to 60 s | 68 s |
| Task release (uncompleted task cleanup) | up to 5 s | 73 s |
| Safety margin | 17 s | 90 s |

The 60-second worker drain is configurable per deployment. For fast-task workloads, 30 seconds may suffice. For heavy map tasks (Trino queries, Parquet exports), 120 seconds with a matching `terminationGracePeriodSeconds: 150` is reasonable.

---

## 4. Shutdown Coordinator

The shutdown coordinator is a CDI `@ApplicationScoped` bean that observes the Quarkus `ShutdownEvent`. It is the single entry point for all shutdown logic and runs the phases in strict order.

### 4.1 State Machine

```
RUNNING ──► DRAINING ──► RELEASING ──► TERMINATED
               │
               │ (drain timeout expired)
               ▼
           RELEASING ──► TERMINATED
```

- **RUNNING**: Normal operation. Workers claim tasks, leader orchestrates.
- **DRAINING**: No new claims. In-flight tasks run to completion (or until timeout).
- **RELEASING**: Uncompleted tasks are flipped back to `PENDING`. Leader lease is released.
- **TERMINATED**: All cleanup done. Process may exit.

The coordinator exposes its current state via a health endpoint for debugging:

```
GET /q/health/shutdown
→ { "state": "DRAINING", "inFlightTasks": 3, "drainDeadline": "2026-03-15T10:00:45Z" }
```

### 4.2 Phase Sequence

```
          ShutdownEvent received
                   │
                   ▼
     ┌─────────────────────────────┐
     │  Phase 0: Signal            │
     │  Set state = DRAINING       │
     │  Set drain deadline         │
     │  Readiness probe → false    │
     └──────────────┬──────────────┘
                    │
          ┌─────────▼──────────┐
          │ Am I the leader?   │
          └──┬──────────┬──────┘
            yes         no
             │           │
             ▼           │
     ┌───────────────┐   │
     │ Phase 1:      │   │
     │ Leader        │   │
     │ Teardown      │   │
     └───────┬───────┘   │
             │           │
             ▼           ▼
     ┌─────────────────────────────┐
     │  Phase 2: Worker Drain      │
     │  Await in-flight tasks      │
     │  (up to drain timeout)      │
     └──────────────┬──────────────┘
                    │
                    ▼
     ┌─────────────────────────────┐
     │  Phase 3: Release           │
     │  Flip uncompleted tasks     │
     │  back to PENDING            │
     └──────────────┬──────────────┘
                    │
                    ▼
     ┌─────────────────────────────┐
     │  Phase 4: Final             │
     │  Close Oracle connections   │
     │  Log summary, emit metrics  │
     │  state = TERMINATED         │
     └─────────────────────────────┘
```

---

## 5. Phase 0 — Signal

**Trigger:** Quarkus `ShutdownEvent` observed.

**Actions:**

1. Set coordinator state to `DRAINING`. This is an `AtomicReference` checked by the worker loop on every claim cycle.
2. Compute `drainDeadline = now + drainTimeout` (configurable, default 60 s).
3. Log: `"Shutdown initiated. Drain deadline: {drainDeadline}. In-flight tasks: {count}."`
4. Readiness probe switches to returning `503`. Kubernetes removes the pod from Service endpoints.

**What this prevents:** The worker loop checks the coordinator state before every `SELECT FOR UPDATE SKIP LOCKED`. Once `DRAINING` is set, no new tasks are claimed. The claim loop exits cleanly.

```kotlin
// In the worker loop's claim cycle
while (true) {
    if (shutdownCoordinator.state != RUNNING) {
        logger.info("Shutdown signaled, stopping claim loop")
        break
    }
    val task = claimNextTask(subscribedQueues)
    // ...
}
```

---

## 6. Phase 1 — Leader Teardown

**Applies to:** Only the pod currently holding the Kubernetes Lease.

**Problem:** The leader runs several background loops — barrier monitor, stale reaper, cleanup scheduler. These loops must stop before the lease is released. If the lease is released first, the loops might attempt Oracle writes without a valid fencing token. If the loops aren't stopped, the new leader and old leader could run overlapping orchestration.

### 6.1 Stop Orchestration Loops

All leader-side loops are launched as structured coroutines under a single `SupervisorJob` scope tied to leadership:

```kotlin
// LeaderLifecycle.kt
private var leaderScope: CoroutineScope? = null

fun onLeadershipAcquired(fencingToken: Long) {
    leaderScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    leaderScope!!.launch { barrierMonitorLoop(fencingToken) }
    leaderScope!!.launch { staleReaperLoop(fencingToken) }
    leaderScope!!.launch { cleanupLoop(fencingToken) }
}

fun onLeadershipRevoked() {
    leaderScope?.cancel("Leadership revoked — shutdown or lease lost")
    leaderScope = null
}
```

On shutdown, the coordinator calls `onLeadershipRevoked()`:

1. The `SupervisorJob` is cancelled.
2. Each loop's next suspension point (the `delay()` at the end of each polling interval) throws `CancellationException`.
3. Each loop's `finally` block logs its exit. No Oracle writes happen after cancellation.

**Timeout:** 5 seconds. If loops don't exit within 5 s (e.g., stuck in a long Oracle query), the coordinator proceeds anyway — the lease release will fence subsequent writes.

### 6.2 Release the Kubernetes Lease

After loops have stopped (or the 5 s timeout has elapsed):

1. The pod explicitly updates the Lease object, clearing its holder identity.
2. This triggers immediate lease availability — another pod's `LeaderElector` can acquire it without waiting for the lease duration to expire.

```kotlin
suspend fun releaseLeaseExplicitly(leaseName: String, namespace: String) {
    val leaseApi = client.leases().inNamespace(namespace)
    val lease = leaseApi.withName(leaseName).get()
    lease.spec.holderIdentity = null
    lease.spec.acquireTime = null
    leaseApi.withName(leaseName).patch(lease)
    logger.info("Lease released explicitly. New leader can acquire immediately.")
}
```

**Why explicit release matters:** Without it, the new leader must wait for the full `leaseDurationSeconds` (typically 15–30 s) to expire before acquiring. During that window, no barrier detection, no stale reclaiming, no reduce dispatching. Explicit release reduces this gap to sub-second.

### 6.3 Leader Teardown — What About In-Flight Transitions?

If the leader is mid-CAS (e.g., transitioning a job from RUNNING → REDUCING) when shutdown is signaled:

- The CAS is an atomic Oracle UPDATE. It either commits or doesn't.
- If it commits, the job is in REDUCING. The new leader will detect this and handle reduce dispatch.
- If it doesn't commit (transaction rolled back due to connection close), the job stays in RUNNING. The new leader's barrier monitor picks it up.

No special logic needed. The combination of atomic CAS and new-leader recovery handles all mid-transition states.

### 6.4 Leader Teardown Summary

```
Phase 1 (leader only, ≤ 5 s)
├── Cancel leaderScope (stops all orchestration coroutines)
├── Await coroutine termination (up to 5 s)
├── Release Kubernetes Lease explicitly
└── Log: "Leader teardown complete. Lease released."
```

After Phase 1, this pod is no longer the leader. It proceeds to Phase 2 as a plain worker.

---

## 7. Phase 2 — Worker Drain

**Applies to:** Every pod (including the former leader, which still has in-flight tasks from its worker loop).

**Goal:** Give in-flight tasks time to finish. Tasks that complete during the drain window follow the normal completion path — update status to `COMPLETED`, increment job counters, persist outputs. No special handling.

### 7.1 Drain Mechanism

The worker loop has already stopped claiming (Phase 0). What remains are tasks currently being executed by bulkhead slots:

```
Bulkhead (draining)
┌──────────────────────────────────┐
│  bulkhead = 4                    │
│                                  │
│  ┌──────┐ ┌──────┐              │
│  │ slot │ │ slot │  ← finishing  │
│  │  1   │ │  2   │    work      │
│  └──────┘ └──────┘              │
│  ┌──────┐ ┌──────┐              │
│  │ slot │ │ slot │  ← already   │
│  │  3   │ │  4   │    idle      │
│  └──────┘ └──────┘              │
│                                  │
│  No new claims. Waiting for      │
│  slots 1 & 2 to finish.         │
└──────────────────────────────────┘
```

The coordinator tracks in-flight tasks via a `Semaphore` or `AtomicInteger` that the bulkhead already maintains. Drain is a simple wait:

```kotlin
suspend fun awaitDrain(deadline: Instant) {
    while (bulkhead.activeCount > 0) {
        val remaining = Duration.between(Instant.now(), deadline)
        if (remaining.isNegative) {
            logger.warn("Drain timeout. {} tasks still in-flight.", bulkhead.activeCount)
            break
        }
        logger.info(
            "Draining: {} tasks in-flight. {} s remaining.",
            bulkhead.activeCount,
            remaining.seconds
        )
        delay(1.seconds)  // check every second
    }
}
```

### 7.2 What Happens to Tasks That Finish During Drain

Nothing special. The handler completes, the framework updates the task to `COMPLETED` (or `FAILED`), job counters are incremented — all the normal post-execution logic runs. The fact that the pod is draining doesn't affect the transaction.

### 7.3 What Happens to Tasks That Don't Finish

Tasks still running when the drain timeout expires proceed to Phase 3 (Release). They will be forcibly released.

### 7.4 Handler Cooperation (Optional but Recommended)

Handlers that perform long-running work (streaming from Trino, writing large Parquet files) can optionally check a cancellation signal:

```kotlin
interface TaskContext {
    /** Returns true if the pod is shutting down and the handler should wrap up. */
    val isShuttingDown: Boolean
}

// In a long-running handler:
class ParquetExportHandler : TaskHandler {
    override suspend fun handle(payload: JsonNode, ctx: TaskContext): TaskResult {
        val cursor = trinoClient.execute(query)
        val writer = parquetWriter(outputPath)
        for (batch in cursor) {
            if (ctx.isShuttingDown) {
                writer.abort()
                return TaskResult.retry(delay = Duration.ZERO)  // re-enqueue immediately
            }
            writer.writeBatch(batch)
        }
        writer.close()
        return TaskResult.success()
    }
}
```

This is cooperative, not mandatory. Handlers that don't check the signal simply run until completion or until Phase 3 forcibly releases them. But cooperative handlers avoid wasting drain time on work that will be thrown away anyway.

**Return semantics for cooperative exit:**

| Handler returns | Framework behavior |
|---|---|
| `TaskResult.retry(delay = ZERO)` | Task set back to PENDING immediately. No retry count increment. Available for instant reclaim by another pod. |
| `TaskResult.failure(...)` | Normal failure path. Retry count increments. May dead-letter if retries exhausted. |
| Handler doesn't return (still running at release) | Phase 3 handles it — see next section. |

---

## 8. Phase 3 — Release

**Applies to:** Every pod.

**Trigger:** Drain timeout has expired, or all in-flight tasks have completed (whichever comes first).

### 8.1 Forcible Release

Any tasks still in `CLAIMED` status with `claimed_by = {this pod}` are released:

```sql
UPDATE task
   SET status       = 'PENDING',
       claimed_by   = NULL,
       claimed_at   = NULL,
       scheduled_at = NULL       -- available immediately
 WHERE claimed_by   = :podId
   AND status       = 'CLAIMED'
```

**No retry count increment.** The task didn't fail — the pod is shutting down. The task should be picked up by another pod at the same retry count, as if nothing happened.

**Why not increment retry count?** Consider a rolling deployment of 4 pods with `max_retries = 3`. If each restart increments retry on in-flight tasks, a task could exhaust its retries just from deployments, not from actual failures.

### 8.2 How This Interacts with Coroutine Cancellation

When Phase 3 starts, in-flight handler coroutines may still be running. The sequence is:

1. Phase 3 executes the release UPDATE against Oracle. The task rows are now `PENDING`.
2. The handler coroutine is cancelled (via scope cancellation or structured concurrency teardown).
3. If the handler was mid-transaction when cancelled, the transaction is rolled back (standard JDBC/connection behavior on close).
4. Another pod claims the now-PENDING task and re-executes from scratch.

**Race condition:** What if a handler completes its transaction (sets task to `COMPLETED`) in the instant between Phase 3's SELECT and UPDATE? The WHERE clause (`status = 'CLAIMED'`) prevents the release UPDATE from touching it. No conflict.

### 8.3 Release and Map-Reduce Counters

If a released task was a map task, its `mr_job.completed_tasks` counter was **not** incremented (because the handler didn't finish the completion transaction). When another pod re-executes the task and completes it, the counter increments normally. No double-counting.

If a released task was a reduce task, the job stays in `REDUCING`. The stale reaper on the new leader or the barrier monitor will detect it and handle re-dispatch if needed, following the standard recovery path from the parent design doc (§7.2).

### 8.4 Release Summary

```
Phase 3 (≤ 5 s)
├── Cancel remaining handler coroutines
├── Execute release UPDATE (CLAIMED → PENDING for this pod)
├── Log: "Released {n} tasks back to PENDING."
└── Emit metric: shutdown_tasks_released{pod="..."} = n
```

---

## 9. Phase 4 — Final

**Actions:**

1. Close the Oracle `DataSource` / connection pool.
2. Emit final shutdown metrics:
    - `shutdown_duration_seconds` — total time from Phase 0 to now.
    - `shutdown_tasks_completed` — tasks that finished during drain.
    - `shutdown_tasks_released` — tasks forcibly released.
3. Log a structured shutdown summary:

```json
{
  "event": "shutdown_complete",
  "pod": "dispatch-worker-2",
  "wasLeader": true,
  "drainDurationMs": 34521,
  "tasksCompletedDuringDrain": 7,
  "tasksReleased": 2,
  "leaseReleasedCleanly": true
}
```

4. Set state to `TERMINATED`. The Quarkus shutdown hook returns, and the process exits.

---

## 10. Complete Shutdown Timeline

The following diagram shows a leader pod shutting down with 4 in-flight tasks, where 3 complete during drain and 1 is released:

```
time ──────────────────────────────────────────────────────────────────►

SIGTERM received
│
├─ preStop sleep (3 s)                    K8s removes pod from Service
│                                         endpoints during this window
│
├─ Phase 0: Signal (instant)              State → DRAINING
│   ├─ Claim loop stops
│   └─ Readiness → 503
│
├─ Phase 1: Leader Teardown (≤ 5 s)
│   ├─ Cancel barrier monitor loop
│   ├─ Cancel stale reaper loop
│   ├─ Cancel cleanup loop
│   └─ Release K8s Lease                  New leader acquires within ~1 s
│
├─ Phase 2: Worker Drain (up to 60 s)
│   │
│   │  t+0s   4 tasks in-flight
│   │  t+5s   task A completes            ✓ normal completion
│   │  t+12s  task B completes            ✓ normal completion
│   │  t+31s  task C completes            ✓ normal completion
│   │  t+60s  drain timeout               task D still running
│   │
│
├─ Phase 3: Release (≤ 5 s)
│   ├─ Cancel task D's coroutine
│   ├─ UPDATE task SET status='PENDING'   task D available for reclaim
│   │    WHERE claimed_by = this_pod
│   │    AND status = 'CLAIMED'
│   └─ Log: "Released 1 task"
│
├─ Phase 4: Final (≤ 2 s)
│   ├─ Close connection pool
│   ├─ Emit metrics
│   └─ Log shutdown summary
│
└─ Process exits                          Total: ~73 s (within 90 s budget)
```

---

## 11. Edge Cases

### 11.1 Shutdown During Fan-Out

The leader is mid-transaction inserting a `mr_job` row and 500 map tasks when shutdown is signaled.

**Outcome:** The transaction either commits fully (all 500 tasks + job row) or rolls back entirely (atomicity guarantee from Oracle). If committed, the tasks are claimed by other pods normally. If rolled back, the job never existed. No partial fan-out is possible.

### 11.2 Shutdown During Barrier CAS

The leader has detected `completed + failed == total` and is executing the CAS UPDATE (`RUNNING → REDUCING`) when shutdown is signaled.

**Outcome:** Same atomic transaction argument. Either the CAS commits (job is now REDUCING, new leader handles reduce dispatch) or it rolls back (job stays RUNNING, new leader re-detects barrier). Both are safe.

### 11.3 Two Pods Shutting Down Simultaneously

During a rolling deployment, pod A (old) is draining while pod B (new) is starting. Pod A holds tasks and is releasing them. Pod B's worker loop is starting to claim.

**No conflict.** Released tasks are set to `PENDING`. Pod B's `SKIP LOCKED` claim naturally picks them up. The released tasks flow through the system as if they were freshly enqueued.

### 11.4 Leader and Non-Leader Shutting Down at the Same Time

The leader pod and a worker pod both receive SIGTERM simultaneously (e.g., node drain).

**Outcome:** The leader releases its lease (Phase 1). A surviving pod acquires leadership. Both shutting-down pods release their tasks (Phase 3). The new leader's stale reaper provides a safety net in case any release fails. The system converges to a consistent state.

### 11.5 SIGKILL (Ungraceful)

The pod is killed without warning (OOM, `kill -9`, node failure).

**Outcome:** No shutdown protocol runs. Tasks remain `CLAIMED` with a stale `claimed_at`. The leader's stale reaper reclaims them after the configured timeout. This is the baseline behavior that graceful shutdown improves upon — not replaces.

### 11.6 Handler Ignores TaskContext.isShuttingDown

The handler doesn't cooperate and keeps running a 10-minute Trino query.

**Outcome:** The drain timeout expires. Phase 3 cancels the coroutine, which interrupts the JDBC call (if the JDBC driver supports interrupt — most do via `Statement.cancel()`). The Oracle release UPDATE runs. The task goes back to PENDING. The Trino query may continue server-side until Trino's own timeout kills it — this is outside the framework's control, but the task queue state is consistent.

---

## 12. Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `taskqueue.shutdown.drain-timeout` | `60s` | Max time to wait for in-flight tasks to complete |
| `taskqueue.shutdown.leader-teardown-timeout` | `5s` | Max time to wait for leader orchestration loops to stop |
| `taskqueue.shutdown.release-timeout` | `5s` | Max time for the release UPDATE to execute |
| `taskqueue.shutdown.log-interval` | `5s` | How often to log drain progress |
| `k8s.terminationGracePeriodSeconds` | `90` | Must be ≥ preStop + leader teardown + drain + release + margin |

**Constraint:** `terminationGracePeriodSeconds` must always exceed the sum of all phase timeouts plus the preStop duration. The deployment pipeline should validate this.

---

## 13. Observability

### 13.1 Metrics (Micrometer/Prometheus)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `taskqueue_shutdown_state` | Gauge (enum) | `pod` | Current shutdown state (0=RUNNING, 1=DRAINING, 2=RELEASING, 3=TERMINATED) |
| `taskqueue_shutdown_inflight_tasks` | Gauge | `pod` | Tasks still executing during drain |
| `taskqueue_shutdown_tasks_completed` | Counter | `pod` | Tasks that finished during drain |
| `taskqueue_shutdown_tasks_released` | Counter | `pod` | Tasks forcibly released in Phase 3 |
| `taskqueue_shutdown_duration_seconds` | Histogram | `pod`, `was_leader` | Total shutdown duration |
| `taskqueue_shutdown_drain_timeout_exceeded` | Counter | `pod` | Incremented if drain timed out (indicates drain timeout may be too short) |

### 13.2 Alerting Rules

```yaml
# Alert if pods frequently can't drain in time
- alert: TaskQueueDrainTimeoutFrequent
  expr: rate(taskqueue_shutdown_drain_timeout_exceeded_total[1h]) > 0.5
  for: 5m
  annotations:
    summary: "Pods frequently hitting drain timeout during shutdown"
    action: "Increase taskqueue.shutdown.drain-timeout or investigate slow handlers"

# Alert if many tasks are being released (not completing during drain)
- alert: TaskQueueHighReleaseRate
  expr: |
    sum(rate(taskqueue_shutdown_tasks_released_total[1h]))
    /
    sum(rate(taskqueue_shutdown_tasks_completed_total[1h])) > 0.3
  for: 10m
  annotations:
    summary: "More than 30% of tasks released during shutdown instead of completing"
    action: "Handlers may be too slow for current drain timeout"
```

### 13.3 Structured Logging

Every phase transition emits a structured log entry:

| Phase | Log Level | Key Fields |
|-------|-----------|------------|
| Phase 0 (Signal) | INFO | `event=shutdown_signal`, `inFlightTasks`, `drainDeadline` |
| Phase 1 (Leader) | INFO | `event=leader_teardown`, `loopsStopped`, `leaseReleased` |
| Phase 2 (Drain progress) | INFO | `event=drain_progress`, `remaining`, `elapsed` (every `log-interval`) |
| Phase 2 (Drain complete) | INFO | `event=drain_complete`, `tasksCompleted`, `durationMs` |
| Phase 2 (Drain timeout) | WARN | `event=drain_timeout`, `tasksStillRunning` |
| Phase 3 (Release) | INFO | `event=tasks_released`, `count` |
| Phase 4 (Final) | INFO | `event=shutdown_complete`, full summary object |

---

## 14. Testing Strategy

### 14.1 Unit Tests

| Test | Validates |
|------|-----------|
| Coordinator state transitions (RUNNING→DRAINING→RELEASING→TERMINATED) | State machine correctness |
| Claim loop exits when state is DRAINING | Phase 0 stops new claims |
| Release UPDATE sets correct status and clears claimed_by | Phase 3 SQL correctness |
| Release does not touch COMPLETED tasks | Race condition safety |
| Retry count is not incremented on release | Deployment-safety invariant |

### 14.2 Integration Tests (Testcontainers + Oracle)

| Test | Validates |
|------|-----------|
| Enqueue 10 tasks, claim 5, trigger shutdown, verify 5 complete + 0 released | Clean drain path |
| Enqueue 10 tasks, claim 5 (with slow handlers), trigger shutdown with 2 s drain timeout, verify N complete + (5-N) released | Timeout + release path |
| Start leader, trigger shutdown, verify lease is released, start second pod, verify it acquires lease | Leader handoff |
| Fan-out 20 map tasks, trigger leader shutdown mid-execution, verify new leader continues monitoring | Leader failover during map-reduce |
| Fan-out + trigger worker shutdown, verify released map tasks are re-claimed and job eventually completes | End-to-end map-reduce resilience |

### 14.3 Chaos / Manual Tests

| Scenario | Expected outcome |
|----------|------------------|
| `kubectl delete pod --grace-period=90` during active map-reduce | Tasks drain or release, job completes via other pods |
| `kubectl drain node` with 3 pods on same node | All 3 pods shut down gracefully, remaining pods absorb work |
| Rolling deployment (`kubectl rollout restart`) | Zero stale tasks, continuous throughput with brief dip |
| `kill -9` on leader pod | Ungraceful — stale reaper recovers within timeout (baseline behavior) |

---

## 15. Implementation Checklist

- [ ] `ShutdownCoordinator` bean with `@Observes ShutdownEvent`
- [ ] `ShutdownState` enum and `AtomicReference` exposed to worker loop
- [ ] Worker loop claim guard (check state before `SKIP LOCKED`)
- [ ] `TaskContext` interface with `isShuttingDown` property
- [ ] Leader coroutine scope cancellation in `LeaderLifecycle`
- [ ] Explicit Kubernetes Lease release via fabric8 client
- [ ] Release UPDATE query (CLAIMED → PENDING for this pod)
- [ ] Drain await loop with configurable timeout
- [ ] Shutdown health endpoint (`/q/health/shutdown`)
- [ ] Micrometer metrics registration
- [ ] Structured log events for each phase
- [ ] Quarkus configuration properties (`taskqueue.shutdown.*`)
- [ ] K8s manifest updates (`terminationGracePeriodSeconds`, `preStop`)
- [ ] Integration tests with Testcontainers
- [ ] Runbook entry for shutdown-related alerts