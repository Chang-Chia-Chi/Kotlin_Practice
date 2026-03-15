# Stale Task Reaper — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0  
**Depends on:** Event Bus, Fenced Leader Election

---

## 1. Problem Statement

When a pod dies ungracefully (OOM kill, SIGKILL, node failure), its in-flight tasks remain in `CLAIMED` status. No one is processing them. No one knows they're orphaned. They sit indefinitely until something intervenes.

Graceful shutdown (SIGTERM) handles the planned case — the shutdown coordinator releases tasks back to PENDING in Phase 3. But graceful shutdown doesn't run when:

- The pod is OOM-killed.
- The node loses power.
- `kill -9` is issued.
- The JVM crashes (segfault, native memory corruption).
- The drain timeout expires and SIGKILL follows.

In all these cases, the stale task reaper is the recovery mechanism. It is the safety net beneath graceful shutdown.

---

## 2. Goals & Non-Goals

### Goals

- **Recover orphaned tasks.** Tasks stuck in CLAIMED with a dead worker are returned to PENDING for re-execution by another pod.
- **Bounded recovery time.** A stale task is detected and reclaimed within a configurable window (default: 90 seconds from last heartbeat).
- **No false reclaims.** Tasks legitimately being worked on (including during graceful shutdown drain) are never reclaimed.
- **Fenced writes.** The reaper runs on the leader and its Oracle writes are fenced with the leader's epoch, preventing a zombie leader's reaper from interfering with the current leader.
- **Dead-letter exhausted tasks.** Tasks that have exhausted their retry budget during reclaim are moved directly to DEAD_LETTER, not cycled through PENDING again.

### Non-Goals

- Detecting slow handlers. A handler that takes 10 minutes but sends heartbeats is alive, not stale. Performance monitoring is a metrics/alerting concern.
- Notifying the dead pod. The pod is dead. The reaper acts on the database, not on pods.
- Redistributing tasks to specific pods. Released tasks go back to PENDING and are claimed by whoever is fastest via SKIP LOCKED.

---

## 3. Detection Strategy: Heartbeat-Based

### 3.1 Why Not Claimed-At Age

The simplest approach — flag tasks where `NOW - claimed_at > threshold` — creates an impossible tuning dilemma:

- Set the threshold short (e.g., 2 minutes) and you reclaim tasks that are legitimately slow (a 5-minute Trino query, a large Parquet export).
- Set the threshold long (e.g., 10 minutes) and crashed-pod tasks sit orphaned for 10 minutes.

There is no single threshold that works for both fast tasks (email.send, 5 seconds) and slow tasks (report.generate, 5 minutes). Claimed-at age conflates "how long has this task been running" with "is the worker still alive."

### 3.2 Heartbeat Mechanism

A heartbeat decouples liveness from duration.

**Worker side:** Each bulkhead slot runs a background heartbeat alongside the handler. Every `heartbeat_interval` (default 30 seconds), the worker updates the task's `last_heartbeat` column to the current timestamp. This is a lightweight single-row UPDATE by primary key.

**Reaper side:** The reaper checks `NOW - last_heartbeat > stale_threshold`. A task whose heartbeat is fresh (even if it's been running for an hour) is alive. A task whose heartbeat stopped updating (because the pod crashed) is stale.

**Threshold relationship:** `stale_threshold` should be at least 3× `heartbeat_interval` to tolerate transient delays (GC pauses, Oracle load spikes that delay the heartbeat UPDATE). Default: 90 seconds (3 × 30s).

### 3.3 Schema Addition

One column added to the task table:

| Column | Type | Default | Purpose |
|--------|------|---------|---------|
| last_heartbeat | TIMESTAMP | NULL | Updated periodically by the worker. NULL when task is not claimed. |

Set to current timestamp when the task is claimed (alongside `claimed_at`). Updated every `heartbeat_interval` while the handler executes. Cleared (set to NULL) when the task completes, fails, or is released.

### 3.4 Heartbeat Lifecycle

```
Task claimed
  │
  ├── last_heartbeat = NOW (set during claim)
  │
  ├── Handler executing
  │   ├── t+30s: last_heartbeat = NOW
  │   ├── t+60s: last_heartbeat = NOW
  │   ├── t+90s: last_heartbeat = NOW
  │   └── ...continues until handler returns
  │
  ├── Handler completes
  │   └── last_heartbeat = NULL (cleared during status update)
  │
  └── Pod crashes (no more heartbeats)
      └── last_heartbeat frozen at last update
          └── Reaper detects: NOW - last_heartbeat > 90s → stale
```

### 3.5 Heartbeat Failure Is Non-Fatal

If a heartbeat UPDATE fails (Oracle temporary unavailability, connection pool exhaustion), the handler continues executing. The heartbeat is a best-effort liveness signal, not a correctness requirement. Missing one heartbeat is tolerated by the 3× threshold. Missing three consecutive heartbeats will trigger reclaim — but if Oracle is unreachable for 90 seconds, the handler is likely failing anyway.

---

## 4. Reaper Design

### 4.1 Leader-Only Execution

The reaper runs exclusively on the leader pod as a scheduled coroutine within the leader scope. Reasons:

- The reaper writes to the task table (flips status, increments retry count). These writes must be fenced with the leader's epoch to prevent zombie-leader interference.
- Running on all pods would require coordination to prevent multiple pods from reclaiming the same task simultaneously. The leader pattern already solves this.
- The reaper's Oracle queries are lightweight (a single scan + batch update). There's no need to distribute the work.

The reaper starts when the pod acquires leadership (observes `LeadershipAcquired` event) and stops when leadership is lost (the leader scope is cancelled).

### 4.2 Scan-Reclaim Cycle

The reaper executes on a fixed interval (default: 30 seconds):

```
Reaper loop (every scan_interval)
  │
  ├── 1. Find stale tasks
  │      SELECT task_id, handler, claimed_by, retry_count, max_retries
  │      FROM task
  │      WHERE status = 'CLAIMED'
  │        AND last_heartbeat < NOW - stale_threshold
  │      ORDER BY last_heartbeat ASC
  │      FETCH FIRST :batchSize ROWS ONLY
  │
  ├── 2. For each stale task, reclaim:
  │      UPDATE task
  │      SET status         = PENDING or DEAD_LETTER (based on retry count),
  │          claimed_by     = NULL,
  │          claimed_at     = NULL,
  │          last_heartbeat = NULL,
  │          retry_count    = retry_count + 1 (if not dead-lettering),
  │          error_message  = 'Reclaimed: heartbeat stale (pod: {claimed_by})'
  │      WHERE task_id      = :taskId
  │        AND status       = 'CLAIMED'           -- still claimed (not completed in the meantime)
  │        AND last_epoch   <= :leaderEpoch        -- fencing
  │
  ├── 3. Check affected rows per UPDATE:
  │      1 row  → reclaim succeeded
  │      0 rows → task was completed or reclaimed by someone else (race, harmless)
  │
  ├── 4. Fire TaskReclaimed event for each successful reclaim
  │
  └── 5. If retry_count + 1 >= max_retries → status = DEAD_LETTER
         Fire TaskDeadLettered event
```

### 4.3 Batch Size

The reaper processes stale tasks in batches (default: 50 per scan) to avoid a single massive UPDATE that locks many rows. If more than `batchSize` tasks are stale in one scan, the reaper processes one batch and picks up the remainder on the next scan interval.

In a healthy system, the stale task count per scan should be zero or very low. A sustained high count indicates pod instability (frequent crashes) — the metrics and alerting layers should catch this.

### 4.4 Reclaim vs. Dead-Letter Decision

When reclaiming a task, the reaper checks whether the task has retries remaining:

| Condition | Action | Status set to |
|-----------|--------|---------------|
| `retry_count + 1 < max_retries` | Reclaim: increment retry_count, set status to PENDING | PENDING |
| `retry_count + 1 >= max_retries` | Dead-letter: set status to DEAD_LETTER | DEAD_LETTER |

This means a task can be dead-lettered by the reaper without ever being explicitly "failed" by a handler. Example: a task is claimed, the pod crashes, the reaper reclaims it (retry 1). It's claimed again, the pod crashes again, reclaimed again (retry 2). On the third crash (retry 3, max_retries = 3), the reaper dead-letters it. The handler never ran to completion — but the retry budget is exhausted.

---

## 5. Fencing

The reaper's UPDATE includes `AND last_epoch <= :leaderEpoch` in the WHERE clause. This is the standard database fence from the Fenced Leader Election pattern.

**Why it matters:** If the leader pod experiences a GC pause, loses its lease, and a new leader takes over, the old leader's reaper (if it wakes up) might try to reclaim tasks that the new leader has already handled. The epoch fence ensures only the current leader's reaper can write.

The `last_epoch` column on the task table serves double duty: it's used by the reaper for fenced reclaims, and it can be used by any leader-only operation that touches the task table.

---

## 6. Interaction with Graceful Shutdown

### 6.1 Draining Pods

During graceful shutdown, a pod is in Phase 2 (DRAINING). Its tasks are legitimately CLAIMED and being worked on. Their heartbeats continue updating (the heartbeat coroutine runs alongside the handler). The reaper doesn't need special logic — fresh heartbeats mean the tasks are not stale.

### 6.2 Phase 3 Release

If the draining pod's drain timeout expires, Phase 3 releases uncompleted tasks to PENDING. This happens before heartbeats stop. The reaper never sees these tasks as stale because they're released proactively.

### 6.3 SIGKILL During Drain

If a pod is killed mid-drain (e.g., `terminationGracePeriodSeconds` exceeded, K8s sends SIGKILL), heartbeats stop immediately. The reaper detects these tasks as stale after `stale_threshold` seconds and reclaims them. This is the correct behavior — the pod is dead.

### 6.4 Summary

| Shutdown scenario | Who recovers the tasks | Latency |
|-------------------|----------------------|---------|
| Graceful (SIGTERM, drain completes) | Graceful shutdown Phase 2 (tasks complete normally) | 0 (tasks finish) |
| Graceful (SIGTERM, drain timeout) | Graceful shutdown Phase 3 (explicit release) | Drain timeout (default 60s) |
| Ungraceful (SIGKILL after drain timeout) | Stale task reaper | stale_threshold (default 90s) |
| Ungraceful (OOM, node failure) | Stale task reaper | stale_threshold (default 90s) |

---

## 7. Interaction with Map-Reduce

When the reaper reclaims a map task (one with a `group_id`):

- The task goes back to PENDING. Another pod claims and executes it. On completion, `mr_job.completed_tasks` is incremented normally.
- The barrier logic (`completed + failed == total`) still works because the task was never counted as completed or failed — it was stuck in CLAIMED.

When the reaper dead-letters a map task (retries exhausted):

- The task is moved to DEAD_LETTER. `mr_job.failed_tasks` is incremented.
- The barrier may fire if this was the last outstanding task.
- The failure policy (FAIL_JOB, THRESHOLD, BEST_EFFORT) determines whether the job proceeds to reduce or fails.

No special Map-Reduce logic in the reaper. The reaper acts on individual tasks; the leader's barrier monitor interprets the counters.

---

## 8. Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `taskqueue.heartbeat.interval` | `30s` | How often workers update task heartbeats |
| `taskqueue.reaper.scan-interval` | `30s` | How often the reaper scans for stale tasks |
| `taskqueue.reaper.stale-threshold` | `90s` | Heartbeat age beyond which a task is stale. Must be ≥ 3× heartbeat interval. |
| `taskqueue.reaper.batch-size` | `50` | Max tasks reclaimed per scan cycle |

**Constraint:** `stale-threshold` ≥ 3 × `heartbeat.interval`. The framework should validate this at startup and fail fast if violated.

---

## 9. Observability

### 9.1 Metrics

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `taskqueue.reaper.reclaimed` | Counter | handler | Tasks reclaimed (returned to PENDING) |
| `taskqueue.reaper.dead_lettered` | Counter | handler | Tasks dead-lettered by reaper (retries exhausted) |
| `taskqueue.reaper.scan_duration` | Timer | — | Time per reaper scan cycle |
| `taskqueue.reaper.stale_age` | Histogram | handler | How long reclaimed tasks were stale (last_heartbeat age at reclaim time) |

### 9.2 Events

| Event | Consumers |
|-------|-----------|
| TaskReclaimed | Metrics, alerting |
| TaskDeadLettered (from reaper) | Dead letter processor, metrics, alerting |

### 9.3 Alerting

| Rule | Condition | Meaning |
|------|-----------|---------|
| Sustained reclaims | `rate(taskqueue.reaper.reclaimed[5m]) > 0` for 10 minutes | Pods are crashing regularly |
| High stale age | `taskqueue.reaper.stale_age` p95 > 3 minutes | Reaper scan interval may be too long, or heartbeat threshold too high |
| Reaper not scanning | `taskqueue.reaper.scan_duration` has no data points for > 2× scan_interval | Reaper coroutine may be dead (leader health issue) |

---

## 10. Testing Strategy

| Test | Validates |
|------|-----------|
| Claim task, stop heartbeat, wait stale_threshold, verify task is PENDING with retry_count+1 | Basic reclaim |
| Claim task, stop heartbeat, set retry_count = max_retries - 1, verify task is DEAD_LETTER | Dead-letter on exhausted retries |
| Claim task, keep heartbeat running for 5 minutes, verify task is NOT reclaimed | No false reclaim on long-running task |
| Claim task on pod A, complete it, verify reaper UPDATE returns 0 rows (no double-reclaim) | Race condition safety |
| Run reaper with stale epoch, verify UPDATE returns 0 rows | Fencing correctness |
| Reclaim a map task (with group_id), verify MR barrier still fires correctly | Map-reduce integration |
| Claim 200 tasks, kill pod, verify reaper processes in batches of 50 across 4 scans | Batch processing |
| Start graceful shutdown, verify heartbeats continue during drain, reaper does not interfere | Graceful shutdown compatibility |
