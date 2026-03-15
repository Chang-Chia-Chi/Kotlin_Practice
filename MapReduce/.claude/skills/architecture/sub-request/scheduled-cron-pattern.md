# Scheduled / Cron Pattern — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0  
**Layer:** 2 (Orchestration Pattern)

---

## 1. Problem Statement

The framework's task queue processes tasks that are enqueued explicitly. But many workloads need to run on a schedule: hourly data syncs, daily report generation, nightly cleanup, periodic health checks against external systems.

Quarkus already has `@Scheduled` for cron-like execution. But `@Scheduled` runs on every pod simultaneously. In a 3-pod deployment, a scheduled job fires 3 times. Combined with the fenced leader interceptor (`@FencedLeader`), only the leader's execution proceeds — the other two throw `NotLeaderException` silently. This works but has drawbacks:

- **Wasted computation.** Two pods do the pre-check dance every tick.
- **No retry.** If the leader's execution fails, there's no retry until the next cron tick.
- **No dead-letter.** A repeatedly failing scheduled job is invisible to the dead letter processor.
- **No observability.** Scheduled execution doesn't flow through the handler execution pipeline — no metrics, no tracing, no circuit breakers.
- **Tight coupling to leader.** The scheduled job always runs on the leader. If the leader is busy with a large map-reduce fan-out, the scheduled job competes for the same thread/coroutine resources.

The Scheduled/Cron pattern solves these by making scheduled work flow through the generic task queue. The schedule determines *when* to enqueue; the queue determines *where* and *how* to execute.

---

## 2. Core Concept

A scheduled trigger is a leader-side loop that periodically enqueues a task into the generic task queue. Once enqueued, the task is executed by any pod's worker loop — it gets all standard queue guarantees: retry, dead-letter, pipeline middleware, bulkhead-controlled parallelism, heartbeats, stale reclaim.

```
┌──────────────────────────────────────────────────┐
│  Leader                                          │
│                                                  │
│  Cron Trigger Loop                               │
│  ┌──────────────────────────────────────────┐    │
│  │  Every interval or cron expression:      │    │
│  │  1. Check: is it time to fire?           │    │
│  │  2. Check: is previous execution done?   │    │
│  │  3. Enqueue task into task table         │    │
│  └──────────────────────────────────────────┘    │
│                                                  │
└──────────────────────────────────────────────────┘
         │
         │  INSERT into task table
         ▼
┌──────────────────────────────────────────────────┐
│  Generic Task Queue (Layer 1)                    │
│                                                  │
│  Any pod claims and executes via SKIP LOCKED     │
│  Handler execution pipeline wraps it             │
│  Retry, dead-letter, heartbeat all apply         │
└──────────────────────────────────────────────────┘
```

---

## 3. Schedule Definition

A schedule definition is a named configuration that tells the trigger loop what to enqueue and when.

| Field | Purpose |
|-------|---------|
| name | Unique identifier (e.g., `"daily-report"`, `"hourly-sync"`) |
| handler | Task handler routing key (e.g., `"report.generate"`, `"sync.run"`) |
| schedule | Cron expression or fixed interval (e.g., `"0 0 2 * * ?"` for 2 AM daily, or `"every 1h"`) |
| payload | Static JSON payload to include in the enqueued task (parameters for the handler) |
| queue | Target queue (default: `"default"`) |
| maxRetries | Retry limit for each enqueued task (default: 3) |
| overlap policy | What to do if the previous execution hasn't finished (see §4) |
| enabled | Whether this schedule is active |

Schedule definitions are stored in Oracle (a configuration table) and read by the leader's trigger loop. They can be managed via the configuration API — creating, updating, enabling, or disabling schedules at runtime without redeployment.

---

## 4. Overlap Policy

A critical design decision: what happens when the cron ticks but the previous task is still running?

| Policy | Behavior | Use case |
|--------|----------|----------|
| **SKIP** | Do not enqueue a new task. Wait for the current one to complete. Next tick re-evaluates. | Default. Prevents pile-up for slow jobs. |
| **ENQUEUE** | Enqueue regardless. Multiple instances may run concurrently. | When each execution is independent and idempotent (e.g., polling an external API). |
| **REPLACE** | Cancel (dead-letter) the running task and enqueue a new one. | When only the latest execution matters (e.g., cache refresh where stale runs are worthless). |

The SKIP policy requires the trigger to check whether a task for this schedule is currently in PENDING or CLAIMED status. This is a simple query: `SELECT 1 FROM task WHERE handler = :handler AND metadata ->> 'scheduleName' = :name AND status IN ('PENDING', 'CLAIMED')`.

---

## 5. Execution Tracking

### 5.1 Schedule State Table

```
cron_schedule
┌──────────────────────┬────────────────────────────────────┐
│ Column               │ Purpose                            │
├──────────────────────┼────────────────────────────────────┤
│ schedule_id          │ Primary key (UUID)                 │
│ name                 │ Unique schedule name               │
│ handler              │ Task handler routing key           │
│ cron_expression      │ Cron or interval expression        │
│ payload              │ Static JSON payload                │
│ queue                │ Target queue                       │
│ max_retries          │ Per-task retry limit               │
│ overlap_policy       │ SKIP / ENQUEUE / REPLACE           │
│ enabled              │ Active flag                        │
│ last_fired_at        │ Last time a task was enqueued      │
│ last_completed_at    │ Last time an enqueued task finished │
│ last_task_id         │ Most recently enqueued task ID     │
│ last_status          │ Outcome of most recent execution   │
│ next_fire_at         │ Precomputed next fire time         │
│ version              │ Optimistic lock                    │
│ created_at           │ Timestamp                          │
│ updated_at           │ Timestamp                          │
└──────────────────────┴────────────────────────────────────┘
```

The `last_*` columns are updated by the trigger loop (on enqueue) and by an event observer (on task completion). `next_fire_at` is precomputed after each fire for efficient "what's due?" queries.

### 5.2 Task Metadata

When the trigger enqueues a task, it sets `metadata` on the task row to link it back to the schedule:

| Field (in metadata JSON) | Purpose |
|--------------------------|---------|
| scheduleName | Which schedule produced this task |
| scheduleId | FK back to cron_schedule |
| fireTime | The logical cron tick time |
| sequenceNumber | Monotonic counter for ordering |

This metadata allows the overlap policy check, the inspection API, and the completion observer to correlate tasks back to their schedule.

---

## 6. Trigger Loop

The trigger loop runs on the leader as a coroutine in the leader scope. It starts when `LeadershipAcquired` fires and stops when the scope is cancelled.

### 6.1 Tick Logic

On each tick (fixed interval, e.g., every 10 seconds):

1. Query all enabled schedules where `next_fire_at <= NOW`.
2. For each due schedule:
   a. Evaluate the overlap policy. If SKIP and a task is in-flight, skip.
   b. Enqueue a new task into the task table with the schedule's handler, payload, queue, and metadata.
   c. Update `cron_schedule`: set `last_fired_at = NOW`, `last_task_id = new task ID`, compute `next_fire_at`.
3. All of step 2 (enqueue + schedule update) in a single Oracle transaction per schedule.

### 6.2 Leader Failover

If the leader dies and a new leader takes over, the trigger loop restarts. The new leader reads `next_fire_at` from Oracle and picks up where the old leader left off. No missed fires (unless the leadership gap exceeds the schedule interval).

If the gap exceeds the interval (e.g., leader down for 2 hours, hourly schedule), the trigger fires once on recovery — it does not backfill missed ticks. Backfilling is dangerous: a schedule that runs "every 5 minutes" doesn't need 24 executions dumped at once after a 2-hour outage.

### 6.3 Fencing

The trigger loop's writes to `cron_schedule` (updating `last_fired_at`, `next_fire_at`) are fenced with the leader's epoch. This prevents a zombie leader from enqueuing duplicate tasks.

---

## 7. Completion Observer

When a scheduled task completes (the pipeline fires a `TaskCompleted` event), an observer checks the task's metadata for a `scheduleId`. If present, it updates the `cron_schedule` row:

- `last_completed_at = NOW`
- `last_status = result type (SUCCESS, FAILED, DEAD_LETTERED)`

This provides a persistent record of each schedule's execution history without requiring a separate execution log table.

---

## 8. Configuration API Extensions

The existing configuration API gains CRUD endpoints for schedules:

```
GET    /api/schedules                  — list all schedules
GET    /api/schedules/{id}             — get schedule detail + execution history
POST   /api/schedules                  — create a new schedule
PUT    /api/schedules/{id}             — update schedule (cron, payload, policy)
PATCH  /api/schedules/{id}/enable      — enable
PATCH  /api/schedules/{id}/disable     — disable
POST   /api/schedules/{id}/trigger     — fire immediately (bypass cron, enqueue now)
```

The manual trigger endpoint is valuable for testing and for "run it now" operational needs without waiting for the next cron tick.

---

## 9. Interaction with Other Framework Components

| Component | Interaction |
|-----------|-------------|
| **Generic task queue** | Trigger enqueues tasks. Execution uses all queue guarantees. |
| **Handler execution pipeline** | Scheduled tasks flow through the full middleware chain — metrics, tracing, circuit breaker, timeout. |
| **Dead letter processor** | Failed scheduled tasks are dead-lettered and visible in the inspection/replay API. |
| **Stale task reaper** | If the pod executing a scheduled task crashes, the reaper reclaims it. |
| **Event bus** | Trigger observes `LeadershipAcquired` to start. Completion observer listens for `TaskCompleted` to update schedule state. |

---

## 10. Observability

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `taskqueue.schedule.fires` | Counter | schedule_name | Times each schedule has fired |
| `taskqueue.schedule.skipped` | Counter | schedule_name | Times fire was skipped (overlap policy) |
| `taskqueue.schedule.last_duration` | Gauge | schedule_name | Duration of most recent execution |
| `taskqueue.schedule.overdue` | Gauge | schedule_name | Seconds past `next_fire_at` without firing (alert if > interval) |

---

## 11. Testing Strategy

| Test | Validates |
|------|-----------|
| Create schedule with 5s interval, wait 15s, verify 3 tasks were enqueued | Basic trigger |
| Create schedule with SKIP overlap, make handler sleep 10s, verify only 1 task at a time | Overlap policy: SKIP |
| Create schedule with ENQUEUE overlap, make handler sleep 10s, verify multiple concurrent tasks | Overlap policy: ENQUEUE |
| Kill leader mid-schedule, new leader takes over, verify no missed fire | Leader failover |
| Disable schedule via API, verify no more tasks enqueued | Enable/disable |
| Manually trigger via API, verify task enqueued immediately | Manual trigger |
| Schedule fires, task dead-letters, verify `last_status` updated to DEAD_LETTERED | Completion observer |
