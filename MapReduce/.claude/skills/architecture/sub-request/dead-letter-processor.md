# Dead Letter Processor — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0  
**Depends on:** Event Bus

---

## 1. Problem Statement

When a task exhausts its retries, the framework flips it to `DEAD_LETTER` and moves on. From the framework's perspective, the task is resolved. From an operational perspective, it's an unresolved failure sitting in a database table with no mechanism to:

- **See it.** There's no API to query dead-lettered tasks by handler, time range, error pattern, or group. Discovery is accidental — someone notices a report didn't generate, a notification wasn't sent.
- **Act on it.** There's no way to replay a dead-lettered task (move it back to PENDING with reset retries) without manual SQL.
- **Be alerted about it.** There's no threshold evaluation — 500 tasks could dead-letter overnight and no one would know until morning.

The dead letter processor fills these three gaps: inspection, replay, and alerting.

---

## 2. Goals & Non-Goals

### Goals

- **Inspection.** Query dead-lettered tasks by handler, time range, error message pattern, and group_id. View individual tasks with full payload for debugging.
- **Replay.** Move dead-lettered tasks back to PENDING — individually, by filter, or by map-reduce job — with configurable retry limits and scheduling.
- **Alerting.** Threshold-based notifications when dead-letter rates exceed configured limits per handler.
- **Retention.** Automatic cleanup of old dead-lettered tasks after a configurable period.

### Non-Goals

- Root cause analysis. The processor surfaces the data; a human or external system diagnoses the cause.
- Automatic replay. The processor provides the mechanism; the decision to replay is manual or policy-driven by external automation.
- Message queue semantics. Dead-lettered tasks are database rows, not messages. There's no consumer group, no acknowledgment protocol.

---

## 3. Inspection API

Stateless REST endpoints backed by Oracle queries. Any pod can serve these — no leader requirement.

### 3.1 List Dead-Lettered Tasks

```
GET /api/dead-letters
    ?handler=dispatch.map          (optional: filter by handler)
    &groupId=abc-123               (optional: filter by map-reduce job)
    &since=2026-03-14T00:00:00Z    (optional: created after this time)
    &until=2026-03-15T00:00:00Z    (optional: created before this time)
    &errorPattern=%ORA-04031%      (optional: LIKE match on error_message)
    &limit=50                      (default: 50, max: 200)
    &offset=0                      (pagination)
```

Returns: paginated list of dead-lettered tasks with taskId, handler, queue, groupId, retryCount, errorMessage, createdAt, deadLetteredAt. Payload is excluded from list responses for performance (payloads can be large).

### 3.2 Get Single Task Detail

```
GET /api/dead-letters/{taskId}
```

Returns: full task row including payload and metadata. This is the debugging endpoint — an operator looks at the payload to understand what went wrong.

### 3.3 Summary / Aggregation

```
GET /api/dead-letters/summary
    ?since=2026-03-14T00:00:00Z
```

Returns: dead-letter counts grouped by handler, and separately by group_id (for map-reduce jobs). Each group includes count, latest error, and time range. This is the dashboard endpoint — one call gives the operator a picture of what's failing and how badly.

### 3.4 Error Pattern Grouping

```
GET /api/dead-letters/errors
    ?handler=dispatch.map
    &since=2026-03-14T00:00:00Z
```

Returns: dead-lettered tasks grouped by error message pattern (first 200 characters of `error_message`), with count per pattern. This distinguishes:

- "142 tasks failed with `ORA-04031: unable to allocate shared memory`" — systemic issue, one root cause.
- "142 tasks failed with 142 different errors" — heterogeneous bugs, each task has a unique problem.

The distinction changes the response: systemic issues need infrastructure fixes before replay; heterogeneous issues need per-task investigation.

---

## 4. Replay API

### 4.1 Replay Single Task

```
POST /api/dead-letters/{taskId}/replay
Content-Type: application/json

{
  "maxRetries": 5,           (optional: override, default: keep original)
  "scheduledAt": null         (optional: null = immediate, or ISO timestamp for delayed replay)
}
```

**What happens in Oracle:**

1. UPDATE the task: status → PENDING, retry_count → 0, error_message → NULL, scheduled_at → provided value or NULL.
2. WHERE clause includes `AND status = 'DEAD_LETTER'` — if the task was already replayed (race condition), returns 409 Conflict.

**Returns:** 200 OK with the updated task, or 409 Conflict.

### 4.2 Bulk Replay by Filter

```
POST /api/dead-letters/replay
Content-Type: application/json

{
  "filter": {
    "handler": "dispatch.map",
    "groupId": "job-456",
    "since": "2026-03-14T10:00:00Z",
    "errorPattern": "%ORA-04031%"
  },
  "maxRetries": 5,
  "scheduledAt": null
}
```

**What happens in Oracle:**

1. UPDATE all matching tasks: same transformation as single replay.
2. WHERE clause combines the filter with `AND status = 'DEAD_LETTER'`.

**Returns:** count of tasks replayed. Tasks that were already replayed (status changed between query and update) are naturally excluded by the WHERE clause.

### 4.3 Replay and Map-Reduce Counter Consistency

This is the most important design subtlety in the dead letter processor.

When a map task is dead-lettered, the framework increments `mr_job.failed_tasks`. The barrier fires when `completed_tasks + failed_tasks == total_tasks`. If we replay the task (moving it back to PENDING) without adjusting the counter, the math breaks:

- Before replay: completed=8, failed=2, total=10. Barrier met (8+2=10). Job may have already reduced.
- After replay (naive): The 2 tasks re-execute and complete. completed=10, failed=2, total=10. Now completed + failed = 12 > 10.

**Solution:** The replay transaction must atomically decrement `mr_job.failed_tasks` by the number of replayed tasks:

1. UPDATE matching tasks to PENDING.
2. UPDATE mr_job SET failed_tasks = failed_tasks - :replayedCount WHERE job_id = :groupId AND status = 'RUNNING'.
3. Both in a single Oracle transaction.

If the job is already in COMPLETED or FAILED status, the replay needs to also transition the job back to RUNNING (see §4.4).

### 4.4 Replay for Failed Jobs

When a map-reduce job fails (failure policy threshold exceeded), the job is in FAILED status. Replaying its dead-lettered tasks requires resurrecting the job:

```
POST /api/dead-letters/replay-job/{jobId}
```

**Transaction:**

1. Validate the job is in FAILED status.
2. Replay all DEAD_LETTER tasks for that group_id (same as bulk replay).
3. Decrement failed_tasks by the replayed count.
4. Transition job from FAILED → RUNNING (CAS with version check for optimistic locking).

Once the job is back in RUNNING, the leader's barrier monitor resumes polling it. When the replayed tasks complete, the barrier fires and reduce is dispatched.

**Guard:** If the job is in COMPLETED status (reduce already ran), replaying dead-lettered map tasks is usually wrong — the reduce output was based on partial data, and re-running map tasks won't retroactively fix it. The API should reject replay for COMPLETED jobs, or require an explicit `force: true` flag with a warning that the existing reduce output will be stale.

---

## 5. Alerting

### 5.1 Event-Driven Evaluation

The alerting layer subscribes to `TaskDeadLettered` events from the event bus. It does not poll the database — every dead-letter is observed in real time as it happens.

### 5.2 Alert Rules

Configured per handler or globally:

| Field | Example | Purpose |
|-------|---------|---------|
| handler | `"dispatch.map"` or `"*"` (catch-all) | Which handler this rule applies to |
| threshold | 10 | Dead-letter count to trigger the alert |
| window | 5 minutes | Sliding window for counting |
| severity | critical / warning / info | Alert priority |

Multiple rules can match the same handler. A handler-specific rule and a catch-all `"*"` rule can both fire independently.

### 5.3 Sliding Window Counter

For each handler, the alerting layer maintains an in-memory sliding window counter. On each `TaskDeadLettered` event:

1. Increment the counter for that handler.
2. If the count within the window exceeds the threshold, fire an alert.
3. After firing, reset the counter to prevent alert storms (one alert per threshold crossing, not one per dead-letter after threshold).

The counter is in-memory only — it resets on pod restart. This is acceptable for alerting: a pod restart clears the window, but if the problem persists, the counter will re-accumulate quickly.

### 5.4 Alert Delivery

Alerts are delivered via an `AlertSink` abstraction. The framework provides multiple implementations, all active simultaneously:

| Sink | Mechanism | When to use |
|------|-----------|-------------|
| Prometheus | Increment a labeled counter. Alertmanager evaluates rules. | Standard monitoring stack. |
| Slack webhook | POST to a configured webhook URL. | Immediate team visibility. |
| Structured log | Log at WARN/ERROR with structured fields. | Minimal setup; relies on log aggregation. |

The sink choice is deployment-specific. The alert evaluation logic is independent of delivery.

---

## 6. Retention and Cleanup

Dead-lettered tasks should not accumulate indefinitely. The cleanup mechanism:

- **Retention period:** Configurable, default 30 days.
- **Cleanup execution:** A scheduled task on the task queue itself, with handler `"system.dead-letter-cleanup"`. The leader enqueues it periodically (daily). It deletes DEAD_LETTER tasks older than the retention period.
- **Archive before delete (optional):** For compliance or post-mortem analysis, export dead-lettered tasks to a `dead_letter_archive` table or object storage (MinIO) before purging. The cleanup handler can be configured to archive or delete directly.

The cleanup task is itself a task on the queue — it gets all standard queue guarantees (retry, dead-letter, monitoring). Self-referential and elegant.

---

## 7. Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `taskqueue.dead-letter.retention-days` | `30` | Days to keep dead-lettered tasks before cleanup |
| `taskqueue.dead-letter.cleanup-schedule` | `daily` | How often to run the cleanup task |
| `taskqueue.dead-letter.archive-before-delete` | `false` | Whether to archive to MinIO/table before deletion |
| `taskqueue.dead-letter.alerts[].handler` | — | Handler name or `*` for catch-all |
| `taskqueue.dead-letter.alerts[].threshold` | `10` | Dead-letter count to trigger alert |
| `taskqueue.dead-letter.alerts[].window` | `5m` | Sliding window duration |
| `taskqueue.dead-letter.alerts[].severity` | `warning` | Alert severity |

---

## 8. Observability

### 8.1 Metrics

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `taskqueue.deadletter.total` | Gauge | handler | Current count of dead-lettered tasks |
| `taskqueue.deadletter.replayed` | Counter | handler | Tasks replayed (returned to PENDING) |
| `taskqueue.deadletter.alerts_fired` | Counter | handler, severity | Alert threshold crossings |
| `taskqueue.deadletter.cleaned` | Counter | — | Tasks purged by cleanup |

### 8.2 Dashboard Queries

The inspection API (§3.3 Summary endpoint) doubles as the data source for operational dashboards. A Grafana panel can poll `/api/dead-letters/summary` and display dead-letter counts by handler over time.

For deeper Prometheus-native dashboards, the metrics middleware already emits `taskqueue.handler.executions` with `result=DEAD_LETTERED` — the dead letter processor's Prometheus metrics are supplementary, not primary.

---

## 9. Testing Strategy

| Test | Validates |
|------|-----------|
| Dead-letter 5 tasks, query via list API, verify all returned with correct fields | Inspection API |
| Dead-letter a task, replay via single replay API, verify status is PENDING with retry_count=0 | Single replay |
| Dead-letter 50 tasks for one handler, bulk replay with handler filter, verify all replayed | Bulk replay |
| Dead-letter 3 map tasks, replay, verify mr_job.failed_tasks decremented by 3 | MR counter consistency |
| Fail a map-reduce job, replay via replay-job endpoint, verify job transitions to RUNNING | Job resurrection |
| Fire 10 TaskDeadLettered events within 5 minutes, verify alert fires | Alert threshold |
| Fire 9 TaskDeadLettered events, verify alert does NOT fire | Below-threshold silence |
| Wait retention period, verify cleanup task deletes old dead-lettered tasks | Retention cleanup |
| Attempt replay on a COMPLETED job, verify rejection (409) | Guard against stale replay |
