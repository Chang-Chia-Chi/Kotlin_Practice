# Fan-Out (No Reduce) Pattern — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0  
**Layer:** 2 (Orchestration Pattern)

---

## 1. Problem Statement

Map-Reduce handles the case where parallel work fans out and then converges into a single reduce step. But many workloads fan out without needing convergence:

- **Notification broadcast:** Send push notifications to 10,000 users. Each send is independent. There's no reduce — you just want to know when they're all done.
- **Batch validation:** Validate 500 configuration records independently. Report how many passed and how many failed, but there's no aggregation step.
- **Cache warming:** Pre-populate caches for 200 tenants after a deployment. Each tenant is independent. Completion means "all tenants are warm."
- **Parallel deletion:** Purge records from 50 partitions. Each partition is independent. No reduce — just confirm all partitions are cleaned.

Using Map-Reduce for these is overkill: you'd implement a no-op reduce that does nothing. The fan-out pattern is Map-Reduce minus the reduce step, with a simpler lifecycle and lower overhead.

---

## 2. Core Concept

Fan-out is parallel task execution with completion tracking and no convergence step. The leader splits work into N tasks, workers execute them independently, and the leader monitors counters to detect completion. When all tasks are done, the job is marked complete and an optional callback fires.

```
┌──────────────────────────────────────────────────┐
│  Fan-Out Job: "notify-all-users"                 │
│                                                  │
│  Split: generate 10,000 notification tasks       │
│                                                  │
│  ┌──────┐ ┌──────┐ ┌──────┐       ┌──────┐     │
│  │task 1│ │task 2│ │task 3│  ...  │task N│      │
│  └──┬───┘ └──┬───┘ └──┬───┘       └──┬───┘     │
│     │        │        │              │          │
│     ▼        ▼        ▼              ▼          │
│  [execute] [execute] [execute]    [execute]     │
│     │        │        │              │          │
│     ▼        ▼        ▼              ▼          │
│  completed + failed == total → JOB COMPLETE     │
│                                                  │
│  Optional: onCompleted callback                  │
│  (no reduce — just a notification/summary)       │
└──────────────────────────────────────────────────┘
```

---

## 3. Relationship to Map-Reduce

Fan-out is a strict subset of Map-Reduce. The comparison:

| Aspect | Map-Reduce | Fan-Out |
|--------|-----------|---------|
| Split phase | ✅ Leader splits into N tasks | ✅ Identical |
| Parallel execution | ✅ Workers claim and execute | ✅ Identical |
| Intermediate outputs | ✅ Map tasks produce `mr_output` records | ❌ No intermediate outputs |
| Barrier detection | ✅ Leader monitors counters | ✅ Identical |
| Reduce phase | ✅ Single reduce task processes all outputs | ❌ No reduce |
| OnCompleted callback | ✅ Called after reduce | ✅ Called after barrier (no reduce in between) |
| Job table | `mr_job` | `fanout_job` (simpler — no reduce-related columns) |

The key simplification: no `mr_output` table interaction, no reduce task, no REDUCING state. The job goes directly from RUNNING to COMPLETED when the barrier is met.

---

## 4. Fan-Out Definition

A developer implements a fan-out definition with three methods:

| Method | Executed by | Purpose |
|--------|-------------|---------|
| **Split** | Leader | Given job parameters, produce the list of task inputs |
| **Execute** | Worker (via handler) | Process a single task input |
| **OnCompleted** | Leader (inline, not a task) | Optional: post-completion action (send summary, update status, log) |

The framework registers one handler per definition: `"{jobType}.execute"`. There is no reduce handler.

**Difference from Map-Reduce's OnCompleted:** In Map-Reduce, OnCompleted runs inside the reduce task (on a worker). In Fan-Out, OnCompleted runs on the leader as a lightweight inline callback after the barrier is met — because there's no reduce task to carry it. This means OnCompleted should be fast and non-blocking (log, enqueue a follow-up task, update a status row). If OnCompleted needs heavy processing, it should enqueue a standalone task.

---

## 5. Job Table

```
fanout_job
┌─────────────────────┬─────────────────────────────────────────┐
│ Column              │ Purpose                                 │
├─────────────────────┼─────────────────────────────────────────┤
│ job_id              │ Primary key (UUID) — group_id in tasks  │
│ job_type            │ Definition routing key                  │
│ status              │ CREATED / RUNNING / COMPLETED / FAILED  │
│ job_params          │ Input to Split (JSON)                   │
│ total_tasks         │ N (set at fan-out)                      │
│ completed_tasks     │ Atomically incremented by workers       │
│ failed_tasks        │ Atomically incremented on dead-letter   │
│ failure_policy      │ FAIL_JOB / THRESHOLD / BEST_EFFORT     │
│ failure_threshold   │ Max failure ratio (for THRESHOLD)       │
│ result_summary      │ Summary from OnCompleted (JSON)         │
│ version             │ Optimistic lock                         │
│ created_at          │ Timestamp                               │
│ updated_at          │ Timestamp                               │
└─────────────────────┴─────────────────────────────────────────┘
```

Compared to `mr_job`, this table has no `reducing_fence_token` (no reduce phase) and no REDUCING state. The lifecycle is simpler.

---

## 6. Job State Machine

```
  ┌─────────┐   atomic     ┌─────────┐   barrier met   ┌───────────┐
  │ CREATED ├─────────────►│ RUNNING ├────────────────►│ COMPLETED │
  └─────────┘  fan-out     └────┬────┘   + onCompleted └───────────┘
                                │
                                │ failure policy
                                │ threshold exceeded
                                ▼
                           ┌─────────┐
                           │ FAILED  │
                           └─────────┘
```

No REDUCING state. The barrier detection triggers OnCompleted directly (inline on the leader), then transitions to COMPLETED.

---

## 7. Detailed Flow

### Phase 1 — Fan-Out (identical to Map-Reduce)

1. Leader calls the definition's **Split** method.
2. In a single Oracle transaction:
   - Insert a `fanout_job` row (status = RUNNING, total_tasks = N).
   - Insert N tasks into the generic `task` table with `handler = "{jobType}.execute"`, `group_id = job_id`.
3. Return Job ID.

### Phase 2 — Parallel Execution

Workers claim tasks via SKIP LOCKED. Each `"{jobType}.execute"` handler:

1. Deserializes the input from the payload.
2. Calls the definition's **Execute** method.
3. On completion, atomically increments `fanout_job.completed_tasks`.
4. On dead-letter, `fanout_job.failed_tasks` is incremented.

No intermediate outputs are stored. Each task's work is self-contained.

### Phase 3 — Barrier Detection + Completion

The leader's monitoring loop reads job counters. When `completed_tasks + failed_tasks == total_tasks`:

1. Evaluate the failure policy:
   - FAIL_JOB: any dead-letter → job fails.
   - THRESHOLD: if `failed / total > X%` → job fails.
   - BEST_EFFORT: always proceed.
2. If the policy passes: call **OnCompleted** inline, transition job to COMPLETED.
3. If the policy fails: transition job to FAILED.

The barrier detection reuses the same leader monitoring loop as Map-Reduce. The loop polls both `mr_job` and `fanout_job` tables.

---

## 8. Why a Separate Table?

The fan-out job could reuse `mr_job` by leaving reduce-related columns null. But a separate `fanout_job` table is clearer:

- No null reduce columns cluttering the schema.
- No REDUCING state in the state machine.
- The barrier monitoring query is cleaner (no need to filter by pattern type).
- Schema evolution is independent — adding fan-out-specific columns doesn't affect Map-Reduce.

The cost is minimal: one additional table, one additional monitoring query in the leader loop.

---

## 9. Failure Handling and Recovery

Fan-out inherits all of Map-Reduce's reliability guarantees at the task level (retry, dead-letter, stale reclaim, heartbeat). At the job level:

**Worker crash:** The stale task reaper reclaims the task. Another worker re-executes it. The counter increments normally on completion.

**Leader crash:** The new leader's recovery loop inspects in-flight fan-out jobs:
- Jobs in RUNNING: Resume monitoring. If the barrier is met, transition to COMPLETED.
- No REDUCING state to worry about (simplification over Map-Reduce).

**Partial failure:** Same failure policy mechanism as Map-Reduce (FAIL_JOB, THRESHOLD, BEST_EFFORT).

**Replay:** Dead-lettered fan-out tasks can be replayed via the dead letter processor. The same counter-decrement logic from the Map-Reduce replay design applies: decrement `failed_tasks` atomically when replaying.

---

## 10. OnCompleted: Inline vs. Task

OnCompleted runs inline on the leader — not as a separate task. This is a deliberate design choice:

**Why inline:**
- No extra task overhead for what is typically a lightweight action (log, update a status row, enqueue a follow-up).
- No REDUCING state or reduce task lifecycle to manage.
- Completes synchronously within the barrier detection loop — the job transitions to COMPLETED in the same tick.

**Limitation:**
- OnCompleted must be fast. It runs inside the leader's monitoring loop. If it blocks for 30 seconds (e.g., uploading a large file to MinIO), it delays barrier detection for all other jobs.
- OnCompleted failures cause the job to transition to FAILED. There's no retry mechanism for OnCompleted itself.

**Mitigation:** If OnCompleted needs heavy processing, it should enqueue a standalone task or start a new chain. The fan-out job completes immediately, and the follow-up work runs through the standard queue with full retry/dead-letter guarantees.

---

## 11. Comparison with Fire-and-Forget

Fan-out is not the same as bulk fire-and-forget:

| Dimension | Fire-and-Forget | Fan-Out |
|-----------|----------------|---------|
| Correlation | No group_id, tasks are independent | group_id links all tasks to a job |
| Completion tracking | None — individual tasks complete independently | Job tracks total/completed/failed counters |
| Failure policy | Per-task only (retry, dead-letter) | Per-job (FAIL_JOB, THRESHOLD, BEST_EFFORT) |
| OnCompleted | None | Optional callback when all tasks are done |
| Visibility | Individual tasks visible, no aggregate view | Job-level dashboard: "500/1000 complete, 3 failed" |

Fire-and-forget is for tasks that don't need coordination. Fan-out is for tasks that need to be tracked as a group.

---

## 12. Observability

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `taskqueue.fanout.jobs_started` | Counter | job_type | Fan-out jobs initiated |
| `taskqueue.fanout.jobs_completed` | Counter | job_type | Jobs finished successfully |
| `taskqueue.fanout.jobs_failed` | Counter | job_type | Jobs failed (policy threshold) |
| `taskqueue.fanout.task_completion_rate` | Gauge | job_type | completed / total for in-flight jobs |
| `taskqueue.fanout.barrier_latency` | Histogram | job_type | Time from last task complete to job complete |

---

## 13. Testing Strategy

| Test | Validates |
|------|-----------|
| Fan-out 10 tasks, verify all execute and job transitions to COMPLETED | Happy path |
| Fan-out 10 tasks, dead-letter 1, verify FAIL_JOB policy fails the job | Failure policy: FAIL_JOB |
| Fan-out 10 tasks, dead-letter 1, verify BEST_EFFORT policy completes the job | Failure policy: BEST_EFFORT |
| Fan-out 10 tasks, verify OnCompleted callback fires with correct summary | OnCompleted invocation |
| Kill leader mid-fan-out, verify new leader resumes monitoring | Leader failover |
| Fan-out 100 tasks, kill a worker, verify reaper reclaims and job eventually completes | Crash recovery |
| Replay dead-lettered fan-out tasks, verify counter decremented and job can re-complete | Dead letter replay |
| Fan-out with heavy OnCompleted, verify it's recommended to enqueue follow-up task instead | Documentation/convention |
