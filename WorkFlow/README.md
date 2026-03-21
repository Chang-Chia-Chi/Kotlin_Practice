# Lock-Free Workflow Engine — Design Document

## 1. Problem Statement

In a distributed task queue with Oracle as the backing store, the classic approach to DAG progression uses a mutable counter on the parent activity (`tasks_pending -= 1`). When thousands of workers complete simultaneously, they all race to decrement the same row. This creates catastrophic row-lock contention: connection pool exhaustion, transaction timeouts, and cascading rollback storms.

The counter itself is the bottleneck. We must eliminate it entirely.

Additionally, the engine needs a declarative DSL to describe workflow shapes — fan-out, parallel execution, and join — without coupling definitions to execution details. The DSL produces pure data that can be serialized, persisted, and replayed from any checkpoint.

**Stack:** Kotlin · Quarkus · JDBI · Oracle · Kubernetes

---

## 2. Architecture: Dual-Path Progression

Two independent mechanisms guarantee that every workflow activity eventually completes, even under arbitrary node failure.

**Primary Path — Lock-Free Barrier.** Workers derive completion from a read-only aggregate query (MVCC snapshot, zero locks) and advance the workflow via optimistic CAS. Under normal operation, this path handles 100% of transitions with zero contention on the activity row.

**Backup Path — Leader Sweeper.** A single leader polls at low frequency to detect activities that are stuck due to worker death between CAS success and downstream task insertion. The sweeper executes the same CAS + fan-out logic, which is inherently idempotent. It is a safety net, not a performance path.

**Key invariant:** Both paths use the same CAS predicate. At most one actor (worker or leader) can ever win the transition for a given activity. This is the single serialization point in the entire design — and it fires at most once per activity, not once per task.

---

## 3. Workflow DSL

### 3.1 Purpose

A declarative DSL describes the shape of a workflow — fan-out, parallel execution, and join — without prescribing execution details. The DSL produces a `WorkflowDefinition`: pure data, zero behaviour, JSON-serializable, persistable, and replayable from any checkpoint.

All tasks run on workers. Different activities — even retries of the same activity — may execute on different workers. The DSL defines flow shape; the engine handles dispatch.

### 3.2 Examples

**Fan-out with aggregation (scatter → parallel → merge):**

```kotlin
val pipeline = workflow {
    activity("split") {
        transition("dsl.split")
        retries(3)
        deadline(Duration.ofMinutes(10))
        fanOut {
            transition("dsl.process-chunk")
            retries(2)
            deadline(Duration.ofMinutes(5))
            failurePolicy(FailurePolicy.BEST_EFFORT)
            join {
                policy(JoinPolicy.PERCENTAGE(95))
                transition("dsl.merge")
            }
        }
    }
}
```

Reading: `split` produces N payloads. The engine fans out N `process-chunk` tasks to the worker pool. After all complete, the last worker evaluates JoinPolicy. If ≥ 95% succeeded, it executes the `merge` handler to aggregate results.

**Pure barrier fan-out (scatter → parallel → barrier):**

```kotlin
val pipeline = workflow {
    activity("dispatch") {
        transition("batch.prepare")
        fanOut {
            transition("batch.execute")
            retries(1)
            join {
                policy(JoinPolicy.ALL)
                // No transition — pure barrier, no aggregation handler
            }
        }
    }
    activity("notify") {
        transition("batch.notify")
    }
}
```

Reading: After all `batch.execute` tasks succeed, the engine advances directly to `notify`. The join acts purely as a barrier.

### 3.3 Data Model

**WorkflowDefinition** — An ordered sequence of `ActivityDefinition`, representing the entire workflow blueprint.

**ActivityDefinition:**

| Field         | Type              | Default | Description                                                |
|---------------|-------------------|---------|------------------------------------------------------------|
| name          | String            | —       | Business label for logging, tracing, and replay location   |
| transition    | String            | (required) | Dot-separated handler key, resolved via CDI on the worker  |
| retries       | Int               | 0       | Max retry count, excluding the initial attempt             |
| failurePolicy | FailurePolicy    | ABORT   | Behaviour on failure                                       |
| deadline      | Duration          | 30 min  | Execution timeout; doubles as worker-liveness detection    |
| fanOut        | FanOutDefinition? | null    | Embedded fan-out definition; null = linear activity        |

**FanOutDefinition** (embedded in ActivityDefinition):

| Field         | Type           | Default | Description                                    |
|---------------|----------------|---------|------------------------------------------------|
| transition    | String         | (required) | Handler key for each sub-task                  |
| retries       | Int            | 0       | Max retries per sub-task                       |
| failurePolicy | FailurePolicy | ABORT   | Sub-task failure behaviour                     |
| deadline      | Duration       | 30 min  | Per-sub-task execution timeout                 |
| join          | JoinDefinition | (required) | Convergence definition                         |

**JoinDefinition** (embedded in FanOutDefinition):

| Field      | Type       | Default | Description                                                |
|------------|------------|---------|------------------------------------------------------------|
| policy     | JoinPolicy | ALL     | Success/failure gate applied after all sub-tasks complete   |
| transition | String?    | null    | Aggregation handler key; null = pure barrier               |

**FailurePolicy** — controls behaviour _during_ execution:

| Value       | Behaviour                                                  |
|-------------|------------------------------------------------------------|
| ABORT       | Any task failure → stop, do not dispatch further tasks     |
| BEST_EFFORT | Record failure, continue executing remaining tasks, tally errors at the end |

**JoinPolicy** — controls the success gate _after_ all sub-tasks complete:

| Value          | Behaviour                                                   |
|----------------|-------------------------------------------------------------|
| ALL            | All sub-tasks succeeded → success; any failure → failure    |
| THRESHOLD(n)   | ≥ n sub-tasks succeeded → success; otherwise failure        |
| PERCENTAGE(pct)| ≥ pct% sub-tasks succeeded → success; otherwise failure     |

THRESHOLD suits fixed-N fan-outs. PERCENTAGE suits dynamic-N fan-outs where hardcoding an absolute count is fragile.

Note: JoinPolicy governs the success _gate_, not timing. Regardless of policy, the engine waits for all sub-tasks to reach a terminal state before evaluating. The only difference is the success threshold.

### 3.4 DSL Builder

```
workflow { }                → WorkflowBuilder    → WorkflowDefinition
  activity("name") { }     → ActivityBuilder     → ActivityDefinition
    transition(...)
    retries(...)
    failurePolicy(...)
    deadline(...)
    fanOut { }              → FanOutBuilder       → FanOutDefinition
      transition(...)
      retries(...)
      failurePolicy(...)
      deadline(...)
      join { }              → JoinBuilder         → JoinDefinition
        policy(...)
        transition(...)     // optional
```

Constraints: `@DslMarker` prevents scope leakage. Build-phase validation enforces: activity `transition` is required; if `fanOut` is declared, its `transition` and `join` are required; join `transition` is optional. The produced `WorkflowDefinition` is immutable after build.

---

## 4. Domain Models (Engine Level)

### 4.1 Workflow Run

Top-level execution instance of a `WorkflowDefinition`.

**Identity:** Carries a reference to the serialized `WorkflowDefinition` and a `current_activity_index` tracking progression.

**Mutable state:** Status: `PENDING → RUNNING → COMPLETED | FAILED`.

### 4.2 Activity Instance

Represents a single activity in a running workflow. Maps 1:1 with an `ActivityDefinition` at runtime.

**Identity:** Belongs to a Workflow Run. Has a `sequence_number` defining linear ordering within the run.

**Static configuration (write-once at creation):** Execution parameters sourced from the `ActivityDefinition` — handler routing key (`transition`), timeout (`deadline`), retry policy, failure policy. For fan-out activities: the embedded `FanOutDefinition` including join policy and join transition. Next activity index (or null if terminal).

**Mutable state (exactly one column plus version):**

- Status: `PENDING → DISPATCHED → SUCCEEDED | FAILED`.
- A `version` column for CAS (integer, incremented on every status transition).
- `updated_at` timestamp, updated on every status transition. Used by the sweeper for grace-period filtering.

**What is NOT here:** No `tasks_pending`, `tasks_completed`, `tasks_failed`, or any counter updated per-task. Progress is always derived, never stored.

### 4.3 Task

Represents a single unit of work dispatched to a worker. For a linear activity, there is exactly one task. For a fan-out activity, there are N sub-tasks (one per payload from the scatter phase), plus optionally one join-aggregation task.

**Identity:** Belongs to an Activity Instance. Globally unique ID.

**Mutable state:**

- Status: `PENDING → PROCESSING → COMPLETED | FAILED | DEAD_LETTER`.
- Claimed-by, claimed-at, completed-at — standard task queue lifecycle fields.
- Retry count, max retries, deadline — standard fault tolerance fields.

**Independence guarantee:** A worker completing or failing a task updates ONLY that task's row. No writes propagate upward to the activity row during normal task lifecycle.

**Relationship:** Every task carries an `activity_id` foreign key. This is the sole join point for the lock-free aggregate query.

---

## 5. State Machines

### 5.1 Workflow Run Lifecycle

```
PENDING → RUNNING → COMPLETED
                  ↘
                   FAILED
```

### 5.2 Activity Instance Lifecycle

```
PENDING ──(tasks inserted)──► DISPATCHED ──(barrier fires)──► SUCCEEDED
                                   │
                                   └──(join policy breach)──► FAILED
```

**Transition rules:**

- `PENDING → DISPATCHED`: Set by the producer (previous activity's trigger or workflow initializer) after all tasks for this activity have been inserted. Part of the same transaction that inserts the tasks.
- `DISPATCHED → SUCCEEDED`: CAS by exactly one actor (worker or sweeper). Predicate: `status = 'DISPATCHED' AND version = :expected_version`.
- `DISPATCHED → FAILED`: Same CAS mechanism, target state FAILED when join policy / failure policy evaluation determines the activity is unrecoverable.
- SUCCEEDED and FAILED are terminal states. All other transitions are illegal.

### 5.3 Fan-out Activity Internal Phases

When an activity carries a `FanOutDefinition`, execution unfolds in three phases within the DISPATCHED state:

```
activity DISPATCHED
  └→ scatter (execute activity's own transition, produces N payloads)
       └→ N sub-tasks: PENDING → PROCESSING → COMPLETED / FAILED
            └→ join (all sub-tasks terminal → evaluate JoinPolicy)
                 ├→ policy passes + join has transition → execute aggregation handler
                 ├→ policy passes + no transition → pure barrier, advance
                 └→ policy fails → activity FAILED
```

### 5.4 Task Lifecycle

```
PENDING ──(claimed via SKIP LOCKED)──► PROCESSING ──(handler succeeds)──► COMPLETED
                                            │
                                            ├──(handler fails, retries left)──► PENDING
                                            │
                                            ├──(handler fails, no retries)──► FAILED
                                            │
                                            └──(deadline exceeded)──► FAILED (by reaper)
```

Standard task queue semantics. The innovation is in what happens AFTER a task reaches a terminal state.

### 5.5 Failure Propagation

- Linear activity fails, FailurePolicy = ABORT → workflow FAILED.
- Linear activity fails, FailurePolicy = BEST_EFFORT → continue to next activity.
- Fan-out sub-task fails, fanOut FailurePolicy = ABORT → entire fan-out phase fails. Propagate to parent activity's FailurePolicy.
- Fan-out sub-task fails, fanOut FailurePolicy = BEST_EFFORT → continue; wait for remaining sub-tasks.
- All sub-tasks terminal → evaluate JoinPolicy. Threshold not met → fan-out phase fails. Propagate to parent activity's FailurePolicy.
- Join transition execution fails → fan-out phase fails. Propagate to parent activity's FailurePolicy.

---

## 6. Core Algorithm: Lock-Free Barrier (Primary Path)

When a worker completes (or fails) a task, the following operations execute within a **single ACID transaction**, in strict order:

### Step 1 — Self-Update

Update the task row to its terminal state (COMPLETED or FAILED). This touches only one row and contends with no other worker since the task was claimed via `SELECT FOR UPDATE SKIP LOCKED`.

### Step 2 — Lock-Free Probe

Execute a count query against the task table: "how many tasks belonging to this activity are NOT in a terminal state?"

The query uses Oracle's MVCC read consistency — no `FOR UPDATE`, no pessimistic locks. Under Oracle's default READ COMMITTED isolation, this statement gets a fresh snapshot that includes all commits prior to its start.

**Race condition (expected, not a bug):** Two workers commit their tasks near-simultaneously. Both run the probe and both see count = 0 because neither sees the other's not-yet-visible commit at statement start. Both proceed to Step 3. This is correct — CAS resolves the tie.

**Index requirement:** A composite index on `(activity_id, status)` is mandatory. This turns the probe into an index-only range scan, even on tables with tens of millions of rows.

### Step 3 — Evaluate Outcome & Optimistic CAS

If the probe count > 0: other tasks are still in flight. Commit the transaction (task update only) and exit. No work remains.

If the probe count = 0: this worker _may_ be the last one. Before attempting the CAS, evaluate the target state:

1. Count tasks in FAILED or DEAD_LETTER state for the activity (same transaction, MVCC read).
2. Count total tasks for the activity.
3. Apply the activity's join/failure policy:
    - For fan-out activities: evaluate the `JoinPolicy` (ALL / THRESHOLD / PERCENTAGE) from the `JoinDefinition`.
    - For linear activities: evaluate the `FailurePolicy` from the `ActivityDefinition`.
4. Determine target state: `SUCCEEDED` or `FAILED`.

Then attempt the CAS on the activity row:

```
UPDATE activity_instance
   SET status = :target_status, version = version + 1, updated_at = SYSTIMESTAMP
 WHERE id = :activity_id AND status = 'DISPATCHED' AND version = :expected_version
```

- **Rows affected = 1:** CAS succeeded. This worker owns the transition. Proceed to Step 4.
- **Rows affected = 0:** Another actor already transitioned the activity. Commit and exit gracefully.

The `version` column prevents ABA problems. Even if an activity were somehow reset to DISPATCHED (not possible in this design, but defensive), the version mismatch would reject stale CAS attempts.

**Critical:** Failure/join policy evaluation happens BEFORE the CAS, not after. The target state of the CAS is determined by the policy. A two-phase approach (CAS to SUCCEEDED, then discover failure, then CAS to FAILED) would expose a transient incorrect state.

### Step 4 — Trigger Downstream

The CAS winner, still within the same transaction:

**If the activity has a join transition and the target state is SUCCEEDED:**
Insert and dispatch the join-aggregation task. On its completion, re-enter the barrier (Steps 2–4) for this same activity, where the aggregation task is now the only task that matters. (Alternatively, the join task's completion directly triggers downstream advancement — implementation choice.)

**If the activity is terminal (last in the workflow):**
Mark the workflow run as COMPLETED (or FAILED if this activity failed) and commit.

**If the target state is SUCCEEDED and a next activity exists:**
1. Insert a new Activity Instance row for the successor with status = PENDING.
2. Generate and insert all tasks for the successor activity with status = PENDING.
3. Update the successor activity to DISPATCHED.
4. Commit.

**If the target state is FAILED:**
Evaluate the parent activity's `FailurePolicy`. If ABORT, propagate failure to the workflow run. If BEST_EFFORT, advance to the next activity (if any).

**Atomicity guarantee:** Because Steps 1–4 are in one transaction, either the full chain (task done + activity transitioned + next tasks created) commits, or nothing does. There is no observable intermediate state.

**Contention analysis:** The activity row lock is held only from the CAS UPDATE in Step 3 through the COMMIT at the end of Step 4. This is a single lock, held once per activity (not once per task), for the duration of the downstream INSERT batch. Under normal operation with N tasks, there are N task-row updates (zero contention — each task is a distinct row) and exactly 1 activity-row update.

---

## 7. Backup Algorithm: Leader Sweeper

### Purpose

Catches the edge case where a worker wins the CAS in Step 3 but dies (OOM, node eviction, network partition) before the transaction commits. The transaction rolls back, leaving the activity in DISPATCHED with zero in-flight tasks and no one to trigger the next stage.

### Patrol Logic

The leader executes on a fixed schedule (e.g., every 60 seconds):

1. **Find orphaned activities:** Query for activity instances where `status = 'DISPATCHED'`, zero tasks in non-terminal state, and `updated_at < SYSTIMESTAMP - :grace_period`.

2. **Grace period (mandatory):** Must be significantly longer than the P99 transaction duration of the primary path's Step 3–4 sequence. Recommended: ≥ 2× P99. This prevents the sweeper from contending with a worker whose transaction is still open.

3. **Recover:** For each orphaned activity, execute the same evaluate-outcome + CAS + trigger-downstream logic as the primary path (Steps 3–4). The CAS predicate (`version = :expected_version`) makes this idempotent. If a delayed worker somehow commits between the sweeper's probe and CAS attempt, the CAS fails harmlessly.

### Leader Failure

If the leader itself dies mid-sweep, the fenced leader election mechanism (Kubernetes Lease with `resourceVersion` as fencing epoch) elects a new leader. The new leader's next patrol picks up any orphaned activities. No state is lost because the sweeper's actions are transactional.

---

## 8. Execution Semantics

### Linear Activity

No `fanOut`. The engine dispatches a single task to the queue. A worker claims it, executes the handler, and reports the result. On failure, the task retries per `retries`; retries may land on different workers. On terminal state, the lock-free barrier fires (Section 6) — trivially, since there is only one task.

### Fan-out Activity

Three phases:

1. **Scatter:** Execute the activity's own `transition` handler. It returns a list of payloads. The engine creates N sub-tasks, each carrying one payload and the `fanOut.transition` handler key, and inserts them into the queue.

2. **Parallel:** N sub-tasks are claimed and executed by the worker pool. Each sub-task independently follows the `FanOutDefinition`'s `retries`, `deadline`, and `failurePolicy`. Under ABORT policy, once a sub-task fails irrecoverably, the engine may stop dispatching new sub-tasks (exact semantics: remaining PENDING sub-tasks are not claimed, but already-PROCESSING sub-tasks run to completion).

3. **Join:** After all sub-tasks reach a terminal state, the last worker to complete triggers the lock-free barrier (Section 6). The barrier evaluates the `JoinPolicy` to determine success or failure. If the `JoinDefinition` declares a `transition`, the engine dispatches one aggregation task to execute the join handler. If no `transition`, the join is a pure barrier — the engine advances to the next activity immediately.

---

## 9. Data Model Constraints & Indexing Strategy

### Activity Instance Table

- Primary key: `id`.
- Unique constraint: `(workflow_run_id, sequence_number)` — enforces linear ordering.
- Index: `(status, updated_at)` — sweeper orphan detection. Avoids full table scan.
- The `version` column defaults to 0 and increments on every status transition.
- Stores serialized `ActivityDefinition` (or a reference to it) for handler routing and policy evaluation.

### Task Table

- Primary key: `id`.
- Index: `(activity_id, status)` — the critical composite index. Serves both the lock-free probe (count non-terminal tasks) and the join/failure evaluation (count failed tasks). Must produce index-only scans for both queries.
- Index: `(status, deadline_at)` — stale task reaper. Finds PROCESSING tasks past deadline.
- Index: `(status, claimed_at)` — task claiming via `SKIP LOCKED`. Oracle's skip-locked acquisition scans PENDING tasks in index order.

### Row Lock Contention Audit

Under this design, the ONLY row that ever experiences multi-writer contention is the activity row during CAS. And that contention is bounded: at most N workers attempt CAS simultaneously (where N = number of workers that see count = 0), and the lock is held for the duration of one INSERT batch. Compare this to the counter approach where the activity row is contended N times (once per task completion) with lock-wait chains.

### Implementer Constraint

The task table must have NO trigger or foreign key that propagates writes to the activity table on task status change.

---

## 10. Observability

### Metrics (Micrometer / Prometheus)

**Counters:**

- `workflow.barrier.cas.attempts` — total CAS attempts, labeled by outcome (`won`, `lost`). A healthy system shows exactly 1 `won` per activity and 0–few `lost`.
- `workflow.sweeper.recoveries` — total activities recovered by the sweeper. Non-zero means workers died mid-transition.
- `workflow.activity.transitions` — labeled by `from_status` and `to_status`.

**Gauges:**

- `workflow.activities.dispatched` — number of activities currently DISPATCHED. A growing count suggests activities are getting stuck.
- `workflow.tasks.by_status` — task count per status across all dispatched activities. Derived from periodic sampling, not per-event updates.

**Histograms:**

- `workflow.barrier.transaction.duration` — wall time of the full Step 1–4 transaction. P99 directly informs the sweeper's grace period.
- `workflow.activity.completion.duration` — time from activity DISPATCHED to activity SUCCEEDED/FAILED. End-to-end activity latency.

### Health Checks

- **Sweeper liveness:** Reports unhealthy if the last patrol completed more than 2× the patrol interval ago.
- **Orphan gauge:** Number of activities currently matching orphan criteria. Zero is normal.

### Structured Logging

- On CAS win: log `activity_id`, `workflow_run_id`, `task_count`, `failed_count`, `target_state`, `join_policy`, `transaction_duration_ms`.
- On CAS loss: log `activity_id` at DEBUG level — expected behaviour.
- On sweeper recovery: log at WARN level with `activity_id`, `time_since_last_update`, `grace_period`.

---

## 11. Testing Strategy

### Unit Tests — Barrier Logic

1. **Single task completes (linear activity):** Probe returns 0, CAS wins, downstream tasks inserted. Verify full happy path in one transaction.
2. **Last-of-many completes (fan-out):** N-1 sub-tasks already terminal, Nth completes, probe returns 0, CAS wins. Verify exactly one activity transition.
3. **Not-last task completes:** Probe returns > 0, no CAS attempted. Verify the transaction contains only the task update.
4. **CAS race — two workers see zero:** Both invoke CAS. First wins (rows affected = 1), second loses (rows affected = 0). Verify exactly one set of downstream tasks is created.
5. **JoinPolicy — ALL, threshold breach:** All sub-tasks terminal, 1 failed. JoinPolicy = ALL. Verify CAS targets FAILED.
6. **JoinPolicy — PERCENTAGE, within tolerance:** 100 sub-tasks, 3 failed. JoinPolicy = PERCENTAGE(95). Verify CAS targets SUCCEEDED and successor is created.
7. **JoinPolicy — PERCENTAGE, breach:** 100 sub-tasks, 10 failed. JoinPolicy = PERCENTAGE(95). Verify CAS targets FAILED.
8. **JoinPolicy — THRESHOLD:** 50 sub-tasks, 45 succeeded. THRESHOLD(40). Verify SUCCEEDED.
9. **FailurePolicy — BEST_EFFORT propagation:** Activity fails but parent policy is BEST_EFFORT. Verify workflow advances to next activity instead of failing.
10. **Join with aggregation transition:** CAS wins, join declares a transition. Verify the aggregation task is dispatched.
11. **Join as pure barrier:** CAS wins, join has no transition. Verify the engine skips aggregation and directly advances.

### Unit Tests — Sweeper

12. **Orphan detection with grace period:** Activity DISPATCHED with zero pending tasks for longer than the grace period. Verify sweeper triggers recovery.
13. **Grace period respects in-flight transactions:** Activity with zero pending tasks but `updated_at` within the grace period. Verify sweeper skips it.
14. **Sweeper + Worker race:** Worker's CAS is in flight when the sweeper fires. Sweeper's CAS must fail (rows affected = 0). Verify no duplicate downstream tasks.
15. **Sweeper idempotency:** Sweeper fires twice on the same orphan. First recovers it. Second CAS fails. Verify no side effects.

### Integration Tests — End-to-End

16. **Linear workflow completion:** 3-activity linear workflow. Complete all tasks sequentially. Verify each successor auto-created and workflow COMPLETED.
17. **Fan-out workflow completion:** Activity with fan-out of 50 sub-tasks. Complete all. Verify JoinPolicy evaluated, aggregation handler dispatched (if declared), and next activity created.
18. **Worker death simulation:** Complete all sub-tasks, simulate worker OOM by rolling back the CAS transaction. Verify the sweeper detects the orphan and completes the transition.
19. **High-concurrency barrier:** 100+ sub-tasks for a single fan-out, completed near-simultaneously from concurrent threads. Verify exactly one activity transition, one set of downstream tasks, no duplicates, no lock-wait timeouts.

### Load / Chaos Tests

20. **10,000 concurrent task completions:** Verify zero row-lock waits on the activity row beyond the single CAS winner. Measure P99 transaction duration.
21. **Random worker kills during barrier:** Continuously kill worker pods at random intervals during workflow execution. Verify all workflow runs eventually complete via sweeper coverage.

---

## 12. Design Decisions

| Decision | Rationale |
|---|---|
| Join embedded in fanOut | The join is the convergence point of a fan-out's lifecycle, triggered by the last completing worker. It is not an independent node. |
| Join transition is optional | Aggregation handler when needed; pure barrier when not. Same structure expresses both modes. |
| fanOut embedded in activity | Scatter and parallel are two faces of the same activity, not independent nodes. |
| Definitions are data, not behaviour | In a distributed environment the engine does not hold handler instances. String keys provide indirection; workers resolve via CDI. |
| fanOut has independent retry/deadline | Sub-task execution characteristics typically differ from the parent activity's. |
| JoinPolicy is per fan-out | A single workflow may have multiple fan-out phases, each with a different convergence strategy. |
| Join waits for all sub-tasks | Avoids orphan tasks. All sub-tasks run to terminal state before success/failure evaluation. |
| PERCENTAGE complements THRESHOLD | When N varies dynamically, percentages are more robust than hardcoded counts. |
| Handler uses dot-separated key | CDI-native resolution on the worker side; supports handler group deployments. |
| Deadline doubles as liveness detection | One field covers both business timeout and worker-crash detection, reducing concept count. |
| Definitions are serializable | Crash recovery reads persisted definition + checkpoint and resumes. |
| Naming aligns with Temporal | activity / workflow / definition semantics match Temporal, reducing communication overhead. |
| No mutable counters on activity row | Eliminates the primary source of row-lock contention in high-fanout scenarios. Progress is derived via MVCC read. |
| CAS version column on activity | Prevents ABA, serializes the single transition per activity, makes sweeper idempotent. |
| Sweeper as backup, not primary | Under normal operation, workers handle 100% of transitions. The sweeper exists only for crash recovery. |
| FailurePolicy controls mid-flight behaviour | ABORT stops dispatching new sub-tasks on failure; BEST_EFFORT lets remaining sub-tasks run. This is orthogonal to JoinPolicy's post-completion success gate. |

---

## 13. Implementer Checklist

- [ ] The task table has NO trigger or foreign key that propagates writes to the activity table on task status change.
- [ ] The lock-free probe query uses a plain `SELECT COUNT` (no `FOR UPDATE`, no `LOCK` hints).
- [ ] The CAS UPDATE on the activity row includes both `status = 'DISPATCHED'` and `version = :expected_version` in the WHERE clause.
- [ ] Steps 1–4 of the barrier are in a single JDBI `inTransaction` block.
- [ ] Join/failure policy evaluation happens BEFORE the CAS, and its result determines the CAS target state.
- [ ] The sweeper's grace period is configurable and defaults to ≥ 2× the observed P99 barrier transaction duration.
- [ ] The composite index `(activity_id, status)` exists on the task table and is verified to produce index-only scans for both the probe and failure count queries.
- [ ] The composite index `(status, updated_at)` exists on the activity table for sweeper queries.
- [ ] `WorkflowDefinition` is immutable after build. `@DslMarker` prevents scope leakage.
- [ ] Build-phase validation enforces: activity `transition` required; if `fanOut` declared, `fanOut.transition` and `join` required; `join.transition` optional.
- [ ] Fan-out sub-tasks carry the `FanOutDefinition`'s retry/deadline/failurePolicy, not the parent activity's.
- [ ] Under a 10,000-concurrent-completion test, Oracle AWR / ASH shows zero `enq: TX - row lock contention` waits on the activity table beyond the single CAS winner's brief hold.
- [ ] If the system is killed at any arbitrary millisecond, restarting the sweeper alone is sufficient to resume all in-progress workflow runs.