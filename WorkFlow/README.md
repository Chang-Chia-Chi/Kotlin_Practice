# Lock-Free Workflow Engine — Design Document

## 1. Problem Statement

In a distributed task queue with Oracle as the backing store, the classic approach to DAG progression uses a mutable counter on the parent row (`tasks_pending -= 1`). When thousands of workers complete simultaneously, they all race to decrement the same row. This creates catastrophic row-lock contention: connection pool exhaustion, transaction timeouts, and cascading rollback storms.

The counter itself is the bottleneck. We must eliminate it entirely.

Additionally, the engine needs a declarative DSL to describe workflow shapes — fan-out, parallel execution, and join — without coupling definitions to execution details. The DSL produces pure data that can be serialized, persisted, and replayed from any checkpoint.

**Stack:** Kotlin · Quarkus · JDBI · Oracle · Kubernetes

---

## 2. Architecture: Dual-Path Progression

Two independent mechanisms guarantee that every workflow phase eventually completes, even under arbitrary node failure.

**Primary Path — Lock-Free Barrier.** Workers derive completion from a read-only aggregate query (MVCC snapshot, zero locks) and advance the workflow via optimistic CAS on a single workflow row. Under normal operation, this path handles 100% of transitions with zero contention during task execution.

**Backup Path — Leader Sweeper.** A single leader polls at low frequency to detect workflows stuck due to worker death between CAS success and downstream task insertion. The sweeper executes the same CAS + fan-out logic, which is inherently idempotent. It is a safety net, not a performance path.

**Key invariant:** Both paths use the same CAS predicate. At most one actor (worker or leader) can ever win the transition for a given phase. This is the single serialization point in the entire design — and it fires at most once per phase, not once per task.

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

Reading: `split` produces N payloads. The engine fans out N `process-chunk` tasks to the worker pool. After all complete, the last worker evaluates JoinPolicy. If ≥ 95% succeeded, it executes the `merge` handler inline to aggregate results.

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

### 3.3 DSL Data Model

**WorkflowDefinition** — An ordered sequence of `ActivityDefinition`, representing the entire workflow blueprint.

**ActivityDefinition:**

| Field         | Type              | Default    | Description                                               |
|---------------|-------------------|------------|-----------------------------------------------------------|
| name          | String            | —          | Business label for logging, tracing, and replay location  |
| transition    | String            | (required) | Dot-separated handler key, resolved via CDI on the worker |
| retries       | Int               | 0          | Max retry count, excluding the initial attempt            |
| failurePolicy | FailurePolicy    | ABORT      | Behaviour on failure                                      |
| deadline      | Duration          | 30 min     | Execution timeout; doubles as worker-liveness detection   |
| fanOut        | FanOutDefinition? | null       | Embedded fan-out definition; null = linear activity       |

**FanOutDefinition** (embedded in ActivityDefinition):

| Field         | Type           | Default    | Description                                |
|---------------|----------------|------------|--------------------------------------------|
| transition    | String         | (required) | Handler key for each sub-task              |
| retries       | Int            | 0          | Max retries per sub-task                   |
| failurePolicy | FailurePolicy | ABORT      | Sub-task failure behaviour                 |
| deadline      | Duration       | 30 min     | Per-sub-task execution timeout             |
| join          | JoinDefinition | (required) | Convergence definition                     |

**JoinDefinition** (embedded in FanOutDefinition):

| Field      | Type       | Default | Description                                              |
|------------|------------|---------|----------------------------------------------------------|
| policy     | JoinPolicy | ALL     | Success/failure gate applied after all sub-tasks complete |
| transition | String?    | null    | Aggregation handler key; null = pure barrier             |

**FailurePolicy** — controls behaviour _during_ execution:

| Value       | Behaviour                                              |
|-------------|--------------------------------------------------------|
| ABORT       | Any task failure → stop, do not dispatch further tasks |
| BEST_EFFORT | Record failure, continue executing remaining tasks     |

**JoinPolicy** — controls the success gate _after_ all sub-tasks complete:

| Value          | Behaviour                                                |
|----------------|----------------------------------------------------------|
| ALL            | All sub-tasks succeeded → success; any failure → failure |
| THRESHOLD(n)   | ≥ n sub-tasks succeeded → success; otherwise failure     |
| PERCENTAGE(pct)| ≥ pct% sub-tasks succeeded → success; otherwise failure  |

Note: JoinPolicy governs the success gate, not timing. Regardless of policy, the engine waits for all sub-tasks to reach a terminal state before evaluating.

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

## 4. Two-Table Data Model

The entire runtime state lives in two tables. No activity table exists — activity metadata is embedded in the serialized `WorkflowDefinition`.

### 4.1 Workflow Table

One mutable row per workflow execution. This is the sole CAS target.

| Column           | Type      | Description                                                  |
|------------------|-----------|--------------------------------------------------------------|
| id               | PK        | Workflow execution ID                                        |
| definition       | CLOB/JSON | Serialized WorkflowDefinition (write-once at creation)       |
| current_sequence | Int       | The sequence number currently being executed                 |
| version          | Int       | CAS guard, incremented on every phase transition             |
| status           | Enum      | RUNNING / COMPLETED / FAILED                                 |
| updated_at       | Timestamp | Last phase transition time; used by sweeper for grace period |
| created_at       | Timestamp | Creation time                                                |

The `current_sequence` maps to the runtime sequence being executed. The `version` column prevents ABA. The `status` column tracks terminal states.

### 4.2 Task Table

Standard task queue rows. Each task belongs to a workflow at a specific sequence number.

| Column          | Type      | Description                                              |
|-----------------|-----------|----------------------------------------------------------|
| id              | PK        | Globally unique task ID                                  |
| workflow_id     | FK        | Parent workflow                                          |
| sequence_number | Int       | Which runtime sequence this task belongs to              |
| status          | Enum      | PENDING / PROCESSING / COMPLETED / FAILED / DEAD_LETTER  |
| handler_key     | String    | Dot-separated routing key for CDI resolution             |
| payload         | CLOB/JSON | Input data                                               |
| result          | CLOB/JSON | Output data (populated on completion)                    |
| claimed_by      | String    | Worker identity                                          |
| claimed_at      | Timestamp | Claim time                                               |
| completed_at    | Timestamp | Terminal state time                                      |
| retry_count     | Int       | Current retry count                                      |
| max_retries     | Int       | Max retries allowed                                      |
| deadline_at     | Timestamp | Absolute deadline                                        |

**Independence guarantee:** A worker completing or failing a task updates ONLY that task's row. No writes propagate upward to the workflow row during normal task lifecycle.

### 4.3 Sequence Number Expansion

A single DSL activity may expand into multiple runtime sequences. The `WorkflowDefinition` builder pre-computes this mapping at build time. The engine is sequence-agnostic — it runs the barrier for whatever `current_sequence` is.

Expansion rules:

- **Linear activity** → 1 sequence (single task, handler = activity.transition).
- **Fan-out activity** → 2 sequences:
    - Sequence N: scatter phase (single task, handler = activity.transition, returns payloads).
    - Sequence N+1: parallel phase (N tasks, handler = fanOut.transition, one per payload).

The join is not a separate sequence. When the barrier fires at the parallel-phase sequence, the CAS winner evaluates JoinPolicy and executes the join transition inline (if declared). This keeps the join as a convergence point of the fan-out, not an independent node.

The definition stores metadata per sequence: phase type (LINEAR / SCATTER / PARALLEL), handler key, retry/deadline/failure policy, and for PARALLEL phases, the associated JoinDefinition.

---

## 5. State Machines

### 5.1 Workflow Lifecycle

```
RUNNING ──(last sequence barrier fires, success)──► COMPLETED
    │
    └──(failure propagates to workflow level)──► FAILED
```

A workflow is created directly in RUNNING with `current_sequence = 1` and its first tasks inserted, all in one transaction.

### 5.2 Task Lifecycle

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

### 5.3 Failure Propagation

- Linear task fails, FailurePolicy = ABORT → workflow FAILED.
- Linear task fails, FailurePolicy = BEST_EFFORT → advance to next sequence.
- Fan-out sub-task fails, fanOut FailurePolicy = ABORT → stop dispatching; wait for in-flight sub-tasks to finish. Evaluate JoinPolicy on what completed.
- Fan-out sub-task fails, fanOut FailurePolicy = BEST_EFFORT → continue; wait for all sub-tasks.
- All sub-tasks terminal → evaluate JoinPolicy. Threshold not met → fan-out failed. Propagate to parent activity's FailurePolicy.
- Join transition (inline) fails → fan-out failed. Propagate to parent activity's FailurePolicy.

---

## 6. Core Algorithm: Lock-Free Barrier (Primary Path)

When a worker completes (or fails) a task, the following operations execute within a **single ACID transaction**, in strict order:

### Step 1 — Self-Update

Update the task row to its terminal state (COMPLETED or FAILED). This touches only one row and contends with no other worker since the task was claimed via `SELECT FOR UPDATE SKIP LOCKED`.

### Step 2 — Lock-Free Probe

Count tasks belonging to this workflow at this sequence number that are NOT in a terminal state:

```sql
SELECT COUNT(*) FROM task
 WHERE workflow_id = :wf_id AND sequence_number = :seq
   AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
```

No `FOR UPDATE`, no pessimistic locks. Oracle MVCC read consistency provides a fresh snapshot.

**Expected race:** Two workers commit near-simultaneously, both see count = 0. Both proceed to Step 3. CAS resolves the tie.

**Index requirement:** Composite index on `(workflow_id, sequence_number, status)` — must produce an index-only scan.

### Step 3 — Evaluate Outcome & Optimistic CAS

If the probe count > 0: other tasks are still in flight. Commit (task update only) and exit.

If the probe count = 0: this worker may be the last one. Before the CAS:

1. Count tasks in FAILED / DEAD_LETTER state for this `(workflow_id, sequence_number)`.
2. Count total tasks for this `(workflow_id, sequence_number)`.
3. Look up the sequence metadata from the definition to determine which policy applies:
    - PARALLEL phase → evaluate `JoinPolicy` from the `JoinDefinition`.
    - LINEAR / SCATTER phase → evaluate `FailurePolicy` from the `ActivityDefinition`.
4. Determine target outcome: success or failure.

Then attempt the CAS on the workflow row:

```sql
UPDATE workflow
   SET current_sequence = :next_seq,
       version = version + 1,
       updated_at = SYSTIMESTAMP
 WHERE id = :wf_id
   AND current_sequence = :expected_seq
   AND version = :expected_version
   AND status = 'RUNNING'
```

- **Rows affected = 1:** CAS succeeded. This worker owns the transition. Proceed to Step 4.
- **Rows affected = 0:** Another actor already transitioned. Commit and exit gracefully.

### Step 4 — Advance Workflow

The CAS winner, still within the same transaction:

**If the outcome is failure** and the parent activity's FailurePolicy is ABORT:

```sql
UPDATE workflow SET status = 'FAILED', updated_at = SYSTIMESTAMP
 WHERE id = :wf_id
```

Commit and exit. (Note: the CAS in Step 3 already advanced `current_sequence` and `version`. This second update marks the terminal state.)

**If the outcome is failure** and the parent activity's FailurePolicy is BEST_EFFORT: treat as success — continue to the next sequence below.

**If the current sequence is a PARALLEL phase with a join transition** (outcome is success): execute the join handler inline. If it fails, treat the outcome as failure and apply the logic above.

**If this is the last sequence in the definition:**

```sql
UPDATE workflow SET status = 'COMPLETED', updated_at = SYSTIMESTAMP
 WHERE id = :wf_id
```

Commit. Workflow is done.

**If a next sequence exists:** Look up the next sequence's metadata from the definition. Insert tasks accordingly:

- SCATTER or LINEAR phase: insert 1 task with the appropriate handler key and payload.
- PARALLEL phase: read the scatter task's `result` column (from the preceding SCATTER sequence), deserialize the payloads, bulk-insert N sub-tasks.

Commit.

**Atomicity guarantee:** Steps 1–4 are in one transaction. Either the full chain (task done + workflow advanced + next tasks created) commits, or nothing does.

**Contention analysis:** The workflow row lock is held only from the CAS in Step 3 through the COMMIT at the end of Step 4. One lock, once per phase, for the duration of one INSERT batch. Under normal operation with N tasks, there are N task-row updates (zero contention) and exactly 1 workflow-row update.

---

## 7. Backup Algorithm: Leader Sweeper

### Purpose

Catches the edge case where a worker wins the CAS but dies before the transaction commits. The transaction rolls back, leaving the workflow at `current_sequence = N` with zero in-flight tasks and no one to trigger the next phase.

### Patrol Logic

The leader executes on a fixed schedule (e.g., every 60 seconds):

1. **Find stuck workflows:** Query for workflows where:
    - `status = 'RUNNING'`
    - Zero tasks in non-terminal state at `current_sequence`
    - `updated_at < SYSTIMESTAMP - :grace_period`

2. **Grace period (mandatory):** Must be ≥ 2× the P99 transaction duration of the primary path's Step 3–4 sequence. Prevents the sweeper from contending with a worker whose transaction is still open.

3. **Recover:** For each stuck workflow, execute the same evaluate-outcome + CAS + advance logic as the primary path (Steps 3–4). The CAS predicate makes this idempotent. If a delayed worker somehow commits between the sweeper's probe and CAS attempt, the CAS fails harmlessly.

### Leader Failure

If the leader dies mid-sweep, the fenced leader election mechanism (Kubernetes Lease with `resourceVersion` as fencing epoch) elects a new leader. The new leader's next patrol picks up any stuck workflows. No state is lost because the sweeper's actions are transactional.

---

## 8. Execution Semantics

These describe the DSL-implied runtime behaviour without prescribing engine internals.

### Linear Activity

No `fanOut`. The engine inserts a single task at the current sequence. A worker claims it, executes the handler, and reports the result. On failure, the task retries per `retries`; retries may land on different workers. On terminal state, the lock-free barrier fires — trivially, since there is only one task.

### Fan-out Activity

Expands into two runtime sequences:

1. **Scatter (sequence N):** Single task, handler = activity's own `transition`. Worker executes, produces a list of payloads, writes them to the task's `result` column. Barrier fires (one task, trivially). CAS winner reads the result, bulk-inserts N sub-tasks at sequence N+1.

2. **Parallel (sequence N+1):** N sub-tasks, handler = `fanOut.transition`. Each sub-task independently follows the `FanOutDefinition`'s `retries`, `deadline`, and `failurePolicy`. Workers execute concurrently. Last completing worker triggers the barrier. CAS winner evaluates `JoinPolicy`. If the `JoinDefinition` declares a `transition`, the CAS winner executes the join handler inline to aggregate results. If no `transition`, the join is a pure barrier — the engine advances to the next sequence immediately.

### Join — Inline Execution

The join handler is executed inline by the CAS winner within the barrier transaction (Step 4), not as a separately queued task. This means:

- The join handler must be fast (bounded by the barrier transaction duration budget).
- If the join handler fails, the fan-out phase is marked as failed. No retry — the join is not a task with retry semantics. This is a deliberate simplification; retry-worthy join logic should be modelled as a separate downstream linear activity.

---

## 9. Indexing Strategy

### Workflow Table

- Primary key: `id`.
- Index: `(status, updated_at)` — sweeper query. Finds RUNNING workflows past grace period without a full table scan.

### Task Table

- Primary key: `id`.
- Index: `(workflow_id, sequence_number, status)` — the critical composite index. Serves both the lock-free probe (count non-terminal tasks) and the failure/join evaluation (count failed tasks). Must produce index-only scans.
- Index: `(status, deadline_at)` — stale task reaper. Finds PROCESSING tasks past deadline.
- Index: `(status, claimed_at)` — task claiming via `SKIP LOCKED`. Oracle scans PENDING tasks in index order.

### Row Lock Contention Audit

The ONLY row that ever experiences multi-writer contention is the workflow row during CAS. That contention is bounded: at most N workers attempt CAS simultaneously (where N = number of workers that see count = 0), and the lock is held for the duration of one INSERT batch. Compare this to the counter approach where a parent row is contended N times (once per task completion).

### Constraint

The task table must have NO trigger or foreign key that propagates writes to the workflow table on task status change.

---

## 10. Observability

### Metrics (Micrometer / Prometheus)

**Counters:**

- `workflow.barrier.cas.attempts{outcome=won|lost}` — a healthy system shows exactly 1 `won` per phase and 0–few `lost`.
- `workflow.sweeper.recoveries` — non-zero means workers died mid-transition.
- `workflow.phase.transitions{from_seq, to_seq, phase_type}` — tracks progression.

**Gauges:**

- `workflow.running.count` — number of workflows currently RUNNING. Growing count suggests workflows are getting stuck.
- `workflow.tasks.by_status` — task count per status across running workflows. Periodic sampling.

**Histograms:**

- `workflow.barrier.transaction.duration` — wall time of the full Step 1–4 transaction. P99 directly informs the sweeper's grace period.
- `workflow.phase.completion.duration` — time from phase start to barrier fire. End-to-end phase latency.

### Health Checks

- **Sweeper liveness:** Unhealthy if the last patrol completed more than 2× the patrol interval ago.
- **Stuck workflow gauge:** Number of workflows currently matching stuck criteria. Zero is normal.

### Structured Logging

- On CAS win: `workflow_id`, `sequence_number`, `phase_type`, `task_count`, `failed_count`, `target_outcome`, `transaction_duration_ms`.
- On CAS loss: `workflow_id` at DEBUG level — expected behaviour.
- On sweeper recovery: WARN level with `workflow_id`, `sequence_number`, `time_since_last_update`, `grace_period`.

---

## 11. Testing Strategy

### Unit Tests — Barrier Logic

1. **Single task completes (linear):** Probe returns 0, CAS wins, next sequence's tasks inserted. Verify full happy path in one transaction.
2. **Last-of-many completes (parallel phase):** N-1 sub-tasks already terminal, Nth completes, probe returns 0, CAS wins. Verify exactly one phase transition.
3. **Not-last task completes:** Probe returns > 0, no CAS attempted. Verify the transaction contains only the task update.
4. **CAS race — two workers see zero:** Both invoke CAS. First wins, second loses. Verify exactly one set of downstream tasks.
5. **JoinPolicy — ALL, any failure:** 1 failed sub-task. Verify outcome = failure.
6. **JoinPolicy — PERCENTAGE, within tolerance:** 100 sub-tasks, 3 failed, PERCENTAGE(95). Verify outcome = success.
7. **JoinPolicy — PERCENTAGE, breach:** 100 sub-tasks, 10 failed, PERCENTAGE(95). Verify outcome = failure.
8. **JoinPolicy — THRESHOLD:** 50 sub-tasks, 45 succeeded, THRESHOLD(40). Verify outcome = success.
9. **FailurePolicy — BEST_EFFORT propagation:** Phase fails but parent policy is BEST_EFFORT. Verify workflow advances to next sequence.
10. **Join with inline transition:** CAS wins on parallel phase, join declares a transition. Verify join handler is executed and workflow advances.
11. **Join as pure barrier:** CAS wins, no join transition. Verify workflow advances immediately.
12. **Scatter → parallel handoff:** Scatter task completes with payloads in `result`. Verify CAS winner reads result, inserts correct number of sub-tasks at next sequence.

### Unit Tests — Sweeper

13. **Stuck workflow detection:** Workflow RUNNING, current_sequence has zero non-terminal tasks, `updated_at` past grace period. Verify sweeper triggers recovery.
14. **Grace period respects in-flight transactions:** Same setup but `updated_at` within grace period. Verify sweeper skips it.
15. **Sweeper + Worker race:** Worker's CAS is in flight when the sweeper fires. Sweeper's CAS must fail. Verify no duplicate downstream tasks.
16. **Sweeper idempotency:** Sweeper fires twice on the same stuck workflow. First recovers it. Second CAS fails. Verify no side effects.

### Integration Tests — End-to-End

17. **Linear workflow completion:** 3-sequence linear workflow. Complete tasks in order. Verify each successor auto-created and workflow COMPLETED.
18. **Fan-out workflow completion:** Scatter → parallel (50 sub-tasks) → next linear. Verify scatter produces payloads, sub-tasks created, JoinPolicy evaluated, workflow advances.
19. **Worker death simulation:** All sub-tasks terminal, simulate worker OOM by rolling back the CAS transaction. Verify sweeper detects and completes the transition.
20. **High-concurrency barrier:** 100+ sub-tasks for a single parallel phase, completed near-simultaneously. Verify exactly one phase transition, no duplicates, no lock-wait timeouts.

### Load / Chaos Tests

21. **10,000 concurrent task completions:** Verify zero row-lock waits on the workflow row beyond the single CAS winner. Measure P99 transaction duration.
22. **Random worker kills during barrier:** Continuously kill worker pods at random intervals. Verify all workflow runs eventually complete via sweeper.

---

## 12. Design Decisions

| Decision | Rationale |
|---|---|
| Two tables only (workflow + task) | Activity metadata lives in the serialized definition. No runtime activity table — sequence number on the task row is the sole grouping key. |
| Mutable workflow row as CAS target | One row per execution, locked once per phase (not per task). Minimal contention surface. |
| Sequence number expansion at build time | Fan-out activity → 2 runtime sequences (scatter + parallel). Engine is sequence-agnostic; it runs the barrier for current_sequence. |
| Join executed inline by CAS winner | Eliminates an extra task + second barrier round-trip. The last completing worker runs the join handler directly within the barrier transaction. |
| No join retry semantics | Join handler must be fast and idempotent. Retry-worthy aggregation should be a separate downstream linear activity. |
| Task result column for payload passing | Scatter task writes payloads to its `result` column. CAS winner reads it to create sub-tasks. No side-channel needed. |
| Join embedded in fanOut | The join is the convergence point of a fan-out's lifecycle, not an independent node. |
| Join transition is optional | Aggregation handler when needed; pure barrier when not. Same structure expresses both. |
| fanOut embedded in activity | Scatter and parallel are two faces of the same activity, not independent nodes. |
| Definitions are data, not behaviour | In a distributed environment the engine does not hold handler instances. String keys provide indirection; workers resolve via CDI. |
| fanOut has independent retry/deadline | Sub-task execution characteristics typically differ from the parent activity's. |
| JoinPolicy is per fan-out | A single workflow may have multiple fan-out phases, each with a different convergence strategy. |
| Join waits for all sub-tasks | Avoids orphan tasks. All sub-tasks run to terminal state before evaluation. |
| PERCENTAGE complements THRESHOLD | When N varies dynamically, percentages are more robust than hardcoded counts. |
| Handler uses dot-separated key | CDI-native resolution on the worker side; supports handler group deployments. |
| Deadline doubles as liveness detection | One field covers both business timeout and worker-crash detection. |
| Definitions are serializable | Crash recovery reads persisted definition + current_sequence and resumes. |
| Naming aligns with Temporal | activity / workflow / definition semantics match Temporal, reducing communication overhead. |
| No mutable counters anywhere | Progress is always derived via MVCC read, never stored. |
| FailurePolicy vs JoinPolicy — two axes | FailurePolicy controls mid-flight behaviour (ABORT/BEST_EFFORT). JoinPolicy controls the post-completion success gate (ALL/THRESHOLD/PERCENTAGE). Orthogonal concerns. |

---

## 13. Implementer Checklist

- [ ] The task table has NO trigger or foreign key that propagates writes to the workflow table on task status change.
- [ ] The lock-free probe query uses a plain `SELECT COUNT` (no `FOR UPDATE`, no `LOCK` hints).
- [ ] The CAS UPDATE on the workflow row includes `current_sequence = :expected_seq`, `version = :expected_version`, and `status = 'RUNNING'` in the WHERE clause.
- [ ] Steps 1–4 of the barrier are in a single JDBI `inTransaction` block.
- [ ] Join/failure policy evaluation happens BEFORE the CAS, and its result determines the CAS target outcome.
- [ ] The sweeper's grace period is configurable and defaults to ≥ 2× the observed P99 barrier transaction duration.
- [ ] The composite index `(workflow_id, sequence_number, status)` exists on the task table and is verified to produce index-only scans for both the probe and failure count queries.
- [ ] The composite index `(status, updated_at)` exists on the workflow table for sweeper queries.
- [ ] `WorkflowDefinition` is immutable after build. `@DslMarker` prevents scope leakage.
- [ ] Build-phase validation enforces: activity `transition` required; if `fanOut` declared, `fanOut.transition` and `join` required; `join.transition` optional.
- [ ] Fan-out sub-tasks carry the `FanOutDefinition`'s retry/deadline/failurePolicy, not the parent activity's.
- [ ] Scatter task's `result` colum/n is populated with serialized payloads on completion. The barrier winner deserializes and bulk-inserts sub-tasks.
- [ ] The join handler (if declared) executes inline within the barrier transaction, not as a separate queued task.
- [ ] Under a 10,000-concurrent-completion test, Oracle AWR / ASH shows zero `enq: TX - row lock contention` waits on the workflow table beyond the single CAS winner's brief hold.
- [ ] If the system is killed at any arbitrary millisecond, restarting the sweeper alone is sufficient to resume all in-progress workflow runs.