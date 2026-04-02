# DAG Workflow Engine — Design Spec

**Date:** 2026-04-02  
**Status:** Approved  
**Context:** Extends the existing lock-free workflow engine from a linear pipeline model (scatter/parallel/join only) into a full directed-acyclic-graph (DAG) execution model with conditional routing and true parallel branches — aligned with Kestra/Airflow feature parity.

---

## 1. Problem Statement

The current engine executes workflows as a flat ordered list of activities. The only supported topology is a linear chain where any single activity may fan-out over data items (scatter → parallel → join). This is effectively a "stepped map-reduce."

Real-world workflows require:
- **Conditional routing** — different paths based on activity outcome
- **True parallel branches** — different activities running concurrently (not fan-out over data items)
- **Joins** — convergence points that wait for multiple independent predecessors

Without these, the engine cannot model payment flows, approval workflows, multi-system notification pipelines, or any topology that branches based on business logic.

---

## 2. Core Design Decisions

### 2.1 True DAG — no topo-level grouping

Activities start as soon as **their own** prerequisites are met — not when all activities at the same "level" complete. This matches Kestra and Airflow semantics.

**Rejected alternative:** topo-level grouping (shared sequence number per level). Forces global synchronisation at each level — activity D can't start until C finishes even if D only depends on B. Not a true DAG.

### 2.2 Per-activity unique sequence numbers

Each activity in the definition is assigned a unique `sequenceNumber` at build time via topological sort. Tasks for different activities carry different sequence numbers and can be in-flight simultaneously.

Fan-out activities expand into two consecutive internal sequences (SCATTER + PARALLEL) as today. All other mechanics are unchanged.

### 2.3 Remove `current_sequence` from workflow row

`current_sequence: Int` was a progress pointer tied to the level model. With per-activity sequences, it has no meaning. The CAS guard becomes `version` only.

### 2.4 SKIPPED task rows (aligned with Kestra/Airflow)

When a conditional edge is not taken, a SKIPPED task row is inserted for that activity. SKIPPED is terminal. This gives a full audit trail and keeps the barrier probe unchanged (`NOT IN terminal_set`). Both Kestra and Airflow use this pattern.

**Rejected alternative:** skip state as JSON on workflow row. Loses per-activity audit trail; diverges from industry convention.

### 2.5 Lock-free barrier unchanged

The per-sequence probe (`COUNT non-terminal WHERE workflow_id = :wf AND sequence_number = :seq`) is still used — just applied to individual predecessor sequences rather than a shared current sequence. The CAS mechanics (version + optimistic retry) are preserved.

---

## 3. New / Changed Types

### 3.1 `Edge`

```kotlin
const val DEFAULT_BRANCH = "__default__"

data class Edge(
    val target: String,
    val label: String = DEFAULT_BRANCH,
)
```

Unconditional edge: `Edge("fulfill")`. Conditional edge: `Edge("notify-failure", "FAILED")`.

### 3.2 `FanOutDefinition` (new — consolidates existing fields)

```kotlin
data class FanOutDefinition(
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val joinPolicy: JoinPolicy = JoinPolicy.All,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
)
```

Replaces `fanOut: String?` (target activity name) and the top-level `joinPolicy` that were spread across `ActivityDefinition`.

### 3.3 `ActivityDefinition` (changed)

```kotlin
data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: FanOutDefinition? = null,        // replaces fanOut: String? + joinPolicy
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
    val successors: List<Edge> = emptyList(),    // NEW: outgoing DAG edges
)
```

**Removed:** `fanOut: String?`, `joinPolicy: JoinPolicy`.  
Terminal activity = `successors.isEmpty() && fanOut == null`.

### 3.4 `WorkflowDefinition` (changed)

```kotlin
data class WorkflowDefinition(
    val activities: Map<String, ActivityDefinition>,   // name → definition
    val start: String,
    val deadline: Duration = Duration.ofHours(1),
)
```

**Build-time validation:**
- `start` exists in `activities`
- All `Edge.target` values exist in `activities`
- No cycles (topological sort rejects)
- No unreachable activities
- At least one terminal activity
- `FailurePolicy.BEST_EFFORT` + conditional (`on()`) successors on same activity → reject
- `fanOut != null` + conditional successors → reject

### 3.5 `WorkflowRun` (changed)

Remove `current_sequence: Int`. Column dropped from the workflow table.

Remaining columns: `id`, `definition_json`, `version`, `status`, `deadline_at`, `created_at`, `updated_at`.

### 3.6 `Task` (changed)

```kotlin
data class Task(
    ...
    val activityName: String,   // NEW: which DAG activity this task belongs to
    ...
)
```

Needed so the phase gate can attribute task completion to the correct DAG node when evaluating successor readiness.

### 3.7 `TaskStatus` (changed)

```kotlin
enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED,
    TIMED_OUT, DEAD_LETTER, CANCELLED,
    SKIPPED,   // NEW: terminal, inserted by phase gate when edge not taken
}
```

### 3.8 `PhaseType` (changed)

```kotlin
enum class PhaseType { LINEAR, SCATTER, PARALLEL }
```

`SCATTER` is now explicit (previously collapsed into LINEAR).

### 3.9 `SequenceInfo` (changed)

```kotlin
data class SequenceInfo(
    val sequenceNumber: Int,
    val activityName: String,            // DAG node; fan-out parallel = "$name.__parallel__"
    val activity: ActivityDefinition,
    val phaseType: PhaseType,
    val predecessorSequences: List<Int>, // all must be fully terminal before this dispatches
)
```

**Removed:** `activityIndex`, `nextSequence`, `branchSequences`.

### 3.10 `buildSequenceMap()` (changed)

```kotlin
fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo>
```

Returns a flat map — one entry per sequence number. Computed via topological sort of the activity graph.

**Sequence number assignment:**
- Regular activity: 1 sequence (LINEAR)
- Fan-out activity: 2 consecutive sequences (SCATTER at N, PARALLEL at N+1)
- All activities get unique sequence numbers; no shared-level grouping

**`predecessorSequences` rules:**
- Regular activity X: seq numbers of all activities with an edge to X
- Fan-out SCATTER phase: seq numbers of X's predecessors in the DAG
- Fan-out PARALLEL phase (`X.__parallel__`): `[scatter_seq(X)]`
- Successors of fan-out X: `[parallel_seq(X)]` — wait for join, not scatter

---

## 4. DSL

### 4.1 Builder classes

```kotlin
@WorkflowDsl
class WorkflowBuilder {
    fun start(name: String)
    fun deadline(d: Duration)
    fun activity(name: String, block: ActivityBuilder.() -> Unit)
    fun build(): WorkflowDefinition
}

@WorkflowDsl
class ActivityBuilder(private val name: String) {
    fun transition(t: String)
    fun retries(n: Int)
    fun failurePolicy(p: FailurePolicy)
    fun deadline(d: Duration)
    fun backoffBase(d: Duration)
    fun backoffCap(d: Duration)
    fun queue(q: String)
    fun inputs(block: InputsBuilder.() -> Unit)
    fun next(target: String)                          // unconditional successor (fork if repeated)
    fun on(label: String, block: BranchBuilder.() -> Unit)  // conditional successor
    fun fanOut(block: FanOutBuilder.() -> Unit)
    fun build(): ActivityDefinition
}

@WorkflowDsl
class BranchBuilder {
    fun next(t: String)                               // multiple next() = fork on this branch
    fun buildEdges(label: String): List<Edge>
}

@WorkflowDsl
class FanOutBuilder {
    fun transition(t: String)
    fun retries(n: Int)
    fun failurePolicy(p: FailurePolicy)
    fun deadline(d: Duration)
    fun joinPolicy(p: JoinPolicy)
    fun backoffBase(d: Duration)
    fun backoffCap(d: Duration)
    fun queue(q: String)
    fun build(): FanOutDefinition
}

fun workflow(block: WorkflowBuilder.() -> Unit): WorkflowDefinition
```

**Constraint:** mixing `on()` and `next()` on the same activity is rejected at build time.

### 4.2 Examples

**Conditional routing:**

```kotlin
val paymentWorkflow = workflow {
    start("validate")
    activity("validate") {
        transition("ValidationHandler")
        on("OK")      { next("charge") }
        on("INVALID") { next("reject") }
    }
    activity("charge") {
        transition("ChargeHandler")
        retries(2)
        on("SUCCESS") { next("notify"); next("audit") }  // fork on SUCCESS
        on("FAILED")  { next("reject") }
    }
    activity("notify") { transition("NotifyHandler");  next("done") }
    activity("audit")  { transition("AuditHandler");   next("done") }
    activity("reject") { transition("RejectHandler");  next("done") }
    activity("done")   { transition("DoneHandler") }
}
```

**Unconditional fork:**

```kotlin
val notifyWorkflow = workflow {
    start("prepare")
    activity("prepare") {
        transition("PrepareHandler")
        next("send-email")
        next("update-crm")
        next("log-audit")
    }
    activity("send-email") { transition("EmailHandler");  next("done") }
    activity("update-crm")  { transition("CrmHandler");   next("done") }
    activity("log-audit")   { transition("AuditHandler"); next("done") }
    activity("done")        { transition("DoneHandler") }
}
```

**Fan-out embedded in DAG (migration of `dispatchWorkflow`):**

```kotlin
val dispatchWorkflow = workflow {
    start("scatter")
    activity("scatter") {
        transition("DispatchScatterHandler")
        fanOut {
            transition("DispatchSimulationHandler")
            retries(2)
            joinPolicy(JoinPolicy.All)
        }
        next("join")
    }
    activity("join") {
        transition("DispatchJoinHandler")
        deadline(Duration.ofMinutes(10))
        inputs { "batchToken" from "scatter.batchToken" }
    }
}
```

The `simulate` activity is no longer a named graph node. It is embedded as the fan-out within `scatter`. Input reference changes from `simulate.batchToken` to `scatter.batchToken`.

---

## 5. Phase Gate Logic

### 5.1 CAS change

**Old:** `WHERE current_sequence = :expected AND version = :expected_version`  
**New:** `WHERE version = :expected_version` only.

Two workers completing activities in a fork both attempt CAS. One wins; the other retries with the new version, reads fresh state, sees the dispatch guard, exits cleanly.

### 5.2 `onTaskCompleted()` algorithm

All steps execute in one ACID transaction:

```
1. UPDATE task T → terminal status

2. BARRIER PROBE (unchanged):
   SELECT COUNT(*) FROM task
   WHERE workflow_id = :wf AND sequence_number = :seq(T)
   AND status NOT IN terminal_set
   → If > 0: commit task update only, exit.

3. SCATTER special case:
   If phaseType = SCATTER and probe = 0:
   → Create N parallel tasks at parallel_seq from T.resultJson. CAS. Commit. Signal.

4. SUCCESSOR EVALUATION
   Maintain an in-transaction evaluation queue Q, initially = successors of activity X.
   While Q is not empty:
     Pop successor S from Q.

     a. DISPATCH GUARD: task exists for seq(S)? → skip (S already decided by another predecessor)

     b. PREDECESSOR GATE: all predecessorSequences of S terminal?
        → If no: skip (we are not the last predecessor to settle — another will evaluate S)

     c. FATE DECISION (all predecessors of S are terminal — we decide S's fate):
        An edge P→S is "taken" if:
          - P's task is COMPLETED, AND
          - edge label is DEFAULT_BRANCH OR P's resultJson branch key matches edge label
          - (BEST_EFFORT FAILED predecessor: treated as DEFAULT_BRANCH completion)
        
        If ANY edge to S is taken:
          → INSERT task for seq(S) (PENDING). Add queue to signal set.
        
        If NO edge is taken:
          → INSERT SKIPPED task for seq(S).
          → If S is terminal (no successors): add S to completion-check set (step 5).
          → If S has successors: add S's successors to Q (cascade, same transaction).

5. COMPLETION CHECK
   For each terminal activity settled in this transaction (COMPLETED in step 2 or SKIPPED in step 4):
   SELECT COUNT(*) FROM task WHERE workflow_id = :wf AND status NOT IN terminal_set
   → If 0: UPDATE workflow SET status = COMPLETED

6. CAS:
   UPDATE workflow SET version = version + 1, updated_at = SYSTIMESTAMP
   WHERE id = :wf AND version = :expected_version AND status = 'RUNNING'
   → If 0 rows: rollback, retry from step 1.

7. Commit. Signal queues for dispatched tasks.
```

### 5.3 Key properties

| Property | Mechanism |
|---|---|
| No double-dispatch | Dispatch guard (step 4b) inside CAS transaction |
| Idempotent CAS loss | Retry reads fresh state; guard prevents re-dispatch |
| Skip audit trail | SKIPPED task row inserted per activity (Kestra/Airflow convention) |
| Completion correct | Terminal activity triggers global non-terminal count check |
| One serialisation point | CAS on `version` — fires once per activity settlement |

### 5.4 Sweeper `recoverStuckWorkflow()` (updated)

Stuck pattern changes from "zero tasks at `current_sequence`" to "activity whose predecessors are all terminal but no task row exists for its sequence."

```
For each RUNNING workflow past grace period:
  Load definition → buildSequenceMap()
  For each (seq, info) in sequenceMap:
    1. Does any task exist for seq? → If yes: skip
    2. Are all predecessorSequences fully terminal? → COUNT non-terminal = 0 for each
       → If yes: re-dispatch via same logic as step 4 above (CAS + dispatch guard)
```

Idempotent: dispatch guard prevents duplicate task creation.

---

## 6. Error Handling

### 6.1 Regular activity failure

| Policy | Behaviour |
|---|---|
| `ABORT` (default) | Workflow → FAILED; PENDING tasks cancelled; no successors dispatched |
| `BEST_EFFORT` | Settle as completed; dispatch unconditional (`DEFAULT_BRANCH`) successors only |

`BEST_EFFORT` + conditional (`on()`) successors on the same activity: **rejected at build time**. A failed handler produces no branch key.

### 6.2 Fan-out failure

Two-level policy (unchanged from today):

```
Per-item failure:
  fanOut.failurePolicy = ABORT        → stop dispatching new sub-tasks
  fanOut.failurePolicy = BEST_EFFORT  → continue dispatching remaining

After all parallel tasks terminal — evaluate JoinPolicy:
  Passes → settle scatter activity as COMPLETED → follow successors
  Fails  → apply scatter activity's own failurePolicy:
             ABORT        → workflow FAILED
             BEST_EFFORT  → follow unconditional successors
```

### 6.3 SKIPPED is neutral

SKIPPED never causes workflow failure. It is treated as satisfied for all join evaluations. Failure propagates only through FAILED/DEAD_LETTER on ABORT paths.

### 6.4 Dead-letter replay

Unchanged. `replayWorkflow()` resets DEAD_LETTER → PENDING at the existing `sequence_number`. Sequence numbers are stable, so replay resumes from the correct activity.

---

## 7. Testing Strategy

### Unit tests — `buildSequenceMap()`
1. Linear chain: correct sequence numbers and predecessors
2. Fork: B and C get different seq numbers; D's predecessors = [seq(B), seq(C)]
3. Conditional: same shape as fork; edge labels recorded
4. Fan-out: SCATTER at N, PARALLEL at N+1; successor predecessors = [N+1]
5. Fan-out inside DAG: correct predecessor chains across the graph
6. Validation: cycle → reject
7. Validation: unreachable activity → reject
8. Validation: unknown edge target → reject
9. Validation: BEST_EFFORT + on() → reject
10. Validation: fanOut + on() → reject
11. Validation: no start → reject

### Unit tests — Phase gate
12. Linear completion → successor dispatched
13. Terminal activity completes → workflow COMPLETED
14. Parallel join incomplete → no dispatch
15. Parallel join complete → successor dispatched
16. Conditional SUCCESS branch → correct task + SKIPPED for other
17. Conditional FAIL branch → correct task + SKIPPED for other
18. Skip cascade → chain of SKIPPED in one transaction
19. Fork → both branch tasks inserted in one transaction
20. Join — one predecessor done, other pending → no dispatch
21. Join — second predecessor done → dispatch
22. Dispatch guard — seq already has task → no second insert
23. CAS loss → retry converges correctly
24. BEST_EFFORT failure → unconditional successors dispatched
25. ABORT failure → workflow FAILED
26. SCATTER completes → N parallel tasks created
27. PARALLEL join passes → successor dispatched
28. PARALLEL join fails (ABORT) → workflow FAILED
29. PARALLEL join fails (BEST_EFFORT) → unconditional successors dispatched
30. Fan-out activity SKIPPED → SCATTER + PARALLEL + successors all SKIPPED

### Unit tests — DSL builders
31. Linear workflow builds correctly
32. Conditional workflow builds with correct edge labels
33. Fork builds with multiple DEFAULT_BRANCH edges
34. Fan-out builds with FanOutDefinition embedded and next() as successor
35. Migrated dispatchWorkflow builds; scatter.batchToken resolves
36. Mixed on() + next() → build fails

### Integration tests (Oracle, OracleTestContainer)
37. Linear DAG end-to-end → COMPLETED
38. Conditional routing SUCCESS path → correct branch runs; other SKIPPED in DB
39. Conditional routing FAIL path → correct branch runs; other SKIPPED in DB
40. Unconditional fork → all branch tasks PENDING simultaneously
41. Fork + join → join only dispatched after all branches COMPLETED
42. Asymmetric fork timing → join waits for slow branch
43. Fan-out embedded in DAG → COMPLETED
44. Fan-out activity on SKIPPED branch → scatter, parallel, successors all SKIPPED in DB
45. Multi-level skip cascade persisted correctly
46. CAS race — two workers complete fork branches simultaneously → no duplicate join dispatch
47. Worker death after CAS, before task insert → sweeper re-dispatches → COMPLETED
48. `replayWorkflow()` on failed DAG → resumes from correct activity → COMPLETED
49. Workflow deadline exceeded mid-DAG → TIMED_OUT; PENDING tasks cancelled
50. Cancel API mid-fork → CANCELLED; PENDING branch tasks cancelled

---

## 8. What Does NOT Change

| Component | Status |
|---|---|
| CAS lock-free barrier (version-based optimistic lock) | Unchanged |
| Task table schema (+ `activityName` column, `SKIPPED` status) | Additive only |
| Worker loop (poll → claim → execute → report) | Unchanged |
| Retry / backoff / deadline per task | Unchanged |
| Fan-out data parallelism mechanics | Unchanged |
| Dead-letter replay | Unchanged |
| Leader sweeper patrol interval and grace period | Unchanged |
| Metrics, health probes, shutdown coordinator | Unchanged |
| JoinPolicy variants (All, Threshold, Percentage) | Unchanged |
| FailurePolicy enum values | Unchanged |

---

## 9. Migration: `dispatchWorkflow`

| Before | After |
|---|---|
| `activity("scatter") { fanOut("simulate") }` | `activity("scatter") { fanOut { transition(...) }; next("join") }` |
| `activity("simulate") { ... joinPolicy(...) }` | Removed — embedded in scatter's `FanOutDefinition` |
| `inputs { "batchToken" from "simulate.batchToken" }` | `inputs { "batchToken" from "scatter.batchToken" }` — resolves from the SCATTER phase task result; if batchToken originates from parallel task results, `ActivityInputResolver` must be extended to aggregate fan-out results (implementation decision required) |
| `workflow.current_sequence` column | Dropped |
| `task.sequence_number` semantics | Now unique per activity (not shared per level) |
