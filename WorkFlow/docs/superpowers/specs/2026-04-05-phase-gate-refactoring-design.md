# Phase Gate Refactoring: DagRouter Extraction, Lock-Based Orchestration, and Repository Cleanup

**Date:** 2026-04-05
**Status:** Draft

## Problem Statement

`DefaultPhaseGate` (579 lines) is a god-class that conflates three concerns: pure DAG routing logic, transaction orchestration with CAS-based concurrency control, and repository interactions. The CAS retry mechanism was designed for linear/sequential workflows and is a poor fit for DAG topologies where concurrent branch completions are structurally guaranteed. Additionally, `TaskRepository` has accumulated 28 methods, many of which are superseded by the bulk `countStatusSummariesByWorkflowWithHandle` query.

## Goals

1. **Readability/maintainability:** Make `DefaultPhaseGate` easy to reason about when adding new phase types or edge semantics.
2. **Testability:** Extract pure DAG logic so it can be unit-tested without DB, transactions, or mocks.
3. **Correctness:** Replace CAS retry with pessimistic locking to eliminate both the last-mile race condition and wasted retry work.
4. **Interface hygiene:** Remove dead methods from `TaskRepository`.

## Non-Goals

- Changing the fundamental DB-backed queue architecture.
- Adding new workflow features (signals, timers, etc.).
- Modifying the `WorkerLoop`, `WorkflowEngine`, or `WorkflowWatchdog` beyond minimal call-site updates.

---

## Phase 1: Extract `DagRouter` (Pure Domain Logic)

### New File

`workflow/usecase/service/orchestration/DagRouter.kt`

### Responsibility

All DAG navigation and decision-making, extracted as pure functions operating on an immutable snapshot. Zero dependencies on JDBI, CDI, Jackson, or any framework.

### Data Types

```kotlin
data class GateSnapshot(
    val workflowId: String,
    val definition: WorkflowDefinition,
    val sequenceMap: Map<Int, SequenceInfo>,
    val seqByName: Map<String, SequenceInfo>,
    val allCounts: Map<Int, TaskStatusCounts>,
    val tasksBySeq: Map<Int, List<Task>>,
    val now: Instant,
)

sealed interface PhaseDecision {
    data object Abort : PhaseDecision
    data class ScatterExpand(val items: List<String>, val parallelInfo: SequenceInfo) : PhaseDecision
    data object ForceDefaultBranch : PhaseDecision
    data object Normal : PhaseDecision
}

data class SuccessorResult(
    val tasksToInsert: List<Task>,
    val signalQueues: Set<String>,
    val hasTerminalCompletion: Boolean,
)
```

### Functions Extracted

All functions are pure (no side effects, no I/O):

| Function | Current Location | Notes |
|---|---|---|
| `resolvePhaseDecision(snapshot, seqInfo, status, scatterItems?)` | DefaultPhaseGate L218-259 | Caller deserializes scatter items before calling; DagRouter never touches Jackson |
| `dispatchSuccessors(snapshot, seqInfo, forceDefault)` | DefaultPhaseGate L286-378 | Indegree-based topological BFS (Kahn's algorithm) |
| `successorsOf(seqInfo, seqByName, definition)` | DefaultPhaseGate L473-483 | |
| `isAnyEdgeTaken(tasksBySeq, successor, sequenceMap, definition)` | DefaultPhaseGate L512-538 | |
| `isEdgeTaken(task, edgeLabel, failurePolicy)` | DefaultPhaseGate L543-559 | |
| `hasDefaultBranchEdge(successor, definition)` | DefaultPhaseGate L490-500 | |
| `evaluateJoinPolicy(joinPolicy, completedCount, totalCount)` | DefaultPhaseGate L565-573 | |

### Design Decision: Scatter Item Deserialization

Currently `resolvePhaseDecision` calls `objectMapper.readValue<List<String>>(resultJson)` to parse scatter items. This is the one impure operation. The refactored design moves deserialization to the caller (`DefaultPhaseGate`) and passes `scatterItems: List<String>?` to `DagRouter`. This keeps `DagRouter` completely free of Jackson.

### Testing

`DagRouterTest` — pure unit tests using `runTest`. Construct `GateSnapshot` values directly, call functions, assert return values. No DB, no mocks, no containers. Tests cover:

- Linear completion dispatches successor
- Conditional routing: edge label match, default branch, no-match skip
- Cascade skip propagation through chains
- Diamond join: waits for all predecessors
- Scatter expand: produces ScatterExpand decision with parallel info
- Join policy evaluation: All, Threshold, Percentage
- BEST_EFFORT failure policy: forces default branch
- ABORT failure policy: produces Abort decision
- Terminal activity with no successors: empty result
- Scatter skip cascades to companion parallel node

---

## Phase 2: Lock-Based Orchestration (Replace CAS)

### Motivation

The CAS retry mechanism was designed for linear workflows where only one task completes at a time. In DAG topologies, concurrent branch completions are structurally guaranteed by the DAG's shape (e.g., diamond joins). The CAS approach:

1. **Wastes work:** The losing transaction builds the full `GateSnapshot`, evaluates the DAG, prepares inserts, then throws everything away and retries from scratch.
2. **Doesn't solve the last-mile race:** When the last N tasks at a sequence complete concurrently, Oracle READ COMMITTED means each transaction sees others' updates as uncommitted. Multiple workers can see `nonTerminal > 0` and return early, with NO worker advancing the DAG. The watchdog eventually recovers, but this adds latency equal to the watchdog interval.

### The Fix

Replace the optimistic CAS retry with a two-transaction design using pessimistic `SELECT ... FOR UPDATE` on the workflow row.

**Why two transactions?** A single transaction that updates the task status AND counts non-terminal tasks suffers from READ COMMITTED invisibility: concurrent completers cannot see each other's uncommitted updates. The fast-path count overestimates non-terminal tasks, and under high concurrency ALL completers may take the fast-path exit — nobody advances the DAG. Splitting into two transactions ensures TX1 commits the status update before TX2's count query runs, making the fast-path accurate.

### New Flow: `onTaskCompleted`

```
TX1 (fenced task update — commit immediately):
1. BEGIN TRANSACTION
2. Update task to terminal status (fenced write on own task row)
3. If not updated (idempotent fence) → return early
4. COMMIT (task status is now visible to all readers)

TX2 (fast-path probe + lock + route):
5. BEGIN TRANSACTION
6. COUNT non-terminal at this sequence (accurate — sees all committed TX1s)
7. If nonTerminal > LAST_MILE_THRESHOLD → return early (fast path, vast majority of calls)
8. SELECT * FROM workflow WHERE id = :id FOR UPDATE (acquires row lock)
9. If workflow.status != RUNNING → return early
10. Recount non-terminal at this sequence (definitive — holds lock)
11. If nonTerminal > 0 → return early (lock released on commit)
12. Build GateSnapshot, call DagRouter.resolvePhaseDecision / dispatchSuccessors
13. Apply effects (insert tasks, abort workflow, mark completed)
14. Unconditional version increment (audit only)
15. COMMIT (releases lock)
```

**Crash safety:** If the process crashes between TX1 and TX2, the task is correctly marked COMPLETED but the DAG is not advanced. This is the same failure mode as a crash after commit but before queue notification — `recoverStuckWorkflow` (watchdog) handles it.

### New Flow: `recoverStuckWorkflow`

```
1. BEGIN TRANSACTION
2. SELECT * FROM workflow WHERE id = :id FOR UPDATE
3. If workflow.status != RUNNING → return early
4. Build GateSnapshot, evaluate all sequences for missing tasks
5. Insert missing PENDING/SKIPPED tasks
6. Check global completion, mark terminal status if done
7. Unconditional version increment (audit only)
8. COMMIT
```

### What Gets Deleted

- `withCasRetry` loop
- `requireCasWin` method
- `RetryableException` class
- `MAX_CAS_RETRIES` constant
- `GateContext` inner class (replaced by `GateSnapshot` from Phase 1)

### What Gets Added

- `workflowRepo.findByIdForUpdate(handle, workflowId)` — new repository method, `SELECT * FROM workflow WHERE id = :id FOR UPDATE`
- `LAST_MILE_THRESHOLD` — configurable via `PhaseGateConfig`, default 4

### Configuration

```properties
framework.phase-gate.last-mile-threshold=4
```

The threshold controls when the workflow-row lock is acquired:
- `nonTerminal > threshold` → fast path, return early (no lock). The vast majority of task completions. Accurate because TX1 committed the status before this count runs.
- `0 < nonTerminal <= threshold` → acquire lock, recount. Serializes the "last mile" completions to ensure exactly one completer advances the DAG.
- `nonTerminal == 0` on first count → acquire lock, proceed to DAG evaluation.

### Version Field

The `version` column remains in the `workflow` table as an audit counter. It is incremented unconditionally on every DAG advancement but is no longer used for concurrency control. The `casVersionWithHandle` method is removed from `WorkflowRepository`.

### Why This Is Safe

The two-transaction split plus `SELECT ... FOR UPDATE` eliminates both the READ COMMITTED visibility gap and CAS contention:

- **No lost wakeups:** TX1 commits the task status before TX2 counts. Concurrent completers' committed updates are visible to TX2's count query. The fast-path probe is accurate — it cannot overestimate non-terminal tasks due to uncommitted concurrent updates.
- **CAS contention on diamond joins:** The second branch blocks on the `FOR UPDATE` lock until the first commits, then reads accurate state. No retry needed.
- **Last-mile serialization:** At most one TX2 at a time is inside steps 8-15. The recount at step 10 sees all committed state from prior lock holders.
- **Fast-path effectiveness improves:** In the single-TX design, the count overestimates non-terminal tasks (can't see concurrent uncommitted completions), causing unnecessary lock acquisitions. With 2 TXs, the count is accurate, so more tasks correctly take the fast-path exit.

Lock hold time is bounded: one COUNT query + DagRouter evaluation (pure, microseconds) + batch insert. Low single-digit milliseconds in the worst case.

---

## Phase 3: `TaskRepository` Interface Cleanup

### Methods to Remove

These are dead code — no production callers. Only exercised by repository-level tests that test the methods themselves.

**Suspend wrappers (superseded by Handle-based or bulk equivalents):**
- `countNonTerminal(workflowId, sequenceNumber)` — production uses `countNonTerminalWithHandle`
- `countFailed(workflowId, sequenceNumber)` — superseded by `countStatusSummaries`
- `countTotal(workflowId, sequenceNumber)` — superseded by `countStatusSummaries`
- `updateStatus(id, newStatus, resultJson)` — production uses `updateStatusWithHandle`

**Handle-based (superseded by `countStatusSummariesByWorkflowWithHandle`):**
- `countFailedWithHandle(handle, workflowId, sequenceNumber)`
- `countTotalWithHandle(handle, workflowId, sequenceNumber)`
- `countCompletedWithHandle(handle, workflowId, sequenceNumber)`

**Handle-based (zero production callers):**
- `findByWorkflowAndSequenceWithHandle(handle, workflowId, sequenceNumber)`

### Also Removed from `WorkflowRepository` (Phase 2)

- `casVersionWithHandle(handle, workflowId, expectedVersion)` — CAS no longer used

### Result

`TaskRepository` interface goes from 28 methods to 20 methods. Corresponding tests in `RepositoryTest` for the removed methods are deleted.

### Methods Retained

| Method | Primary Caller |
|---|---|
| `insertBatch(suspend)` | Standalone inserts |
| `claimNext(suspend)` | WorkerLoop |
| `findByWorkflowAndSequence(suspend)` | WorkerLoop input resolution |
| `resetForRetry(suspend)` | WorkerLoop |
| `replayDeadLetterTask(suspend)` | API endpoint |
| `replayDeadLetterBatch(suspend)` | WorkflowEngine |
| `findExpired(suspend)` | WorkflowWatchdog |
| `resetStaleTasks(suspend)` | WorkflowWatchdog |
| `deadLetterExhaustedTasks(suspend)` | WorkflowWatchdog |
| `updateStatusWithHandle` | DefaultPhaseGate |
| `countNonTerminalWithHandle` | DefaultPhaseGate barrier probe |
| `countAllNonTerminalWithHandle` | DefaultPhaseGate completion check |
| `countStatusSummariesByWorkflowWithHandle` | DefaultPhaseGate snapshot |
| `findByWorkflowIdWithHandle` | DefaultPhaseGate snapshot |
| `insertBatchWithHandle` | DefaultPhaseGate, WorkflowEngine |
| `cancelPendingTasksWithHandle` | DefaultPhaseGate, WorkflowEngine |
| `replayDeadLetterBatchWithHandle` | WorkflowEngine |
| `findDistinctQueuesByWorkflowId` | WorkflowEngine replay |
| `cancelTasksForOverdueWorkflowsWithHandle` | WorkflowWatchdog |

---

## Phase 4: Integration Test — Multi-Terminal DAG with Asymmetric Depth and Conditional Routing

### Motivation

Existing tests cover diamond joins and simple forks, but no test verifies workflow completion when a DAG has multiple independent terminal nodes at different depths, reached via conditional routing and skip cascades. This is the topology that most stresses the global completion check and the last-mile lock serialization.

### Test Topology

```
          ┌──► fast (terminal, depth 1)
start ──┤
          └──► router ──(A)──► deep1 ──► deep2 (terminal, depth 3)
                       └──(B)──► alt (terminal, depth 2)
```

- `start` forks unconditionally into `fast` and `router`
- `router` conditionally routes to either `deep1 → deep2` (depth 3) or `alt` (depth 2)
- Terminal nodes exist at three different depths depending on the branch taken

### Test Cases

Located in `WorkflowIntegrationTest.kt`, new `@Nested inner class MultiTerminalDagCompletion`.

#### Test 1: Branch A taken — terminals at depth 1 and depth 3

```
Route: start → fast (terminal), start → router →(A)→ deep1 → deep2 (terminal)
Skipped: alt

Steps:
1. Start workflow, complete "start" → fast PENDING, router PENDING
2. Complete "router" with branch=A → deep1 PENDING, alt SKIPPED
3. Complete "fast" → workflow RUNNING (deep1 still PENDING)
4. Complete "deep1" → deep2 PENDING, workflow RUNNING
5. Complete "deep2" → workflow COMPLETED

Assertions at each step:
- Step 2: alt is SKIPPED, deep2 not yet created
- Step 3: workflow still RUNNING despite fast being terminal and completed
- Step 5: all tasks globally terminal, workflow COMPLETED
```

#### Test 2: Branch B taken — terminals at depth 1 and depth 2, skip cascade to depth 3

```
Route: start → fast (terminal), start → router →(B)→ alt (terminal)
Skipped: deep1, deep2 (cascade)

Steps:
1. Start workflow, complete "start" → fast PENDING, router PENDING
2. Complete "router" with branch=B → alt PENDING, deep1 SKIPPED, deep2 SKIPPED
3. Complete "alt" → workflow RUNNING (fast still PENDING)
4. Complete "fast" → workflow COMPLETED

Assertions:
- Step 2: deep1 SKIPPED, deep2 SKIPPED (cascade), alt PENDING
- Step 4: global completion check passes — all tasks terminal (mix of COMPLETED and SKIPPED)
```

#### Test 3: Branch A taken, fast completes first — early terminal does not short-circuit

Same as Test 1 but `fast` completes before `router`:

```
Steps:
1. Complete "start" → fast PENDING, router PENDING
2. Complete "fast" → workflow RUNNING
3. Complete "router" with branch=A → deep1 PENDING, alt SKIPPED
4. Complete "deep1" → deep2 PENDING
5. Complete "deep2" → workflow COMPLETED
```

Verifies that a terminal node completing early (step 2) does not prematurely trigger global completion when non-terminal tasks remain on other branches.

#### Test 4: Concurrent terminal completions — both terminals finish simultaneously

Branch B taken. `fast` and `alt` are the two remaining terminals. Complete both via `async/awaitAll`:

```
Steps:
1. Complete "start", complete "router" with branch=B
2. awaitAll(complete("fast"), complete("alt"))
3. Workflow reaches COMPLETED (not stuck in RUNNING)
4. No duplicate tasks at any sequence
```

Directly exercises the lock-based last-mile serialization under concurrent terminal completions at different sequences.

---

## Implementation Phases

Each phase is independently mergeable and testable:

| Phase | Scope | Files Changed |
|---|---|---|
| 1 | Extract `DagRouter` + unit tests | 2 new (DagRouter.kt, DagRouterTest.kt), 0 modified |
| 2 | Lock-based `DefaultPhaseGate` + config | ~4 modified (DefaultPhaseGate, WorkflowRepository, JdbiWorkflowRepository, + PhaseGateConfig new), existing tests updated |
| 3 | `TaskRepository` cleanup | 3 modified (TaskRepository, JdbiTaskRepository, RepositoryTest) |
| 4 | Multi-terminal integration tests | 1 modified (WorkflowIntegrationTest) |

Phase 1 has zero production code changes (additive only). Phase 2 is the behavioral change. Phase 3 is pure deletion. Phase 4 is test-only.
