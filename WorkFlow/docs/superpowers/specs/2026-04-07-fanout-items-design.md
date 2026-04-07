# Fan-Out Items Durability & Type Safety

**Date:** 2026-04-07
**Branch:** misc/ai_gen

---

## Problem

`HandlerResult.Completed.items` is passed in-memory through a two-transaction settle path:

- **TX1** — commits `task.result` and task status to DB (durable)
- **TX2** — parses `itemsJson` in-memory to spawn PARALLEL child tasks (ephemeral)

A worker crash between TX1 and TX2 loses the items. `recoverStuckWorkflow` skips PARALLEL sequences, so the workflow gets stuck permanently.

Two secondary issues:
1. `items: String?` forces handlers to serialize to JSON and `DefaultPhaseGate` to deserialize — double round-trip with no type safety.
2. Each fan-out item carries `batchToken` which is identical across all items, inflating stored payload unnecessarily.

---

## Design

### 1. Schema — `task.items` CLOB

```sql
ALTER TABLE task ADD items CLOB;
```

- Populated only for SCATTER tasks; `NULL` for all others.
- Persisted in TX1 alongside `task.result`.
- At ~100 items × ~40 bytes each ≈ 4KB — within Oracle inline CLOB threshold (~32KB).

### 2. Type change — `HandlerResult.Completed`

```kotlin
// Before
data class Completed(val result: String?, val items: String? = null) : HandlerResult

// After
data class Completed(val result: String?, val items: List<String>? = null) : HandlerResult
```

Handlers return a typed list of ID strings. `DefaultPhaseGate` owns serialization (List → JSON) before TX1 and deserialization (JSON → List) on the recovery read path.

### 3. Strip shared context from items

`batchToken` is identical across all fan-out items. Handlers should return only the discriminating ID per item; shared context is carried in `task.result` (already durable in TX1).

`DispatchScatterHandler` returns:
```kotlin
HandlerResult.Completed(
    result = """{"batchToken":"$token"}""",
    items = configs.map { it.id },  // List<String> — plain configId strings
)
```

Stored in `task.items`:
```json
["uuid1", "uuid2", "uuid3"]
```

### 4. PhaseGate assembles full child task items

`FanOutDefinition` has no `inputs` field, and `DispatchSimulationHandler` reads both `configId` and `batchToken` from `input.item`. To preserve this contract without workflow DSL changes, `DefaultPhaseGate` assembles the full child `item` during ScatterExpand by merging each raw item string with the scatter task's `resultJson` fields:

```
stored items:    ["id1", "id2"]
scatter result:  {"batchToken": "tok"}
child task item: {"configId": "id1", "batchToken": "tok"}   ← assembled by PhaseGate
```

`DispatchSimulationHandler` requires **no changes** — it continues reading `configId` and `batchToken` from `input.item`.

### 5. Durability — TX1 persists items

`TaskRepository.updateStatusWithHandle` gains `itemsJson: String?`.

`DefaultPhaseGate.onTaskCompleted`:
1. Serializes `List<String>` → JSON string before TX1.
2. TX1 writes both `resultJson` and `itemsJson` to DB atomically.
3. TX2 uses the in-memory `itemsJson` (no extra DB read on the happy path).

Recovery path (`recoverStuckWorkflow`): detects SCATTER tasks that are COMPLETED but have no PARALLEL children, reads `task.items` from DB, and spawns the missing PARALLEL tasks with assembled items — no handler re-execution needed.

---

## Data Flow

```
DispatchScatterHandler.execute()
  → HandlerResult.Completed(result={"batchToken":tok}, items=["id1","id2",...])

WorkerLoop.executeAndReport()
  → TaskSettler.settle(resultJson, itemsJson=serialize(items))

DefaultPhaseGate.onTaskCompleted()
  TX1: task.result = {"batchToken":tok}, task.items = ["id1","id2",...]   ← durable
  TX2: merge each id + scatter resultJson → spawn PARALLEL tasks
       child task.item = {"configId":"id1","batchToken":"tok"}

DispatchSimulationHandler.execute()
  input.item = {"configId":"id1","batchToken":"tok"}    ← unchanged
```

---

## Idempotency

This design removes the handler idempotency requirement for recovery. The scatter handler runs exactly once. Recovery reads `task.items` from DB and spawns children directly — it never re-executes the handler. `DispatchScatterHandler`'s batch creation in Path B remains safe.

### Required Recovery Test (gap — currently uncovered)

There is no existing test for the TX1-success / TX2-fail scenario. This must be added before or alongside the implementation.

**Scenario:** Worker crashes after TX1 (SCATTER task is COMPLETED, `task.items` persisted to DB) but before TX2 spawns the PARALLEL tasks.

**Setup:**
1. Create workflow with a SCATTER → PARALLEL → LINEAR definition.
2. Insert the SCATTER task directly as COMPLETED with `items = ["id1","id2","id3"]` and `result = {"batchToken":"tok"}` — simulating TX1 having committed.
3. Assert no PARALLEL tasks exist.

**Action:** Call `recoverStuckWorkflow(workflowId)`.

**Assertions:**
- Exactly 3 PARALLEL tasks are created with statuses PENDING.
- Each child `task.item` = `{"configId":"idN","batchToken":"tok"}` (assembled from stored items + scatter result).
- Calling `recoverStuckWorkflow` a second time produces no additional tasks (idempotent recovery).

### Required Concurrent Duplicate Tests

The workflow row lock serializes `onTaskCompleted` TX2 and `recoverStuckWorkflow`. A recount under lock prevents double-insertion when they race. This must be verified explicitly.

| # | Scenario | Assertion |
|---|---|---|
| 1 | `recoverStuckWorkflow` called twice in succession after SCATTER completes with no PARALLEL tasks | Exactly N PARALLEL tasks after both calls — no duplicates |
| 2 | `onTaskCompleted` for SCATTER completes normally, then `recoverStuckWorkflow` called before any PARALLEL task is claimed | Still exactly N PARALLEL tasks — recovery is a no-op |

---

## S3 / External Object Store

Not required for this design. At ~4KB, Oracle inline CLOB is sufficient. A `ScatterItemStore` abstraction (DB vs S3 impl) can be layered on later with minimal disruption — the call site is a single method in `DefaultPhaseGate`.

---

## What Does NOT Change

- Two-transaction design in `DefaultPhaseGate` is preserved.
- `FanOutDefinition`, `PhaseType`, `DagRouter` are untouched.
- `DispatchSimulationHandler` input contract is unchanged.
- No external dependencies added.

---

## Out of Scope

- Cleanup of `task.items` after child tasks are spawned (dead data at low volume; addressable in a separate GC pass).
- Adding `inputs` support to `FanOutDefinition`.
- S3/MinIO storage backend for items.
