# Atomic Scatter Fan-Out Design

## Problem

The current scatter-to-fan-out path stores the scatter handler's result as a CLOB in the `task.result` column, then reads it back from the database via `JSON_TABLE` to create fan-out tasks. This has two issues:

1. **Storage**: A scatter handler returning thousands of items produces a large JSON array stored as a single CLOB on one row.
2. **Efficiency**: The result round-trips through the database (memory -> CLOB -> JSON_TABLE -> INSERT) even though it's already available in JVM memory at the call site.

### Current Flow

```
WorkerLoop: handler.execute() -> HandlerOutput(result = "[{...}, {...}, ...]")
    |
    v
BarrierService.onTaskCompleted(resultJson = output.result)
    |
    +--> Transaction 1: UPDATE task SET result = :resultJson  (write CLOB)
    |
    +--> Transaction 2: evaluateAndAdvance()
              |
              +--> insertFanOutFromScatter()
                        |
                        +--> INSERT...SELECT FROM JSON_TABLE(t.result)  (read CLOB back)
```

The two-transaction split exists so that if Transaction 2 fails, the scatter task is already marked COMPLETED with its result persisted. The Sweeper can then recover the workflow by re-reading the CLOB and retrying the fan-out. However, this recovery model forces the CLOB round-trip.

## Design

Merge the task completion and fan-out insertion into a single atomic transaction for the scatter-to-parallel path. Pass the handler's in-memory result directly to a batch INSERT instead of round-tripping through a CLOB.

### New Flow

```
WorkerLoop: handler.execute() -> HandlerOutput(result = "[{...}, {...}, ...]")
    |
    v
BarrierService.onTaskCompleted(resultJson = output.result)
    |
    +--> Single Transaction:
            1. UPDATE task SET status='COMPLETED', result=NULL  (no CLOB)
            2. Count non-terminal tasks
            3. If barrier satisfied: evaluateAndAdvance()
                 -> insertFanOutTasks(items parsed from in-memory resultJson)
```

### Recovery Model

With a single transaction, the failure modes simplify:

| Scenario | State after failure | Recovery |
|---|---|---|
| Transaction commits | Scatter COMPLETED + fan-out tasks exist | Nothing to recover |
| Transaction rolls back (crash, DB error) | Scatter stays PROCESSING | Deadline sweeper expires it -> worker retries scatter handler -> re-produces list -> transaction re-attempts |
| Worker crash before transaction | Scatter stays PROCESSING | Same as above |

There is no state where "scatter is COMPLETED but fan-out tasks are missing." The gap that required CLOB-based recovery no longer exists.

This is safe because scatter handlers are inherently idempotent: they produce a list from current state (the `TransitionHandler` contract already requires idempotency).

### Changes

#### BarrierService

Introduce `onScatterTaskCompleted` that runs everything in one transaction:

```kotlin
suspend fun onScatterTaskCompleted(
    taskId: String,
    workflowId: String,
    sequenceNumber: Int,
    resultJson: String,        // non-null: scatter must produce items
    claimedBy: String?,
    claimedAt: Instant?,
) {
    var signalQueue: String? = null

    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        // 1. Complete scatter task WITHOUT storing result CLOB
        val updated = taskRepo.updateStatusWithHandle(
            handle, taskId, TaskStatus.COMPLETED,
            resultJson = null, claimedBy, claimedAt,
        )
        if (!updated) return@inTransactionSuspend

        // 2. Check barrier (scatter is a single task, so count should be 0 now)
        val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
        if (nonTerminal > 0) return@inTransactionSuspend

        // 3. Evaluate and advance, passing in-memory result for fan-out
        signalQueue = evaluateAndAdvance(handle, workflowId, sequenceNumber, scatterResult = resultJson)
    }

    if (signalQueue != null) notifier.signal(signalQueue!!)
}
```

The existing `onTaskCompleted` remains unchanged for all other paths (linear tasks, failed tasks, timed-out tasks).

#### TaskRepository

Replace `insertFanOutFromScatter` (which uses `JSON_TABLE`) with a new method that takes parsed items:

```kotlin
fun insertFanOutTasks(
    handle: Handle,
    workflowId: String,
    items: List<String>,       // each item is a JSON string
    targetSeqInfo: SequenceInfo,
    now: Instant,
) {
    require(items.isNotEmpty()) { "Fan-out items must not be empty" }
    val activity = targetSeqInfo.activity
    val deadlineAt = LocalDateTime.ofInstant(now.plus(activity.deadline), ZoneOffset.UTC)
        .truncatedTo(ChronoUnit.MICROS)
    // Build Task objects from items, then delegate to insertBatchWithHandle
    val tasks = items.map { item ->
        createTaskForActivity(workflowId, targetSeqInfo.sequenceNumber, activity, now)
            .copy(item = item)
    }
    insertBatchWithHandle(handle, tasks)
}
```

Delete the old `insertFanOutFromScatter` method entirely.

#### WorkerLoop

Detect scatter-to-parallel transitions and call the new method:

```kotlin
// After handler.execute():
if (isScatterTask(task, definition)) {
    barrierService.onScatterTaskCompleted(
        taskId = task.id,
        workflowId = task.workflowId,
        sequenceNumber = task.sequenceNumber,
        resultJson = output.result!!,
        claimedBy = task.claimedBy,
        claimedAt = task.claimedAt,
    )
} else {
    barrierService.onTaskCompleted(/* existing path */)
}
```

#### evaluateAndAdvance / executeDecision

Thread an optional `scatterResult: String?` parameter through `evaluateAndAdvance` → `resolveAndExecute` → `executeDecision`. In `executeDecision`, for the `PARALLEL` branch:
- If `scatterResult` is non-null: parse items, call `insertFanOutTasks`
- If `scatterResult` is null: this is an unreachable state (see Edge Case 3), throw `IllegalStateException`

The sweeper's `recoverStuckWorkflow` passes `scatterResult = null` (it never reaches the PARALLEL branch — see Edge Case 3).

#### Sweeper

No changes needed. The sweeper cannot encounter a scatter→parallel transition with the atomic design. If the scatter task is stuck in PROCESSING, the deadline sweeper expires it and the worker retries.

### Scatter Detection

The WorkerLoop needs to know whether a completed task is a scatter task whose next phase is parallel. Two options:

**Option A (recommended):** Query the workflow definition (already loaded for input resolution) and check if the current sequence's next phase is `PARALLEL`. This keeps the detection in WorkerLoop which already has the definition context.

**Option B:** Add a flag to the task row or handler output. Rejected: unnecessary schema change.

### What Stays the Same

- `onTaskCompleted` — unchanged, still used for linear tasks, failures, timeouts, sweeper-driven completions
- `recoverStuckWorkflow` — unchanged, still handles generic stuck workflows
- `insertBatchWithHandle` — reused for the new fan-out batch insert
- Handler contract (`TransitionHandler`) — unchanged
- `HandlerOutput` — unchanged

### Edge Cases

1. **Scatter handler returns null/empty result**: `onScatterTaskCompleted` requires non-null `resultJson`. The parsed list is validated non-empty (same as current `require(inserted > 0)` check). On empty, the transaction rolls back and the task fails through the normal retry path.

2. **Very large fan-out (thousands of items)**: Items are held in JVM memory briefly during the batch INSERT. This is bounded by the scatter handler's output, which was already fully materialized in memory by `handler.execute()`. The batch INSERT is more efficient than the current `JSON_TABLE` approach for large arrays.

3. **CAS contention on workflow advance**: Not a concern with the atomic design. The sweeper's `recoverStuckWorkflow` can never encounter a scatter→parallel transition:
   - If the worker's atomic transaction hasn't committed, the scatter task is still PROCESSING → `nonTerminal > 0` → sweeper returns early.
   - If the worker's atomic transaction committed, the workflow's `currentSequence` has advanced past the scatter → sweeper evaluates the new sequence, not the scatter.

   There is no state where the scatter task is COMPLETED but fan-out tasks are missing. The `executeDecision` path for `PARALLEL` phases (currently `insertFanOutFromScatter`) is only reachable from `onScatterTaskCompleted`, which always has the in-memory result. The sweeper never needs to perform scatter fan-out, so it never needs the CLOB.
