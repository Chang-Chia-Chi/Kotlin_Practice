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

#### BarrierService — `onTaskCompleted`

Merge the two transactions into one. Thread `resultJson` through to `executeDecision` for fan-out:

```kotlin
suspend fun onTaskCompleted(
    taskId: String,
    workflowId: String,
    sequenceNumber: Int,
    status: TaskStatus,
    resultJson: String?,
    claimedBy: String? = null,
    claimedAt: Instant? = null,
) {
    var signalQueue: String? = null

    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
        if (!updated) return@inTransactionSuspend

        val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
        if (nonTerminal > 0) return@inTransactionSuspend

        signalQueue = evaluateAndAdvance(handle, workflowId, sequenceNumber, resultJson)
    }

    if (signalQueue != null) notifier.signal(signalQueue!!)
}
```

Thread `resultJson: String?` through `evaluateAndAdvance` → `resolveAndExecute` → `executeDecision`. The sweeper's `recoverStuckWorkflow` passes `resultJson = null` (it never reaches the PARALLEL branch — see Edge Case 3).

#### BarrierService — `executeDecision`

Collapse the `PARALLEL`/`LINEAR` branch into a single `insertBatchWithHandle` call. Both paths just build a task list differently:

```kotlin
val nextSeqInfo = sequenceMap[decision.nextSequence]!!
val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
val tasks = when (nextSeqInfo.phaseType) {
    PhaseType.PARALLEL -> {
        val items: List<String> = objectMapper.readValue(
            resultJson ?: throw IllegalStateException(
                "PARALLEL phase requires scatter result but none provided for workflow ${workflow.id}"
            )
        )
        require(items.isNotEmpty()) {
            "Fan-out produced 0 items for workflow ${workflow.id}. Scatter handler must return a non-empty JSON array."
        }
        items.map { createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now, item = it) }
    }
    PhaseType.LINEAR -> {
        listOf(createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now))
    }
}
taskRepo.insertBatchWithHandle(handle, tasks)
```

#### TaskRepository

Delete `insertFanOutFromScatter` entirely. No replacement method needed — the task list construction moves into `executeDecision` and feeds into the existing `insertBatchWithHandle`.

#### WorkerLoop

No changes needed. The routing is handled entirely inside BarrierService.

#### Sweeper

No changes needed. The sweeper cannot encounter a scatter→parallel transition with the atomic design. If the scatter task is stuck in PROCESSING, the deadline sweeper expires it and the worker retries.

### What Stays the Same

- `onTaskCompleted` signature — unchanged (same parameters, same callers)
- `recoverStuckWorkflow` — unchanged
- `insertBatchWithHandle` — reused for both LINEAR and PARALLEL
- Handler contract (`TransitionHandler`) — unchanged
- `HandlerOutput` — unchanged
- `WorkerLoop` — unchanged

### Edge Cases

1. **Scatter handler returns null/empty result**: `onScatterTaskCompleted` requires non-null `resultJson`. The parsed list is validated non-empty (same as current `require(inserted > 0)` check). On empty, the transaction rolls back and the task fails through the normal retry path.

2. **Very large fan-out (thousands of items)**: Items are held in JVM memory briefly during the batch INSERT. This is bounded by the scatter handler's output, which was already fully materialized in memory by `handler.execute()`. The batch INSERT is more efficient than the current `JSON_TABLE` approach for large arrays.

3. **CAS contention on workflow advance**: Not a concern with the atomic design. The sweeper's `recoverStuckWorkflow` can never encounter a scatter→parallel transition:
   - If the worker's atomic transaction hasn't committed, the scatter task is still PROCESSING → `nonTerminal > 0` → sweeper returns early.
   - If the worker's atomic transaction committed, the workflow's `currentSequence` has advanced past the scatter → sweeper evaluates the new sequence, not the scatter.

   There is no state where the scatter task is COMPLETED but fan-out tasks are missing. The `executeDecision` path for `PARALLEL` phases (currently `insertFanOutFromScatter`) is only reachable from `onScatterTaskCompleted`, which always has the in-memory result. The sweeper never needs to perform scatter fan-out, so it never needs the CLOB.
