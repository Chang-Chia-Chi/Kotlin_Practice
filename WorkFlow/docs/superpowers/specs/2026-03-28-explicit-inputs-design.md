# Explicit Inputs & Item Column Design

**Date:** 2026-03-28
**Status:** Approved

## Problem

The engine currently auto-forwards `resultJson` from the previous task as `payloadJson` on the next task during phase transitions. This is implicit — handlers receive the full previous result regardless of whether they need it, need part of it, or need nothing at all.

## Design Decisions

- Adopt Kestra-style explicit input declarations in the workflow DSL
- Handlers that need data from previous activities must declare it via `inputs {}`
- No inputs declared = handler receives null (no data)
- For scatter-to-parallel fan-out, the engine assigns each parallel task its chunk via an `item` field (equivalent to Kestra's `taskrun.value`)
- Input resolution happens at execution time (in the worker), not at barrier/transition time
- The `payloadJson` column is removed from the task table

### Why not auto-forward (current model)?

- Handlers receive data they may not need
- Data dependencies between activities are implicit and invisible in the workflow definition
- Inconsistent with how Temporal and Kestra handle data passing (both require explicit data routing)

### Why not remove payload entirely and have handlers query the DB?

- Breaks the clean handler contract (handlers would need DB access)
- Couples handlers to engine internals (workflow IDs, sequence numbers)
- Extra DB round-trip per handler invocation

### Why field-level references instead of activity-level?

- Handlers receive exactly the fields they need — clean contract
- Data dependencies are precise and visible in the DSL
- Handlers don't need to parse/extract from a large blob
- Activity-level references can still be used when the full result is needed

### Why `item` column instead of `payloadJson` for parallel tasks?

- Semantically clear: "the item this task is processing" vs. generic "payload"
- Matches Kestra's `taskrun.value` concept
- Restricted to scatter-to-parallel only — not a general-purpose data-passing mechanism
- `payloadJson` carried implicit "auto-forwarded from previous" semantics; `item` does not

### Why resolve at execution time (worker) instead of barrier time?

- Barrier becomes simpler (pure state-machine logic, no data resolution)
- Phase strategies no longer carry payload forwarding logic
- Keeps the barrier transaction minimal (CAS + task inserts only)
- Resolution cost is borne by the worker that will use the data

## DSL

### Field-level inputs

```kotlin
activity("notify") {
    transition("batch.notify")
    inputs {
        "chunks" from "split.uri"       // field "uri" from split's resultJson
        "count" from "split.total"      // field "total" from split's resultJson
        "meta" from "enrich.summary"    // field "summary" from enrich's resultJson
    }
}
```

### Whole-result inputs

```kotlin
activity("aggregate") {
    transition("batch.aggregate")
    inputs {
        "data" from "split"             // entire resultJson, no field path
    }
}
```

### No inputs (handler receives null)

```kotlin
activity("cleanup") {
    transition("batch.cleanup")
}
```

### Fan-out with downstream references

```kotlin
activity("split") {
    transition("dsl.split")
    inputs {
        "config" from "init.settings"
    }
    fanOut {
        transition("dsl.process")
        // parallel handlers receive their chunk via task.item (engine-assigned)
    }
}

activity("notify") {
    transition("batch.notify")
    inputs {
        "uris" from "split.uri"         // extracted per-element from fan-out results
        "results" from "split"          // whole aggregated array
    }
}
```

### Reference syntax

- `"activityName"` — entire resultJson from that activity
- `"activityName.fieldName"` — top-level field from that activity's resultJson
- For fan-out activities: field path is applied per-element, result is always an array

### Fan-out activity output resolution

When a downstream activity references a fan-out activity:
- `"split"` returns `[{...}, {...}, ...]` (aggregated array of all parallel task results)
- `"split.uri"` returns `["s3://chunk-1", "s3://chunk-2", ...]` (field extracted from each element)

## Data Model Changes

### ActivityDefinition

```kotlin
data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: FanOutDefinition? = null,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap()  // NEW: "inputName" to "activityName" or "activityName.field"
)
```

### Task

```kotlin
data class Task(
    val id: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val status: TaskStatus,
    val handlerKey: String,
    val item: String? = null,          // NEW: scatter chunk, parallel tasks only
    // payloadJson: REMOVED
    val resultJson: String?,
    val claimedBy: String?,
    val claimedAt: Instant?,
    val completedAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
    val notBefore: Instant?,
    val backoffBase: Int,
    val backoffCap: Int,
    val enqueuedAt: Instant,
    val queueName: String
)
```

### DB Migration

```sql
-- Add item column
ALTER TABLE task ADD (item CLOB);

-- Drop payload column (safe: no running workflows depend on historical payload data
-- since inputs are now resolved at execution time from resultJson)
ALTER TABLE task DROP COLUMN payload;
```

Note: This migration is destructive for the `payload` column. It is safe because the new design resolves inputs from `resultJson` of previous activities, not from stored payloads. Any in-flight workflows at migration time should be drained first.

## Input Resolution (InputResolver)

New single-responsibility class that resolves declared inputs at execution time.

### Interface

```kotlin
class InputResolver(private val objectMapper: ObjectMapper) {

    fun resolve(
        inputs: Map<String, String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: (Int) -> List<Task>
    ): String?
}
```

### Resolution algorithm

1. If `inputs` is empty, return null
2. For each input entry, parse the reference string:
   - `"activityName.field"` -> activity name + field path
   - `"activityName"` -> activity name, no field path
3. Map activity name to sequence number(s) via `sequenceMap`
4. Fetch completed tasks at that sequence via `tasksBySequence`
5. If the activity is a fan-out (has parallel sequence):
   - Fetch all completed parallel tasks
   - If field path specified: extract field from each task's resultJson, return array of values
   - If no field path: return array of full resultJson objects
6. If the activity is linear:
   - Get single completed task's resultJson
   - If field path specified: extract field
   - If no field path: return whole resultJson
7. Build result map: `{ "inputName": resolvedValue, ... }`
8. Serialize as JSON string

### Where it runs

In the worker, after claiming a task and before invoking the handler:

```
Worker claims task
  -> reads workflow definitionJson from workflow table
  -> builds sequenceMap
  -> finds this task's ActivityDefinition
  -> calls InputResolver.resolve(activity.inputs, sequenceMap, taskQueryFn)
  -> calls handler(resolvedInputs, task.item)
```

## Phase Strategy Simplification

Phase strategies no longer carry payload forwarding logic.

### LinearPhaseStrategy

Before:
```kotlin
val payload = context.tasks.firstOrNull()?.resultJson
context.failOrAdvance(payload)?.let { return it }
return context.advanceOrComplete(payload)
```

After:
```kotlin
context.failOrAdvance()?.let { return it }
return context.advanceOrComplete()
```

### ScatterPhaseStrategy

Before: creates parallel tasks with sliced payloadJson.

After: creates parallel tasks with sliced `item` instead. Core slicing logic unchanged, just targets a different field.

### ParallelPhaseStrategy

Before: aggregates all completed resultJson into array, passes as payload to next phase.

After: evaluates join policy only. No aggregation — downstream activities' InputResolver handles it lazily when needed.

```kotlin
// Before
val arrayNode = objectMapper.createArrayNode()
context.tasks.filter { it.status == COMPLETED }
    .mapNotNull { it.resultJson }
    .forEach { arrayNode.add(objectMapper.readTree(it)) }
val aggregatedPayload = objectMapper.writeValueAsString(arrayNode)
return context.advanceOrComplete(payload = aggregatedPayload)

// After
return context.advanceOrComplete()
```

## Handler Contract

```kotlin
// Before
fun handle(payloadJson: String?): String?

// After
fun handle(inputs: Map<String, Any?>?, item: String?): String?
```

- `inputs`: resolved from DSL `inputs {}` declaration. Null if none declared.
- `item`: the scatter chunk. Null for non-parallel tasks.
- Returns: `resultJson`

## Data Flow Summary

| Case | inputs param | item param | Source |
|------|-------------|------------|--------|
| LINEAR, no inputs declared | null | null | Nothing |
| LINEAR, with inputs declared | resolved map | null | InputResolver at execution time |
| SCATTER, no inputs declared | null | null | Nothing |
| SCATTER, with inputs declared | resolved map | null | InputResolver at execution time |
| PARALLEL task | null | chunk string | Engine-assigned at scatter barrier |

## Files Affected

| File | Change |
|------|--------|
| `src/main/kotlin/dsl/WorkflowDsl.kt` | Add `inputs` to `ActivityDefinition` |
| `src/main/kotlin/dsl/WorkflowDslBuilders.kt` | Add `InputsBuilder`, `inputs {}` DSL block |
| `src/main/kotlin/engine/WorkflowModels.kt` | Remove `payloadJson`, add `item` to `Task` |
| `src/main/kotlin/engine/TaskRepository.kt` | Update SQL: drop payload, add item column |
| `src/main/kotlin/engine/LinearPhaseStrategy.kt` | Remove payload forwarding |
| `src/main/kotlin/engine/ScatterPhaseStrategy.kt` | Use `item` instead of `payloadJson` for parallel tasks |
| `src/main/kotlin/engine/ParallelPhaseStrategy.kt` | Remove result aggregation and payload forwarding |
| `src/main/kotlin/engine/PhaseStrategy.kt` | Remove payload from `advanceOrComplete()` / `Advance` |
| `src/main/kotlin/engine/BarrierService.kt` | Remove payload from `executeDecision()` |
| `src/main/kotlin/engine/InputResolver.kt` | NEW: input resolution logic |
| `src/main/resources/db/migration/V8__explicit_inputs.sql` | Add `item`, drop `payload` |
| Worker loop (wherever handler invocation lives) | Build InputResolver context, pass inputs + item to handler |
| All existing tests | Update for new handler contract and removed payloadJson |
