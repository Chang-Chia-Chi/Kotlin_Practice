# Independent Fan-Out Activity Design

## Problem

`evaluateAndAdvance` loads **all** tasks for a sequence into memory via `findByWorkflowAndSequenceWithHandle`. Each `Task` carries two CLOBs (`item`, `resultJson`). At thousands to tens of thousands of tasks per sequence, this is an OOM risk.

The root cause is structural: fan-out is nested inside the scatter activity (`FanOutDefinition`), forcing the engine to load all tasks into `PhaseContext` so strategies can inspect them. But most strategies only need counts, and the one that doesn't (`ScatterPhaseStrategy`) only needs the single completed scatter task's result.

## Solution

Promote fan-out from a nested definition to an independent, named activity. Strategies use counts only. Fan-out task creation happens via SQL INSERT...SELECT. Input resolution between activities uses declared `inputs {}` with the resolver determining single vs aggregate from the workflow definition.

## DSL

```kotlin
workflow("dispatch-and-upload") {
    activity("read-configs") {
        transition("read-configs-handler")
        fanOut("compute-dispatch")           // my result fans out into compute-dispatch
    }
    activity("compute-dispatch") {
        transition("compute-dispatch-handler")
        retries(3)
        deadline(Duration.ofMinutes(10))
    }
    activity("join-to-parquet") {
        transition("join-upload-handler")
        inputs {
            "batchId" from "read-configs.batchId"                // single task, traverse field
            // "results" from "compute-dispatch.processingResult" // N tasks, aggregate into array
        }
    }
}
```

- `fanOut("compute-dispatch")` on the scatter activity declares the relationship. The scatter handler returns a JSON array; the engine creates one task per item for the target activity.
- The fan-out target is an independent activity with its own name, `transition`, `retries`, `deadline`, `queue`, etc.
- `FanOutDefinition` is deleted. No more nested fan-out blocks.

### DSL Validation Rules

1. `fanOut` target must reference an existing activity name.
2. `fanOut` target must be the immediately next activity in the list.
3. `fanOut` target must not itself have a `fanOut` (no chained scatters).

## Model Changes

### ActivityDefinition

```kotlin
data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: String? = null,          // target activity name (was FanOutDefinition?)
    val joinPolicy: JoinPolicy = JoinPolicy.All,  // moved from FanOutDefinition
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
)
```

### PhaseType

```kotlin
enum class PhaseType { LINEAR, PARALLEL }
```

`SCATTER` is removed. A scatter activity is just `LINEAR` — its handler returns data, the engine fans it out.

### PhaseContext

```kotlin
data class PhaseContext(
    val workflow: WorkflowRun,
    val definition: WorkflowDefinition,
    val currentSeqInfo: SequenceInfo,
    val sequenceMap: Map<Int, SequenceInfo>,
    val failedCount: Int,
    val totalCount: Int,
)
```

`tasks: List<Task>` is removed. Strategies receive counts only.

### AdvancementDecision

```kotlin
sealed interface AdvancementDecision {
    data class Advance(val nextSequence: Int) : AdvancementDecision
    data object Complete : AdvancementDecision
    data class Abort(val reason: String) : AdvancementDecision
}
```

`tasks: List<Task>` is removed from `Advance`. Task creation is an execution concern handled by `executeDecision`.

### Sequence Map

Built from `WorkflowDefinition`. An activity that is the target of another activity's `fanOut` gets phase type `PARALLEL`. All others are `LINEAR`.

| Seq | Activity           | Phase Type |
|-----|--------------------|------------|
| 1   | read-configs       | LINEAR     |
| 2   | compute-dispatch   | PARALLEL   |
| 3   | join-to-parquet    | LINEAR     |

## Runtime Flow

### Barrier: evaluateAndAdvance

```kotlin
private fun evaluateAndAdvance(handle: Handle, workflowId: String, sequenceNumber: Int) {
    val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
        ?: throw IllegalStateException("Workflow not found: $workflowId")
    if (workflow.status != WorkflowStatus.RUNNING) return
    if (sequenceNumber != workflow.currentSequence) return

    val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
    val totalCount = taskRepo.countAllWithHandle(handle, workflowId, sequenceNumber)

    resolveAndExecute(handle, workflow, sequenceNumber, failedCount, totalCount)
}
```

No task list loaded. Two COUNT queries only.

### Strategies

**`ScatterPhaseStrategy` is deleted.** Scatter is a LINEAR activity.

**`LinearPhaseStrategy`**: counts in, decision out. Returns `Advance(nextSeq)` or `Complete` or `Abort`.

**`ParallelPhaseStrategy`**: counts in, decision out. Checks failure policy against `failedCount`, returns `Advance(nextSeq)` or `Abort`.

### executeDecision

Handles task creation based on target phase type:

```kotlin
is AdvancementDecision.Advance -> {
    val casWon = workflowRepo.casAdvanceWithHandle(...)
    if (!casWon) return

    val nextSeqInfo = sequenceMap[decision.nextSequence]!!
    when (nextSeqInfo.phaseType) {
        PARALLEL -> taskRepo.insertFanOutFromScatter(handle, workflowId, currentSeq, nextSeqInfo)
        LINEAR   -> taskRepo.insertTask(handle, createTaskForActivity(workflowId, nextSeqInfo))
    }
}
```

- **LINEAR target**: create 1 task from the activity definition.
- **PARALLEL target**: SQL INSERT...SELECT from the scatter result.

### Fan-Out Task Creation (SQL)

```sql
INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, item,
                  retry_count, max_retries, deadline_at, backoff_base, backoff_cap, queue_name)
SELECT SYS_GUID(), :workflowId, :nextSeq, 'PENDING', :handlerKey,
       jt.item,
       0, :maxRetries, :deadlineAt, :backoffBase, :backoffCap, :queueName
FROM task t
CROSS JOIN JSON_TABLE(t.result_json, '$[*]' COLUMNS (item VARCHAR2(4000) PATH '$')) jt
WHERE t.workflow_id = :workflowId
  AND t.sequence_number = :currentSeq
  AND t.status = 'COMPLETED'
```

Zero memory overhead. DB reads scatter result and creates tasks in one operation.

### InputResolver

Determines single vs aggregate by the referenced activity's phase type:

- **LINEAR** (including scatter source): fetch 1 completed task, traverse `fieldPath` through its `resultJson`.
- **PARALLEL** (fan-out target): fetch N completed tasks, aggregate `fieldPath` values into a JSON array.

```kotlin
private suspend fun resolveActivity(
    activityName: String,
    fieldPath: List<String>,
    sequenceMap: Map<Int, SequenceInfo>,
    tasksBySequence: suspend (Int) -> List<Task>,
): JsonNode {
    val seqInfo = sequenceMap.values.first { it.activity.name == activityName }

    return when (seqInfo.phaseType) {
        PARALLEL -> {
            val tasks = tasksBySequence(seqInfo.sequenceNumber)
                .filter { it.status == TaskStatus.COMPLETED }
            aggregateFanOut(tasks, fieldPath)
        }
        LINEAR -> {
            val task = tasksBySequence(seqInfo.sequenceNumber)
                .firstOrNull { it.status == TaskStatus.COMPLETED }
            val resultTree = objectMapper.readTree(task?.resultJson ?: return objectMapper.nullNode())
            traversePath(resultTree, fieldPath)
        }
    }
}
```

No syntax changes for the developer. `"x" from "activity.field"` works the same.

**Note:** The PARALLEL aggregation path still loads completed tasks into memory. This is acceptable because it only happens at worker execution time (not barrier time) and only when a join handler explicitly declares aggregation via `inputs`. For most use cases (like the dispatch workflow), the join handler references the scatter activity, not the fan-out — so no bulk loading occurs. SQL-level aggregation for this path can be added later if needed.

### WorkerLoop / HandlerInput

No changes. `HandlerInput.item` still carries the scatter item for each parallel task. `inputs` are still resolved at execution time by `InputResolver`.

## What's Deleted

- `FanOutDefinition` class
- `ScatterPhaseStrategy`
- `PhaseType.SCATTER`
- `tasks: List<Task>` from `PhaseContext`
- `tasks: List<Task>` from `AdvancementDecision.Advance`
- `findByWorkflowAndSequenceWithHandle` call in `evaluateAndAdvance`

## What's Added

- `taskRepo.insertFanOutFromScatter` — SQL INSERT...SELECT with JSON_TABLE
- DSL validation rules for `fanOut` references
- Phase type derivation: fan-out targets become `PARALLEL`

## What's Changed

- `ActivityDefinition.fanOut`: `FanOutDefinition?` to `String?` (target activity name)
- `evaluateAndAdvance`: COUNT queries only, no task list
- `executeDecision`: routes task creation by target phase type
- `buildSequenceMap`: derives PARALLEL from fan-out target relationship
- `InputResolver.resolveActivity`: single vs aggregate based on `PhaseType`
- DSL builders: `fanOut("target")` replaces nested `fanOut { }` block

## What's Unchanged

- `WorkerLoop` / `HandlerInput` — `item` field works the same
- `ParallelPhaseStrategy` — still counts-based
- `LinearPhaseStrategy` — still advances
- Task table schema — no column changes
