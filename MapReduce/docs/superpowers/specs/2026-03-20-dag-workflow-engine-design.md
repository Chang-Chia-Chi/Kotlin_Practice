# DAG Workflow Engine Refactoring

**Date:** 2026-03-20
**Status:** Approved

## Goal

Generalize the MapReduce task queue from a hardcoded two-phase (map/reduce) engine into a linear-pipeline workflow engine where step chaining is driven by `on_complete_handler` callbacks. Retain the countdown barrier, version-based CAS, and epoch fencing — these are correctness mechanisms, not targets for removal.

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Barrier detection | Keep countdown barrier in workers | Atomic, zero-latency, no single point of failure |
| Fencing columns | Keep `version`, `last_epoch` | Prevents ABA and zombie-leader writes |
| Counter columns | Keep `tasks_pending`, `tasks_failed` | O(1) barrier check vs O(N) COUNT query |
| Definition interface | `WorkflowDefinition<P>` (code-only, no YAML) | One type param; per-step logic in handler beans |
| Pipeline shape | Linear (ordered list of steps) | Covers map-reduce and N-step ETL; fork/join deferred |
| Failure policies | Per-step (on `StepSpec`) | Different tolerance per pipeline stage |
| Step handlers | Developer-implemented `TaskHandler` beans | Framework can't auto-generate for arbitrary step logic |
| Package rename | `mr` -> `workflow` | Reflects generalization from MapReduce |

## Interface: `WorkflowDefinition<P>`

Replaces `MapReduceDefinition<P, I, O, R>`. The four type parameters collapse to one (`P` = job params) because each step handler manages its own I/O serialization.

```kotlin
interface WorkflowDefinition<P> {
    val workflowType: String

    fun serializeParams(params: P): String
    fun deserializeParams(json: String): P

    /** Steps in execution order. */
    fun pipeline(): List<StepSpec>

    /** Produce tasks for step 0 (called at submit time). */
    fun initialTasks(params: P): List<TaskPayload>

    /**
     * Produce tasks for step N when step N-1 completes.
     * Called for steps at index 1..last.
     */
    suspend fun transitionTasks(
        stepIndex: Int,
        params: P,
        previousOutputs: Flow<TaskOutput>,
    ): List<TaskPayload>

    /** Called when the final step completes. */
    suspend fun onCompleted(params: P, finalOutputs: Flow<TaskOutput>)

    data class StepSpec(
        val name: String,
        val handler: String,
        val queue: String = "default",
        val maxRetries: Int = 3,
        val failurePolicy: FailurePolicy = FailurePolicy.FAIL_GROUP,
        val failureThreshold: Double = 0.0,
    )

    data class TaskPayload(
        val payload: String,
        val metadata: String? = null,
    )
}
```

## Step Transition Mechanism

`StepTransitionHandler` replaces `PhaseTransitionHandler`. One generic handler per workflow type, registered as `"{workflowType}.__step_transition"`.

**Flow when a step's barrier fires:**

1. Callback task created atomically by `resolveGroupCounter` (unchanged).
2. `StepTransitionHandler` picks up the callback task.
3. Looks up `WorkflowDefinition` by `group.groupType` from `WorkflowRegistry`.
4. Finds current step index by matching `group.stepLabel` against `pipeline()`.
5. Evaluates the **current step's** failure policy (from `StepSpec`, not from group row).
6. If policy violated: `casGroupStatus(FAILED)`.
7. If this is the last step: call `onCompleted()`, then `casGroupStatus(COMPLETED)`.
8. If more steps remain: call `transitionTasks(nextIndex, ...)`, then `transitionPhase()` with the next step's config.

**`step_label`** stores the step name from `pipeline()` and is used for both display and routing. Step names must be unique within a pipeline (validated at startup).

**`transitionPhase()`** already does the right thing: CAS on version, reset `tasks_pending`/`tasks_failed`, set new step label, insert new tasks. It is extended to also update `failure_policy` and `failure_threshold` per step.

## Schema Migration

```sql
-- V14__workflow_step_label.sql
ALTER TABLE task_group RENAME COLUMN phase TO step_label;
```

All other columns (`tasks_pending`, `version`, `last_epoch`, `failure_policy`, `failure_threshold`, etc.) are retained.

## File Impact

### Create (production)

| New file | Purpose |
|----------|---------|
| `workflow/spi/WorkflowDefinition.kt` | New pipeline definition interface |
| `workflow/handler/StepTransitionHandler.kt` | Generic step barrier callback handler |
| `workflow/registry/WorkflowRegistry.kt` | Discovers `WorkflowDefinition` beans, registers `StepTransitionHandler` per type, validates handler names at startup |
| `resources/db/migration/V14__workflow_step_label.sql` | Column rename migration |

### Create (tests)

| New file | Purpose |
|----------|---------|
| `workflow/handler/StepTransitionHandlerTest.kt` | Step transition logic, per-step failure policy evaluation |
| `workflow/registry/WorkflowRegistryTest.kt` | Bean discovery, handler registration, startup validation |

### Delete (production + tests)

| Deleted file | Reason |
|--------------|--------|
| `mr/spi/MapReduceDefinition.kt` | Replaced by `WorkflowDefinition` |
| `mr/handler/PhaseTransitionHandler.kt` | Replaced by `StepTransitionHandler` |
| `mr/handler/MapTaskHandler.kt` | No auto-generated step handlers in new design |
| `mr/handler/ReduceTaskHandler.kt` | Same |
| `mr/registry/MapReduceRegistry.kt` | Replaced by `WorkflowRegistry` |
| `mr/handler/PhaseTransitionHandlerTest.kt` | Handler deleted |
| `mr/handler/MapTaskHandlerTest.kt` | Handler deleted |
| `mr/handler/ReduceTaskHandlerTest.kt` | Handler deleted |
| `mr/registry/MapReduceRegistryTest.kt` | Registry deleted |

### Modify (production)

| File | Changes |
|------|---------|
| `queue/model/TaskGroup.kt` | `phase` -> `stepLabel` with `@ColumnName("step_label")` |
| `queue/repository/TaskGroupRepository.kt` | SQL: `phase` -> `step_label`; param `newPhase` -> `newStepLabel`; `transitionPhase` also updates `failure_policy` + `failure_threshold` |
| `workflow/api/JobResource.kt` | Use `WorkflowRegistry`; call `initialTasks()`; `stepLabel = pipeline()[0].name`; `onCompleteHandler = "${workflowType}.__step_transition"`; per-step failure policy from `pipeline()[0]` |
| `workflow/api/dto/JobResponse.kt` | `phase` -> `stepLabel`; update `from()` mapper |

### Modify (tests)

| File | Changes |
|------|---------|
| `TestH2Factory.kt` | Schema: `phase VARCHAR(50)` -> `step_label VARCHAR(50)` |
| `workflow/repository/JobRepositoryTest.kt` | All `phase =` -> `stepLabel =`; `__phase_complete` -> `__step_transition` |
| `workflow/api/JobResourceTest.kt` | Mock `WorkflowDefinition`/`WorkflowRegistry`; `phase =` -> `stepLabel =` |
| `queue/repository/TaskGroupRepositoryTest.kt` | All `phase =` -> `stepLabel =`; `__phase_complete` -> `__step_transition` |
| `queue/ConcurrencyStressTest.kt` | All `phase = "map"` -> `stepLabel = "stress-step"`; `__phase_complete` -> `__step_transition` |

### Unchanged

| File | Why |
|------|-----|
| `queue/worker/TaskDispatcher.kt` + test | No phase or definition references |
| `queue/reaper/StaleTaskReaper.kt` + test | No phase references |
| `leader/LeaderManager.kt`, `leader/NotLeader.kt` + test | Unchanged |
| `config/FrameworkConfig.kt` | Unchanged |
| `shutdown/ShutdownState.kt` | Unchanged |
| `workflow/shuffle/BlobStore.kt` | Still needed for any step using shuffle storage |
| `workflow/model/FailurePolicy.kt` | `evaluateFailurePolicy` reused by `StepTransitionHandler` |
| `V11__task_groups.sql`, `V12__countdown_barrier.sql` | Existing migrations never modified |

## What Is NOT Changing

- **Countdown barrier** (`resolveGroupCounter`): atomic decrement + callback creation in one transaction.
- **Zombie fencing**: `execution_generation` guards on all task updates.
- **Leader fencing**: `version` CAS + `last_epoch` on group writes.
- **Task claiming**: `SELECT FOR UPDATE SKIP LOCKED` unchanged.
- **Stale task reaping**: `StaleTaskReaper` + `failExpiredGroups` unchanged.
- **Layer 1 queue infrastructure**: `TaskRepository`, `TaskDispatcher`, `TaskPipeline`, `WorkerLoop` unchanged.
