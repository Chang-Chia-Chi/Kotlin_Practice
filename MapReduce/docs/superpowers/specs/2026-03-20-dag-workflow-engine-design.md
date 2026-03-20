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
| Deadlines | Per-step (on `StepSpec`) with config default | No step should run forever |
| Step handlers | Developer-implemented `TaskHandler` beans | Framework can't auto-generate for arbitrary step logic |
| Step storage | One `workflow_step` row per step (INSERT, not in-place update) | Audit trail; each step's final state preserved |
| No `job` table | Job-level status derived from step rows | Simpler schema; job is just a correlated set of steps |
| Package rename | `com.mapreduce.mr` → `com.mapreduce.workflow` | Reflects generalization |
| Table rename | `task_group` → `workflow_step` | Matches new model |
| FK rename | `task.group_id` → `task.step_id` | References step row |
| Naming | `workflow_name` (not type), `run_id` (not job_id) | `workflow_name` is an identifier; `run_id` is a correlation token |

## Schema: `workflow_step` + `task`

**Target schema** (reference only — see Migration section below for actual DDL):

```sql
-- workflow_step (renamed from task_group, one row per step)
CREATE TABLE workflow_step (
    step_id            VARCHAR2(36)   PRIMARY KEY,
    workflow_name      VARCHAR2(255)  NOT NULL,  -- definition identifier
    run_id             VARCHAR2(36)   NOT NULL,  -- correlation token (UUID)
    step_label         VARCHAR2(50)   NOT NULL,  -- step name from pipeline()
    status             VARCHAR2(20)   NOT NULL,  -- ACTIVE, COMPLETED, FAILED
    params             CLOB,                     -- per-step context
    queue              VARCHAR2(100)  DEFAULT 'default', -- step's task queue
    step_total         NUMBER(10)     DEFAULT 0, -- was phase_total
    tasks_pending      NUMBER(10)     DEFAULT 0,
    tasks_failed       NUMBER(10)     DEFAULT 0,
    on_complete_handler VARCHAR2(255),
    failure_policy     VARCHAR2(20)   DEFAULT 'FAIL_STEP',
    failure_threshold  NUMBER(5,4)    DEFAULT 0,
    result_metadata    CLOB,
    version            NUMBER(19)     DEFAULT 0,
    last_epoch         NUMBER(19)     DEFAULT 0,
    deadline_at        TIMESTAMP,                -- per-step, from StepSpec.deadline
    created_at         TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (workflow_name, run_id, step_label)
);

-- task.step_id references workflow_step.step_id
-- (renamed from task.group_id)
```

### Migration (`V14__workflow_step.sql`)

The Flyway migration uses ALTER/RENAME, not CREATE+INSERT, to preserve existing data:

```sql
-- Table rename
ALTER TABLE task_group RENAME TO workflow_step;

-- Column renames on workflow_step
ALTER TABLE workflow_step RENAME COLUMN group_id TO step_id;
ALTER TABLE workflow_step RENAME COLUMN group_type TO workflow_name;
ALTER TABLE workflow_step RENAME COLUMN phase TO step_label;
ALTER TABLE workflow_step RENAME COLUMN phase_total TO step_total;

-- Add new columns
ALTER TABLE workflow_step ADD run_id VARCHAR2(36);
ALTER TABLE workflow_step ADD queue VARCHAR2(100) DEFAULT 'default';
UPDATE workflow_step SET run_id = step_id WHERE run_id IS NULL;
ALTER TABLE workflow_step MODIFY run_id NOT NULL;
ALTER TABLE workflow_step ADD CONSTRAINT uq_wf_step UNIQUE (workflow_name, run_id, step_label);

-- FK rename on task table
ALTER TABLE task RENAME COLUMN group_id TO step_id;

-- Rebuild indexes referencing old column names
DROP INDEX idx_task_group;
CREATE INDEX idx_task_step ON task (step_id, status);
DROP INDEX idx_task_group_handler;
CREATE INDEX idx_task_step_handler ON task (step_id, handler);

-- Index for job-level queries
CREATE INDEX idx_wf_step_run ON workflow_step (run_id);

-- Rename failure_policy enum value
UPDATE workflow_step SET failure_policy = 'FAIL_STEP' WHERE failure_policy = 'FAIL_GROUP';
```

Existing ACTIVE steps continue working since the barrier/counter logic is unchanged.

**No `job` table.** Job-level queries use `run_id`:
- `GET /api/jobs/{runId}` → `SELECT * FROM workflow_step WHERE run_id = :runId ORDER BY created_at`
- `GET /api/jobs?status=ACTIVE` → `SELECT DISTINCT run_id FROM workflow_step WHERE status = 'ACTIVE'`

## Interface: `WorkflowDefinition<P>`

Replaces `MapReduceDefinition<P, I, O, R>`. The four type parameters collapse to one (`P` = submission params). `P` is typed only at submission; subsequent steps work with raw strings (each step interprets its own params).

`WorkflowDefinition.workflowName` replaces `MapReduceDefinition.jobType`. The registry looks up definitions by matching `step.workflowName == definition.workflowName`.

```kotlin
interface WorkflowDefinition<P> {
    val workflowName: String

    fun serializeParams(params: P): String
    fun deserializeParams(json: String): P

    /** Steps in execution order. */
    fun pipeline(): List<StepSpec>

    /** Produce tasks for step 0 (called at submit time). */
    suspend fun initialTasks(params: P): List<TaskPayload>

    /**
     * Produce tasks for step N when step N-1 completes.
     * Called for steps at index 1..last.
     */
    suspend fun transitionTasks(
        stepIndex: Int,
        previousStepParams: String,
        previousOutputs: Flow<TaskOutput>,
    ): StepTransition

    /** Called when the final step completes. */
    suspend fun onCompleted(lastStepParams: String, finalOutputs: Flow<TaskOutput>)

    data class StepSpec(
        val name: String,
        /** TaskHandler bean name registered in HandlerRegistry (NOT the callback handler). */
        val handler: String,
        val queue: String = "default",
        val maxRetries: Int = 3,
        val failurePolicy: FailurePolicy = FailurePolicy.FAIL_STEP,
        val failureThreshold: Double = 0.0,
        val deadline: Duration = Duration.ofHours(1),
    )

    data class TaskPayload(
        val payload: String,
        val metadata: String? = null,
    )

    data class StepTransition(
        val tasks: List<TaskPayload>,
        val stepParams: String? = null,
    )
}
```

**Developer-written step handlers** are standard CDI beans implementing `TaskHandler`. They inject dependencies (e.g., `BlobStore`) via constructor injection, same as any Quarkus bean. The framework does not auto-generate or wrap them.

## Step Transition Mechanism

`StepTransitionHandler` replaces `PhaseTransitionHandler`. One generic handler per workflow type, registered as `"{workflowName}.__step_transition"`.

**Flow when a step's barrier fires:**

1. Callback task created atomically by `resolveStepCounter` (unchanged in logic; SQL strings updated for table/column renames — see Rename table). Callback payload = `step_id`.
2. `StepTransitionHandler` picks up the callback task (payload = `step_id`).
3. Fetches the `WorkflowStep` row via `findStep(ctx.payload)` to get `workflowName`, `stepLabel`, `params`.
4. Looks up `WorkflowDefinition` by `step.workflowName` from `WorkflowRegistry`.
5. Finds current step index by matching `step.stepLabel` against `pipeline()`.
6. Evaluates the **current step's** failure policy (from `StepSpec`, not from the step row). The `failure_policy` and `failure_threshold` columns on `workflow_step` are written at step creation for observability (admin dashboards, debugging); the `StepTransitionHandler` reads from the in-memory `StepSpec` to avoid stale-row issues.
7. If policy violated: CAS the step row to FAILED.
8. If this is the last step: call `onCompleted(step.params, finalOutputs)`, CAS to COMPLETED.
9. If more steps remain:
   a. Build `previousOutputs` by calling `streamTaskOutputs(stepId, currentStep.handler)` — the current step's `StepSpec.handler` is the handler name filter.
   b. Call `transitionTasks(nextIndex, step.params, previousOutputs)` to get the next step's task payloads and optional step params.
   c. **INSERT** a new `workflow_step` row for the next step (new `step_id`, same `run_id`/`workflow_name`, next `step_label`, `params = stepTransition.stepParams`, `queue = nextStepSpec.queue`, `deadline_at = now + nextStepSpec.deadline`).
   d. CAS the current step row to COMPLETED.

**Step transition is INSERT, not in-place update.** `transitionPhase()` is replaced by `createNextStep()`:
- INSERT new `workflow_step` row with fresh counters (`tasks_pending = N, tasks_failed = 0`)
- INSERT new tasks referencing the new `step_id`
- CAS the previous step row to COMPLETED
- All in one transaction; version/epoch fencing on the CAS prevents duplicate transitions

**`step_label`** stores the step name from `pipeline()` and is used for both display and routing. Step names must be unique within a pipeline (validated at startup).

**Output resolution**: `streamTaskOutputs(stepId, handler)` filters completed tasks by step_id and handler name. The `handler` parameter is the current step's `StepSpec.handler` — this naturally selects only the outputs from the step that just completed.

## Default Deadline

New config property in `FrameworkConfig`:

```kotlin
interface WorkflowConfig {
    @WithName("default-step-deadline")
    @WithDefault("1H")
    fun defaultStepDeadline(): Duration
}
```

`StepSpec.deadline` has a hardcoded default of `Duration.ofHours(1)`. The config value provides a project-wide override: at submission time, if `stepSpec.deadline` equals `Duration.ofHours(1)` (the hardcoded default), the framework uses `config.workflow().defaultStepDeadline()` instead, allowing operators to change the default without code changes. Each step row gets `deadline_at = Instant.now() + resolvedDeadline`. `failExpiredSteps` checks the ACTIVE step row's `deadline_at` — same logic as today.

## File Impact

All paths relative to `src/main/kotlin/com/mapreduce/` (production) or `src/test/kotlin/com/mapreduce/` (test). **Paths in Delete use current (`mr/`) names. Paths in Modify/Create use post-rename (`workflow/`, `WorkflowStep`, etc.) names.**

**Scope note**: The table rename (`task_group` → `workflow_step`) and FK rename (`group_id` → `step_id`) cascades to every file that references these names in SQL or Kotlin. The class renames (`TaskGroup` → `WorkflowStep`, `TaskGroupRepository` → `WorkflowStepRepository`) cascade to every file that imports them. All `SELECT * FROM workflow_step` queries that map to the data class are implicitly affected by column renames through `@ColumnName` annotations on `WorkflowStep.kt`.

**File rename convention**: File names on disk follow the class rename — `TaskGroupRepository.kt` becomes `WorkflowStepRepository.kt`, etc. Files listed in the Modify table with `workflow/` paths are also subject to the `mr/` → `workflow/` package move; they appear only in Modify (not also in Move) to avoid duplication.

### Package rename: `mr/` → `workflow/`

Every file under `mr/` either moves to `workflow/` or is deleted. Files in "Delete" are removed; all others move with package/import updates.

**Files that move (content unchanged besides package/imports):**

| From (`mr/`) | To (`workflow/`) |
|--------------|------------------|
| `mr/model/FailurePolicy.kt` (includes `evaluateFailurePolicy` function; rename `FAIL_GROUP` → `FAIL_STEP`) | `workflow/model/FailurePolicy.kt` |
| `mr/shuffle/BlobStore.kt` | `workflow/shuffle/BlobStore.kt` |
| `mr/shuffle/LocalBlobStore.kt` | `workflow/shuffle/LocalBlobStore.kt` |
| `mr/api/dto/SubmitJobRequest.kt` | `workflow/api/dto/SubmitJobRequest.kt` |
| `mr/model/JobModelTest.kt` (test) | `workflow/model/JobModelTest.kt` |
| `mr/shuffle/LocalBlobStoreTest.kt` (test) | `workflow/shuffle/LocalBlobStoreTest.kt` |
| `mr/repository/JobRepositoryTest.kt` (test) | `workflow/repository/JobRepositoryTest.kt` |

### Create

| New file | Purpose |
|----------|---------|
| `workflow/spi/WorkflowDefinition.kt` | New pipeline definition interface |
| `workflow/handler/StepTransitionHandler.kt` | Generic step barrier callback handler |
| `workflow/registry/WorkflowRegistry.kt` | Discovers `WorkflowDefinition` beans, registers `StepTransitionHandler` per type, validates handler/step names at startup. Exposes `getDefinition(workflowName: String): WorkflowDefinition<*>?` for `StepTransitionHandler` |
| `resources/db/migration/V14__workflow_step.sql` | Table rename, column renames, FK rename (see Schema section) |
| `workflow/handler/StepTransitionHandlerTest.kt` (test) | Step transition, per-step failure policy, multi-step pipeline |
| `workflow/registry/WorkflowRegistryTest.kt` (test) | Bean discovery, handler registration, startup validation |

### Delete

| Deleted file | Reason |
|--------------|--------|
| `mr/spi/MapReduceDefinition.kt` | Replaced by `WorkflowDefinition` |
| `mr/handler/PhaseTransitionHandler.kt` | Replaced by `StepTransitionHandler` |
| `mr/handler/MapTaskHandler.kt` | No auto-generated step handlers |
| `mr/handler/ReduceTaskHandler.kt` | Same |
| `mr/registry/MapReduceRegistry.kt` | Replaced by `WorkflowRegistry` |
| `mr/handler/PhaseTransitionHandlerTest.kt` (test) | Handler deleted |
| `mr/handler/MapTaskHandlerTest.kt` (test) | Handler deleted |
| `mr/handler/ReduceTaskHandlerTest.kt` (test) | Handler deleted |
| `mr/registry/MapReduceRegistryTest.kt` (test) | Registry deleted |

### Rename (class + table renames, cascading)

These renames affect every file that imports or references the old names:

| Old | New | Scope |
|-----|-----|-------|
| `TaskGroup` (data class) | `WorkflowStep` | Every file constructing/referencing the model |
| `TaskGroupRepository` | `WorkflowStepRepository` | Every file injecting/calling the repository |
| `GroupStatus` | `StepStatus` | Every file using the enum |
| `GroupTaskResolution` | `StepTaskResolution` | `WorkflowStepRepository`, `TaskDispatcher`, tests |
| `GroupFailResult` | `StepFailResult` | `WorkflowStepRepository`, `TaskDispatcher`, tests |
| SQL: `task_group` | `workflow_step` | Every SQL string in `WorkflowStepRepository`, including `resolveStepCounter` |
| SQL: `group_id` (in task table) | `step_id` | Every SQL string referencing `task.group_id`, including `TaskRepository` |
| SQL: `group_type` | `workflow_name` | `submitStep` INSERT, queries |
| SQL: `phase` | `step_label` | `submitStep`, `createNextStep` |
| SQL: `phase_total` | `step_total` | All SQL referencing this column |
| Kotlin: `groupId` field | `stepId` | `WorkflowStep`, `EnqueueRequest`, `TaskContext`, `Task` |
| Kotlin: `groupType` field | `workflowName` | `WorkflowStep` |
| Kotlin: `phaseTotal` field | `stepTotal` | `WorkflowStep` |
| Method: `submitGroup` | `submitStep` | `WorkflowStepRepository` |
| Method: `findGroup` | `findStep` | `WorkflowStepRepository` |
| Method: `transitionPhase` | `createNextStep` | `WorkflowStepRepository` |
| Method: `casGroupStatus` | `casStepStatus` | `WorkflowStepRepository` |
| Method: `failExpiredGroups` | `failExpiredSteps` | `WorkflowStepRepository`, `StaleTaskReaper` |
| Method: `resolveStepCounter` (private) | `resolveStepCounter` | `WorkflowStepRepository` |
| Method: `countByGroupAndStatus` | `countByStepAndStatus` | `TaskRepository` |
| Method: `findByGroupAndHandler` | `findByStepAndHandler` | `TaskRepository` |
| Method: `findAllByGroupAndHandler` | `findAllByStepAndHandler` | `TaskRepository` |
| Method: `findCompletedByGroupAndHandler` | `findCompletedByStepAndHandler` | `TaskRepository` |
| Method: `findClaimedByGroupAndHandler` | `findClaimedByStepAndHandler` | `TaskRepository` |
| Param: `groupId` in `TaskRepository` methods | `stepId` | All method signatures in `TaskRepository` |
| Enum: `FailurePolicy.FAIL_GROUP` | `FailurePolicy.FAIL_STEP` | `FailurePolicy.kt`, `StepSpec`, migration |

### Modify (production — content changes beyond renames)

| File | Changes beyond renames |
|------|------------------------|
| `queue/repository/WorkflowStepRepository.kt` | `createNextStep`: INSERT new row instead of CAS-update-in-place. New `run_id`, `workflow_name` columns in all SQL. `submitStep` sets `deadline_at = now + stepSpec.deadline`. `resolveStepCounter`: logic unchanged but all SQL strings must be updated for table/column renames (`task_group` → `workflow_step`, `group_id` → `step_id`, `phase_total` → `step_total`). |
| `queue/repository/TaskRepository.kt` | `group_id` → `step_id` in all INSERT and SELECT SQL strings. |
| `queue/model/WorkflowStep.kt` | Add `runId`, `workflowName` fields. Add `queue` field. Remove `phase` (now `stepLabel`). Rename `phaseTotal` → `stepTotal`. Add `@ColumnName` annotations for new column names. |
| `queue/model/Task.kt` | `@ColumnName("group_id") val groupId` → `@ColumnName("step_id") val stepId` |
| `queue/model/EnqueueRequest.kt` | `groupId` → `stepId` |
| `queue/model/TaskContext.kt` | `groupId` → `stepId` |
| `queue/worker/TaskDispatcher.kt` | `task.groupId` → `task.stepId` in all result routing |
| `queue/reaper/StaleTaskReaper.kt` | `failExpiredGroups` → `failExpiredSteps` |
| `workflow/api/JobResource.kt` | Use `WorkflowRegistry`. Call `initialTasks(params)`. Build `EnqueueRequest` with `handler = pipeline()[0].handler`, `queue = pipeline()[0].queue`, `maxRetries = pipeline()[0].maxRetries`. Set step fields from `pipeline()[0]`. Generate `run_id = UUID`, `step_id = UUID`. Set `onCompleteHandler = "${workflowName}.__step_transition"`. `initialTasks` is now `suspend`; remove `withContext(Dispatchers.IO)` wrapper (JDBI suspend extensions handle dispatching). |
| `workflow/api/dto/JobResponse.kt` | `groupId` → `runId`, `groupType` → `workflowName`, `phase` → `stepLabel`, `phaseTotal` → `stepTotal`; update `from()` mapper for all renamed fields. May return list of step statuses for job-level view. |
| `queue/pipeline/TracingMiddleware.kt` | `context.groupId` → `context.stepId`, span attribute `task.groupId` → `task.stepId` |
| `queue/registry/HandlerRegistry.kt` | Update doc comment reference from "MapReduce" to "workflow definitions" |
| `config/FrameworkConfig.kt` | Add `workflow(): WorkflowConfig` with `defaultStepDeadline` |

### Modify (tests — content changes beyond renames)

| File | Changes beyond renames |
|------|------------------------|
| `TestH2Factory.kt` | Full schema rewrite: `task_group` → `workflow_step` with new columns (`step_id`, `workflow_name`, `run_id`, `step_label`, `step_total`, `queue`). `task.group_id` → `task.step_id`. |
| `workflow/repository/JobRepositoryTest.kt` | Construct `WorkflowStep` with new fields (`runId`, `workflowName`, `stepLabel`). Update all assertions. |
| `workflow/api/JobResourceTest.kt` | Mock `WorkflowDefinition`/`WorkflowRegistry`. Construct `WorkflowStep` with new fields. |
| `queue/repository/WorkflowStepRepositoryTest.kt` | Full update: new field names, new SQL assertions. Replace `transitionPhase` tests with `createNextStep` INSERT behavior tests (verify old step COMPLETED, new step ACTIVE with fresh counters). |
| `queue/repository/TaskRepositoryTest.kt` | Update raw SQL helper `insertTask()` to use `step_id`. Update `EnqueueRequest` construction: `groupId` → `stepId`. |
| `queue/ConcurrencyStressTest.kt` | Construct `WorkflowStep` with new fields. Update callback handler names. |
| `queue/worker/TaskDispatcherTest.kt` | `groupId` → `stepId` in test task construction |
| `queue/reaper/StaleTaskReaperTest.kt` | `failExpiredGroups` → `failExpiredSteps` |
| `queue/pipeline/TracingMiddlewareTest.kt` | `groupId` → `stepId` in test context construction and span attribute assertions |

### Unchanged

| File | Why |
|------|-----|
| `leader/LeaderManager.kt` + test | No queue/group references |
| `leader/NotLeader.kt` + test | Same |
| `shutdown/ShutdownState.kt` | No queue references |
| `queue/pipeline/TaskPipeline.kt` + test | No group/step references |
| `queue/worker/WorkerLoop.kt` + test | Same |
| `queue/pipeline/MetricsMiddleware.kt` + test | No group/step references |
| `queue/pipeline/ErrorClassifierMiddleware.kt` + test | Same |
| `queue/pipeline/TimeoutMiddleware.kt` + test | Same |
| `queue/registry/HandlerRegistryTest.kt` | No group/step references |
| All existing Flyway migrations (`V1` through `V13`) | Migrations are append-only; never modified |

## What Is NOT Changing

- **Countdown barrier** (`resolveStepCounter`): atomic decrement + callback creation in one transaction. Logic unchanged; SQL strings updated for renames.
- **Zombie fencing**: `execution_generation` guards on all task updates.
- **Leader fencing**: `version` CAS + `last_epoch` on writes.
- **Task claiming**: `SELECT FOR UPDATE SKIP LOCKED` unchanged.
- **Stale task reaping**: same logic, renamed methods.
- **`TaskPipeline`, `WorkerLoop`**: unchanged.
