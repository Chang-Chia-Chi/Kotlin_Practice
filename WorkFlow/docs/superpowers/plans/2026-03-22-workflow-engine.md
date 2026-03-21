# Lock-Free Workflow Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a lock-free workflow engine with DAG progression via optimistic CAS, a declarative DSL, and a leader sweeper backup path.

**Architecture:** The engine eliminates per-task row-lock contention by deriving activity completion from MVCC aggregate queries and advancing via optimistic CAS. Two paths guarantee progress: workers (primary) and a leader sweeper (backup). A Kotlin DSL produces immutable `WorkflowDefinition` data that the engine persists and replays.

**Tech Stack:** Kotlin 2.2.0, Quarkus 3.17.5, JDBI 3.45.0 (suspend extensions), Oracle, Flyway, Micrometer, Kubernetes leader election

---

## Existing Infrastructure

Already implemented in `src/main/kotlin/`:
- `extension/JdbiExtension.kt` — `Jdbi.withHandleSuspend()`, `inTransactionSuspend()`, etc.
- `extension/FlowExtension.kt` — `indefinitelyRepeat()`, `unorderedMapAsync()`, `takeUntilSignal()`
- `leader/LeaderManager.kt` — K8s Lease-based leader election with fencing epoch
- `leader/NotLeader.kt` — Scheduler skip predicate
- `shutdown/` — `ShutdownCoordinator`, `ShutdownParticipant`, `ShutdownSignal`, `ShutdownState`

**Not yet created:** `FrameworkConfig` (referenced by leader/shutdown), `application.yaml`, database schema, tests.

Package prefix: `com.mapreduce`

---

## File Structure

```
src/main/kotlin/
  config/
    FrameworkConfig.kt              -- Quarkus @ConfigMapping for all framework settings
  dsl/
    Models.kt                       -- FailurePolicy, JoinPolicy, WorkflowDefinition, ActivityDefinition, FanOutDefinition, JoinDefinition
    Builders.kt                     -- @DslMarker builders: workflow {}, activity {}, fanOut {}, join {}
  engine/
    Models.kt                       -- WorkflowStatus, ActivityStatus, TaskStatus, WorkflowRun, ActivityInstance, Task
    WorkflowRepository.kt           -- JDBI suspend DAO for workflow_run table
    ActivityRepository.kt           -- JDBI suspend DAO for activity_instance table
    TaskRepository.kt               -- JDBI suspend DAO for task table
    BarrierService.kt               -- Lock-free barrier: probe + evaluate + CAS + trigger downstream
    WorkflowEngine.kt               -- Public API: start workflow, initialize activities
    Sweeper.kt                      -- Leader sweeper: orphan detection + recovery
  worker/
    TransitionHandler.kt            -- Handler interface + CDI qualifier
    HandlerRegistry.kt              -- CDI-based handler lookup by dot-separated key
    WorkerLoop.kt                   -- Poll loop: claim via SKIP LOCKED, execute, report
  extension/                        -- (existing, unchanged)
  leader/                           -- (existing, unchanged)
  shutdown/                         -- (existing, unchanged)

src/main/resources/
  application.yaml                  -- Quarkus + datasource + framework config
  db/migration/
    V1__create_workflow_tables.sql   -- workflow_run, activity_instance, task tables + indexes

src/test/kotlin/
  dsl/
    ModelsTest.kt                   -- Serialization round-trip tests
    BuildersTest.kt                 -- DSL builder + validation tests
  engine/
    RepositoryTest.kt               -- Repository CRUD + CAS tests (H2)
    BarrierServiceTest.kt           -- Lock-free barrier unit tests (H2)
    WorkflowEngineTest.kt           -- Workflow start + progression tests
    SweeperTest.kt                  -- Orphan detection + recovery tests
    IntegrationTest.kt              -- End-to-end workflow lifecycle tests
  worker/
    HandlerRegistryTest.kt          -- CDI handler resolution tests
    WorkerLoopTest.kt               -- Claim + execute + report tests
```

---

## Task 1: DSL Data Models & Enums

**Files:**
- Create: `src/main/kotlin/dsl/Models.kt`
- Test: `src/test/kotlin/dsl/ModelsTest.kt`

Pure data layer. No dependencies on engine or JDBI. All types are immutable.

- [ ] **Step 1: Create enums and data classes**

`dsl/Models.kt` — package `com.mapreduce.dsl`:
- `FailurePolicy` enum: `ABORT`, `BEST_EFFORT`
- `JoinPolicy` sealed interface: `All` object, `Threshold(n: Int)`, `Percentage(pct: Int)` — validate n > 0, pct in 1..100
- `JoinDefinition` data class: `policy: JoinPolicy`, `transition: String?`
- `FanOutDefinition` data class: `transition: String`, `retries: Int`, `failurePolicy: FailurePolicy`, `deadline: Duration`, `join: JoinDefinition`
- `ActivityDefinition` data class: `name: String`, `transition: String`, `retries: Int`, `failurePolicy: FailurePolicy`, `deadline: Duration`, `fanOut: FanOutDefinition?`
- `WorkflowDefinition` data class: `activities: List<ActivityDefinition>` — validate non-empty

Default values: `retries = 0`, `failurePolicy = ABORT`, `deadline = Duration.ofMinutes(30)`, `fanOut = null`.

- [ ] **Step 2: Write serialization round-trip test**

`dsl/ModelsTest.kt` — verify Jackson serialization/deserialization of a `WorkflowDefinition` with fan-out produces identical output. Use `jacksonObjectMapper` from `jackson-module-kotlin`. Test both linear and fan-out definitions.

- [ ] **Step 3: Verify tests pass, commit**

Run: `mvn test -pl WorkFlow -Dtest="dsl.ModelsTest"`

---

## Task 2: DSL Builder

**Files:**
- Create: `src/main/kotlin/dsl/Builders.kt`
- Test: `src/test/kotlin/dsl/BuildersTest.kt`

Kotlin DSL with `@DslMarker` to prevent scope leakage. Build-phase validation.

- [ ] **Step 1: Create DslMarker and builders**

`dsl/Builders.kt` — package `com.mapreduce.dsl`:
- `@DslMarker annotation class WorkflowDsl`
- `JoinBuilder`: `policy()`, `transition()` → builds `JoinDefinition`
- `FanOutBuilder`: `transition()`, `retries()`, `failurePolicy()`, `deadline()`, `join {}` → builds `FanOutDefinition`. Validate: transition required, join required.
- `ActivityBuilder`: `transition()`, `retries()`, `failurePolicy()`, `deadline()`, `fanOut {}` → builds `ActivityDefinition`. Validate: transition required.
- `WorkflowBuilder`: `activity("name") {}` → builds `WorkflowDefinition`. Validate: at least one activity.
- Top-level `fun workflow(block: WorkflowBuilder.() -> Unit): WorkflowDefinition`

- [ ] **Step 2: Write builder tests**

`dsl/BuildersTest.kt`:
- Happy path: linear workflow with 2 activities
- Happy path: fan-out with join policy PERCENTAGE(95) and join transition
- Happy path: pure barrier (join with no transition)
- Validation: missing activity transition → `IllegalArgumentException`
- Validation: fanOut without join → `IllegalArgumentException`
- Validation: empty workflow → `IllegalArgumentException`
- Scope leakage: verify `@DslMarker` prevents calling `activity {}` inside `fanOut {}`

- [ ] **Step 3: Verify tests pass, commit**

Run: `mvn test -pl WorkFlow -Dtest="dsl.BuildersTest"`

---

## Task 3: FrameworkConfig & application.yaml

**Files:**
- Create: `src/main/kotlin/config/FrameworkConfig.kt`
- Create: `src/main/resources/application.yaml`

Quarkus `@ConfigMapping` interface. Unblocks LeaderManager and ShutdownCoordinator compilation. **Must complete before Tasks 5-6** (datasource config required for schema/repository tests).

- [ ] **Step 1: Create FrameworkConfig interface**

`config/FrameworkConfig.kt` — package `com.mapreduce.config`:
- `@ConfigMapping(prefix = "framework")` interface with named nested interfaces matching existing usage:
  - `WorkerConfig`: `id(): String` (default: hostname), `pollInterval(): Duration`, `concurrency(): Int`
  - `LeaderElectionConfig`: `namespace(): String`, `leaseName(): String`, `leaseDuration(): Duration`, `renewDeadline(): Duration`, `retryPeriod(): Duration`
  - `ShutdownConfig`: `globalTimeout(): Duration`, `leaderTeardownTimeout(): Duration`
  - `SweeperConfig`: `interval(): Duration`, `gracePeriod(): Duration`
- Methods: `worker(): WorkerConfig`, `leaderElection(): LeaderElectionConfig`, `shutdown(): ShutdownConfig`, `sweeper(): SweeperConfig`

Use `@WithDefault` annotations for sensible defaults.

- [ ] **Step 2: Create application.yaml**

`src/main/resources/application.yaml`:
- Quarkus datasource config (Oracle placeholder)
- Flyway config
- Framework config section with defaults
- Test profile (`%test`): H2 datasource, Flyway for H2

- [ ] **Step 3: Verify compilation**

Run: `mvn compile -pl WorkFlow`

- [ ] **Step 4: Commit**

---

## Task 4: Runtime Domain Models

**Files:**
- Create: `src/main/kotlin/engine/Models.kt`

Status enums and entity classes for runtime state. These map to database rows.

- [ ] **Step 1: Create status enums and entity classes**

`engine/Models.kt` — package `com.mapreduce.engine`:
- `WorkflowStatus`: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`
- `ActivityStatus`: `PENDING`, `DISPATCHED`, `SUCCEEDED`, `FAILED`
- `TaskStatus`: `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED` — add helper `val isTerminal: Boolean` property (COMPLETED, FAILED)
- `TaskType`: `LINEAR`, `SCATTER`, `FAN_OUT_SUB`, `JOIN_AGGREGATION`
- `WorkflowRun` data class: `id: String`, `definitionJson: String`, `currentActivityIndex: Int`, `status: WorkflowStatus`, `version: Int`, `createdAt: Instant`, `updatedAt: Instant`
- `ActivityInstance` data class: `id: String`, `workflowRunId: String`, `sequenceNumber: Int`, `definitionJson: String`, `nextActivityIndex: Int?`, `status: ActivityStatus`, `version: Int`, `createdAt: Instant`, `updatedAt: Instant`
- `Task` data class: `id: String`, `activityId: String`, `type: TaskType`, `transition: String`, `payloadJson: String?`, `status: TaskStatus`, `retryCount: Int`, `maxRetries: Int`, `deadlineAt: Instant?`, `claimedBy: String?`, `claimedAt: Instant?`, `completedAt: Instant?`, `resultJson: String?`, `createdAt: Instant`, `updatedAt: Instant`

- [ ] **Step 2: Commit**

---

## Task 5: Database Schema

**Files:**
- Create: `src/main/resources/db/migration/V1__create_workflow_tables.sql`
- Create: `src/test/resources/db/migration/V1__create_workflow_tables.sql`

Oracle-compatible DDL for production, H2-compatible for tests. Indexes per Section 9 of the design doc.

- [ ] **Step 1: Create Oracle migration**

Three tables: `workflow_run`, `activity_instance`, `task`.

Key constraints:
- `activity_instance`: unique `(workflow_run_id, sequence_number)`
- `task`: composite index `(activity_id, status)` for lock-free probe
- `task`: index `(status, deadline_at)` for stale task reaper
- `task`: index `(status, claimed_at)` for SKIP LOCKED claiming
- `activity_instance`: index `(status, updated_at)` for sweeper
- NO foreign keys from task → activity (per design doc: no trigger or FK that propagates writes)
- `version` column on `activity_instance` and `workflow_run` defaults to 0

- [ ] **Step 2: Create H2 test migration**

`src/test/resources/db/migration/V1__create_workflow_tables.sql` — same schema adapted for H2 syntax (`CLOB` → `CLOB`, `SYSTIMESTAMP` → `CURRENT_TIMESTAMP`, etc.).

- [ ] **Step 3: Commit**

---

## Task 6: Repository Layer

**Files:**
- Create: `src/main/kotlin/engine/WorkflowRepository.kt`
- Create: `src/main/kotlin/engine/ActivityRepository.kt`
- Create: `src/main/kotlin/engine/TaskRepository.kt`
- Test: `src/test/kotlin/engine/RepositoryTest.kt`

All public methods use JDBI suspend extensions (`withHandleSuspend`, `inTransactionSuspend`). Each repository also exposes `*WithHandle(handle: Handle, ...)` variants for methods used by the barrier, so the barrier can call them within its single transaction handle without opening separate connections.

- [ ] **Step 1: WorkflowRepository**

Suspend methods (open own connection):
- `suspend fun insert(run: WorkflowRun)`
- `suspend fun findById(id: String): WorkflowRun?`
- `suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedVersion: Int): Boolean` (CAS)

Handle methods (for barrier transaction):
- `fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus, expectedVersion: Int): Boolean`

- [ ] **Step 2: ActivityRepository**

Suspend methods:
- `suspend fun insert(activity: ActivityInstance)`
- `suspend fun findById(id: String): ActivityInstance?`
- `suspend fun findByWorkflowAndSequence(workflowRunId: String, sequenceNumber: Int): ActivityInstance?`
- `suspend fun casStatus(id: String, targetStatus: ActivityStatus, expectedVersion: Int): Boolean`
- `suspend fun findOrphaned(gracePeriod: Duration): List<ActivityInstance>`

Handle methods (for barrier transaction):
- `fun insertWithHandle(handle: Handle, activity: ActivityInstance)`
- `fun findByIdWithHandle(handle: Handle, id: String): ActivityInstance?`
- `fun casStatusWithHandle(handle: Handle, id: String, targetStatus: ActivityStatus, expectedVersion: Int): Boolean`

- [ ] **Step 3: TaskRepository**

Suspend methods:
- `suspend fun insertBatch(tasks: List<Task>)`
- `suspend fun claimNext(workerId: String, limit: Int): List<Task>` (SELECT FOR UPDATE SKIP LOCKED)
- `suspend fun updateStatus(id: String, newStatus: TaskStatus, resultJson: String?)`
- `suspend fun countNonTerminal(activityId: String): Int` (lock-free probe — plain SELECT COUNT, no FOR UPDATE)
- `suspend fun countFailed(activityId: String): Int` (count FAILED)
- `suspend fun countTotal(activityId: String): Int`
- `suspend fun findByActivityAndType(activityId: String, type: TaskType): List<Task>`
- `suspend fun findExpired(now: Instant): List<Task>` (deadline reaper query)

Handle methods (for barrier transaction):
- `fun updateStatusWithHandle(handle: Handle, id: String, newStatus: TaskStatus, resultJson: String?)`
- `fun countNonTerminalWithHandle(handle: Handle, activityId: String): Int`
- `fun countFailedWithHandle(handle: Handle, activityId: String): Int`
- `fun countTotalWithHandle(handle: Handle, activityId: String): Int`
- `fun insertBatchWithHandle(handle: Handle, tasks: List<Task>)`

- [ ] **Step 4: Write repository tests**

`engine/RepositoryTest.kt` — H2-backed JDBI tests (no Quarkus, just raw `Jdbi.create()`):
- WorkflowRepository: insert + findById, CAS success, CAS version mismatch → false
- ActivityRepository: insert + findById, CAS success, CAS on wrong status → false, findOrphaned with grace period
- TaskRepository: insertBatch + countNonTerminal, countFailed, claimNext via SKIP LOCKED, updateStatus

- [ ] **Step 5: Commit**

---

## Task 7: Handler Interface & Registry

**Files:**
- Create: `src/main/kotlin/worker/TransitionHandler.kt`
- Create: `src/main/kotlin/worker/HandlerRegistry.kt`
- Test: `src/test/kotlin/worker/HandlerRegistryTest.kt`

CDI-based handler resolution by dot-separated transition key.

- [ ] **Step 1: Create handler interface and qualifier**

`worker/TransitionHandler.kt`:
- `@Qualifier @Retention(RUNTIME) annotation class TransitionKey(val value: String)`
- `interface TransitionHandler { suspend fun execute(input: HandlerInput): HandlerOutput }`
- `data class HandlerInput(val taskId: String, val activityId: String, val workflowRunId: String, val payload: String?)`
- `data class HandlerOutput(val result: String?, val fanOutPayloads: List<String>? = null)` — `fanOutPayloads` only used by scatter handlers

- [ ] **Step 2: Create HandlerRegistry**

`worker/HandlerRegistry.kt`:
- `@ApplicationScoped class HandlerRegistry(private val handlers: Instance<TransitionHandler>)`
- `fun resolve(transitionKey: String): TransitionHandler` — use CDI `Instance.select()` with `TransitionKey` qualifier. Throw `IllegalStateException` if not found.

- [ ] **Step 3: Write tests**

- Register a test handler with `@TransitionKey("test.echo")`, verify resolution
- Verify unknown key throws

- [ ] **Step 4: Commit**

---

## Task 8: Barrier Service (Core Algorithm)

**Files:**
- Create: `src/main/kotlin/engine/BarrierService.kt`
- Test: `src/test/kotlin/engine/BarrierServiceTest.kt`

This is the heart of the engine — Section 6 of the design doc. All four steps in a single `inTransactionSuspend` block. Uses `*WithHandle` repository methods to keep everything on one connection.

- [ ] **Step 1: Create BarrierService**

`engine/BarrierService.kt` — `@ApplicationScoped`:
- `suspend fun onTaskCompleted(taskId: String, activityId: String, result: TaskStatus, resultJson: String?)`

Logic (within one `inTransactionSuspend`):
1. **Self-update:** `taskRepo.updateStatusWithHandle(handle, taskId, result, resultJson)`
2. **Lock-free probe:** `taskRepo.countNonTerminalWithHandle(handle, activityId)` — plain SELECT COUNT, no FOR UPDATE
3. If count > 0: commit and return (other tasks in flight)
4. **Evaluate outcome:** Deserialize `ActivityDefinition` from `activityInstance.definitionJson`. Count `failed` via `taskRepo.countFailedWithHandle()` and `total` via `taskRepo.countTotalWithHandle()`. Apply JoinPolicy (for fan-out) or FailurePolicy (for linear) → determine `targetStatus` (SUCCEEDED or FAILED)
5. **CAS:** `activityRepo.casStatusWithHandle(handle, activityId, targetStatus, expectedVersion)` — if 0 rows affected, another actor won, commit and return
6. **Trigger downstream:** call `advanceWorkflow(handle, ...)` for the CAS winner

- [ ] **Step 2: Implement advanceWorkflow()**

Within the same transaction handle:
- If activity has join transition and SUCCEEDED → insert join aggregation task via `taskRepo.insertBatchWithHandle()`
- If last activity → mark workflow COMPLETED or FAILED via `workflowRepo.updateStatusWithHandle()`
- If SUCCEEDED and next activity exists → create next `ActivityInstance` (PENDING → DISPATCHED) via `activityRepo.insertWithHandle()` + insert its tasks
- If FAILED → evaluate parent activity's `FailurePolicy`. ABORT → workflow FAILED. BEST_EFFORT → advance to next activity (if any).

- [ ] **Step 3: Write barrier unit tests (H2-backed)**

`engine/BarrierServiceTest.kt` — raw JDBI + H2, no Quarkus. Tests per Section 11 of design doc:
1. Single task completes (linear): probe=0, CAS wins, downstream activity created
2. Last-of-many completes (fan-out): Nth task, probe=0, CAS wins, exactly one transition
3. Not-last task: probe > 0, no CAS attempted, only task update committed
4. **CAS race — two concurrent completions:** Two threads both complete their tasks, both see count=0, both attempt CAS. Verify exactly one wins (rows=1), one loses (rows=0), exactly one set of downstream tasks created
5. JoinPolicy ALL with 1 failure → FAILED
6. JoinPolicy PERCENTAGE(95) with 3/100 failed → SUCCEEDED
7. JoinPolicy PERCENTAGE(95) with 10/100 failed → FAILED
8. JoinPolicy THRESHOLD(40) with 45/50 succeeded → SUCCEEDED
9. FailurePolicy BEST_EFFORT on failed activity → advances to next activity
10. Pure barrier (join with no transition) → advances directly to next activity
11. Join with aggregation transition → aggregation task inserted with JOIN_AGGREGATION type
12. Failed tasks counted correctly in policy evaluation (FAILED only, no DEAD_LETTER)

- [ ] **Step 4: Commit**

---

## Task 9: Workflow Engine (Public API)

**Files:**
- Create: `src/main/kotlin/engine/WorkflowEngine.kt`
- Test: `src/test/kotlin/engine/WorkflowEngineTest.kt`

Entry point for starting workflows.

- [ ] **Step 1: Create WorkflowEngine**

`engine/WorkflowEngine.kt` — `@ApplicationScoped`:
- `suspend fun startWorkflow(definition: WorkflowDefinition, initialPayload: String? = null): String` — returns workflow run ID
  - Serialize definition to JSON
  - Insert `WorkflowRun` (RUNNING)
  - Create first `ActivityInstance` (PENDING → DISPATCHED)
  - Insert initial task(s) for first activity
  - Return run ID

- [ ] **Step 2: Write tests**

- Start linear workflow → verify run RUNNING, first activity DISPATCHED, one task PENDING
- Start fan-out workflow → verify scatter task created with activity's own transition

- [ ] **Step 3: Commit**

---

## Task 10: Worker Loop

**Files:**
- Create: `src/main/kotlin/worker/WorkerLoop.kt`
- Test: `src/test/kotlin/worker/WorkerLoopTest.kt`

Poll loop that claims tasks, executes handlers, and feeds results into the barrier.

- [ ] **Step 1: Create WorkerLoop**

`worker/WorkerLoop.kt` — `@ApplicationScoped`, implements `ShutdownParticipant`:
- Owns a private `Channel<Unit>` for shutdown signaling. `shutdown()` sends to this channel, which triggers `takeUntilSignal()` to cancel the flow.
- Uses `indefinitelyRepeat()` + `unorderedMapAsync(concurrency)` + `takeUntilSignal()` from FlowExtension
- Each iteration: `claimNext()` → resolve handler → execute → on success/failure → `barrierService.onTaskCompleted()`
- For scatter tasks (fan-out activity's own transition): handler returns `fanOutPayloads` → insert N sub-tasks → re-enter barrier
- Retry logic: on handler failure, if retryCount < maxRetries → reset task to PENDING. Else → mark FAILED → barrier.
- Deadline enforcement: separate reaper coroutine marks expired PROCESSING tasks as FAILED → barrier.

- [ ] **Step 2: Write tests**

- Claim + execute + complete happy path
- Handler failure with retries remaining → task reset to PENDING
- Handler failure with no retries → task FAILED → barrier fires
- Scatter execution → fan-out sub-tasks created
- Shutdown signal → loop exits gracefully

- [ ] **Step 3: Commit**

---

## Task 11: Leader Sweeper

**Files:**
- Create: `src/main/kotlin/engine/Sweeper.kt`
- Test: `src/test/kotlin/engine/SweeperTest.kt`

Backup path per Section 7. Runs on leader only.

- [ ] **Step 1: Create Sweeper**

`engine/Sweeper.kt` — `@ApplicationScoped`:
- `@Scheduled(every = "{framework.sweeper.interval}", skipExecutionIf = NotLeader::class)`
- `suspend fun patrol()`: query orphaned activities via `activityRepo.findOrphaned(gracePeriod)` → for each, execute same evaluate + CAS + advance logic as BarrierService (reuse the same `*WithHandle` methods)
- Grace period filter: `updated_at < now - gracePeriod`

- [ ] **Step 2: Write tests**

- Orphan detected after grace period → sweeper recovers
- Within grace period → sweeper skips
- Sweeper CAS loses to worker → no duplicate downstream tasks
- Sweeper fires twice on same orphan → second is no-op (idempotent via CAS version)

- [ ] **Step 3: Commit**

---

## Task 12: Observability

**Files:**
- Create: `src/main/kotlin/engine/WorkflowMetrics.kt`
- Modify: `src/main/kotlin/engine/BarrierService.kt` — add metric + logging calls
- Modify: `src/main/kotlin/engine/Sweeper.kt` — add metric + logging calls
- Modify: `src/main/kotlin/worker/WorkerLoop.kt` — add metric calls

Per Section 10 of the design doc.

- [ ] **Step 1: Create WorkflowMetrics**

Centralized metric names and registration:
- Counters: `workflow.barrier.cas.attempts` (labeled won/lost), `workflow.sweeper.recoveries`, `workflow.activity.transitions` (labeled from_status/to_status)
- Gauges: `workflow.activities.dispatched`, `workflow.tasks.by_status`
- Histograms: `workflow.barrier.transaction.duration`, `workflow.activity.completion.duration`

- [ ] **Step 2: Add structured logging**

Per Section 10:
- On CAS win: log `activity_id`, `workflow_run_id`, `task_count`, `failed_count`, `target_state`, `join_policy`, `transaction_duration_ms`
- On CAS loss: log `activity_id` at DEBUG level
- On sweeper recovery: log at WARN level with `activity_id`, `time_since_last_update`, `grace_period`

- [ ] **Step 3: Wire metrics into BarrierService, Sweeper, WorkerLoop**

Add timing and counter increments at appropriate points.

- [ ] **Step 4: Add health checks**

- Sweeper liveness: unhealthy if last patrol > 2x interval ago
- Orphan gauge: number of activities currently matching orphan criteria (zero is normal)

- [ ] **Step 5: Commit**

---

## Task 13: Integration Tests

**Files:**
- Create: `src/test/kotlin/engine/IntegrationTest.kt`

End-to-end workflow lifecycle tests per Section 11, tests #16-19.

- [ ] **Step 1: Linear workflow end-to-end (spec #16)**

3-activity linear workflow. Complete all tasks sequentially. Verify each successor auto-created and workflow reaches COMPLETED.

- [ ] **Step 2: Fan-out workflow end-to-end (spec #17)**

Activity with fan-out of 50 sub-tasks. Complete all. Verify JoinPolicy evaluated, aggregation handler dispatched (if declared), and next activity created.

- [ ] **Step 3: Worker death simulation (spec #18)**

Complete all sub-tasks, simulate worker death by rolling back the CAS transaction. Verify sweeper detects orphan after grace period and completes the transition.

- [ ] **Step 4: High-concurrency barrier (spec #19)**

100+ sub-tasks for a single fan-out, completed near-simultaneously from concurrent threads. Verify exactly one activity transition, one set of downstream tasks, no duplicates.

- [ ] **Step 5: Commit**

---

## Out of Scope (Deferred)

Per spec Section 11, tests #20-21:
- **Load test:** 10,000 concurrent task completions — requires Oracle environment, not feasible with H2.
- **Chaos test:** Random worker kills during workflow — requires Kubernetes cluster.

These are production validation tests, not development-phase tests.

---

## Dependency Graph

```
Task 1 (DSL Models)
  └─► Task 2 (DSL Builder)
  └─► Task 4 (Runtime Models)

Task 3 (FrameworkConfig + application.yaml) ── must complete before Tasks 5-6

Task 4 + Task 3
  └─► Task 5 (DB Schema)
        └─► Task 6 (Repositories + tests)
              └─► Task 8 (Barrier Service) ◄── Task 7 (Handler Interface)
              └─► Task 9 (Workflow Engine)
                    └─► Task 10 (Worker Loop) ◄── Task 7
              └─► Task 11 (Sweeper)

Task 7 (Handler Interface) ── independent, can run after Task 1

Tasks 8-11
  └─► Task 12 (Observability)
        └─► Task 13 (Integration Tests)
```

Execution order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13
