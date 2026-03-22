# Lock-Free Workflow Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a lock-free workflow engine with DAG progression via optimistic CAS, a declarative DSL, and a leader sweeper backup path.

**Architecture:** Two-table model (workflow + task). The engine eliminates per-task row-lock contention by deriving phase completion from MVCC aggregate queries and advancing via optimistic CAS on a single workflow row. Two paths guarantee progress: workers (primary) and a leader sweeper (backup). A Kotlin DSL produces immutable `WorkflowDefinition` data that the engine persists and replays. Activities expand into runtime sequences at build time; the engine is sequence-agnostic.

**Tech Stack:** Kotlin 2.2.0 (pom.xml actual; CLAUDE.md says 2.3.x — reconcile before implementation), Quarkus 3.17.5, JDBI 3.45.0 (suspend extensions), Oracle, Flyway, Micrometer, Kubernetes leader election

## File Structure

```
src/main/kotlin/
  config/
    FrameworkConfig.kt              -- Quarkus @ConfigMapping for all framework settings
  dsl/
    WorkflowDsl.kt                 -- FailurePolicy, JoinPolicy, WorkflowDefinition, ActivityDefinition, FanOutDefinition, JoinDefinition, SequenceMetadata
    WorkflowDslBuilders.kt         -- @DslMarker builders: workflow {}, activity {}, fanOut {}, join {}
  engine/
    WorkflowModels.kt              -- WorkflowStatus, TaskStatus, WorkflowRun, Task
    WorkflowRepository.kt           -- JDBI suspend DAO for workflow table (CAS target)
    TaskRepository.kt               -- JDBI suspend DAO for task table
    BarrierService.kt               -- Lock-free barrier: probe + evaluate + CAS + advance workflow
    WorkflowEngine.kt               -- Public API: start workflow
    Sweeper.kt                      -- Leader sweeper: stuck workflow detection + recovery
  worker/
    TransitionHandler.kt            -- Handler interface + CDI qualifier
    HandlerRegistry.kt              -- CDI-based handler lookup by dot-separated key
    WorkerLoop.kt                   -- Poll loop: claim via SKIP LOCKED, execute, report
  extension/                        -- (existing, unchanged)
  leader/                           -- (existing, unchanged)
  shutdown/                         -- (existing, unchanged)

src/main/resources/
  application.properties              -- Quarkus + datasource + framework config
  db/migration/
    V1__create_workflow_tables.sql   -- workflow, task tables + indexes

src/test/kotlin/
  dsl/
    WorkflowDslTest.kt             -- Serialization round-trip tests
    WorkflowDslBuildersTest.kt      -- DSL builder + validation tests
  engine/
    WorkflowModelsTest.kt          -- Runtime model tests
    RepositoryTest.kt               -- Repository CRUD + CAS tests (Oracle Free container)
    BarrierServiceTest.kt           -- Lock-free barrier unit tests (Oracle Free container)
    WorkflowEngineTest.kt           -- Workflow start + progression tests
    SweeperTest.kt                  -- Stuck workflow detection + recovery tests
    IntegrationTest.kt              -- End-to-end workflow lifecycle tests
  worker/
    HandlerRegistryTest.kt          -- CDI handler resolution tests
    WorkerLoopTest.kt               -- Claim + execute + report tests
```

---

## Task 1: DSL Data Models & Enums

**Files:**
- Existing: `src/main/kotlin/dsl/WorkflowDsl.kt` (already implemented, verify alignment)
- Existing: `src/test/kotlin/dsl/WorkflowDslTest.kt` (already implemented, verify alignment)

Pure data layer. No dependencies on engine or JDBI. All types are immutable.

- [ ] **Step 1: Verify existing enums and data classes**

`dsl/WorkflowDsl.kt` — package `com.workflow.dsl` — should contain:
- `FailurePolicy` enum: `ABORT`, `BEST_EFFORT`
- `JoinPolicy` sealed interface: `All` object, `Threshold(n: Int)`, `Percentage(pct: Int)` — validate n > 0, pct in 1..100
- `JoinDefinition` data class: `policy: JoinPolicy`, `transition: String?`
- `FanOutDefinition` data class: `transition: String`, `retries: Int`, `failurePolicy: FailurePolicy`, `deadline: Duration`, `join: JoinDefinition`
- `ActivityDefinition` data class: `name: String`, `transition: String`, `retries: Int`, `failurePolicy: FailurePolicy`, `deadline: Duration`, `fanOut: FanOutDefinition?`
- `PhaseType` enum: `LINEAR`, `SCATTER`, `PARALLEL` — describes the type of runtime sequence
- `SequenceMetadata` data class: `phaseType: PhaseType`, `handlerKey: String`, `retries: Int`, `deadline: Duration`, `failurePolicy: FailurePolicy`, `joinDefinition: JoinDefinition?`, `activityIndex: Int` — pre-computed sequence expansion metadata
- `WorkflowDefinition` data class: `activities: List<ActivityDefinition>` — validate non-empty. Include `fun expandSequences(): List<SequenceMetadata>` that pre-computes the expansion (linear → 1 seq, fan-out → 2 seqs: scatter + parallel)

Default values: `retries = 0`, `failurePolicy = ABORT`, `deadline = Duration.ofMinutes(30)`, `fanOut = null`.

- [ ] **Step 2: Verify serialization round-trip test**

`dsl/WorkflowDslTest.kt` — verify Jackson serialization/deserialization of a `WorkflowDefinition` with fan-out produces identical output. Test both linear and fan-out definitions. Test `expandSequences()` produces correct sequence mapping.

- [ ] **Step 3: Verify tests pass, commit**

Run: `mvn test -pl WorkFlow -Dtest="dsl.WorkflowDslTest"`

---

## Task 2: DSL Builder

**Files:**
- Create: `src/main/kotlin/dsl/WorkflowDslBuilders.kt`
- Test: `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt`

Kotlin DSL with `@DslMarker` to prevent scope leakage. Build-phase validation.

- [ ] **Step 1: Create DslMarker and builders**

`dsl/WorkflowDslBuilders.kt` — package `com.workflow.dsl`:
- `@DslMarker annotation class WorkflowDsl`
- `JoinBuilder`: `policy()`, `transition()` → builds `JoinDefinition`
- `FanOutBuilder`: `transition()`, `retries()`, `failurePolicy()`, `deadline()`, `join {}` → builds `FanOutDefinition`. Validate: transition required, join required.
- `ActivityBuilder`: `transition()`, `retries()`, `failurePolicy()`, `deadline()`, `fanOut {}` → builds `ActivityDefinition`. Validate: transition required.
- `WorkflowBuilder`: `activity("name") {}` → builds `WorkflowDefinition`. Validate: at least one activity.
- Top-level `fun workflow(block: WorkflowBuilder.() -> Unit): WorkflowDefinition`

- [ ] **Step 2: Write builder tests**

`dsl/WorkflowDslBuildersTest.kt`:
- Happy path: linear workflow with 2 activities
- Happy path: fan-out with join policy PERCENTAGE(95) and join transition
- Happy path: pure barrier (join with no transition)
- Validation: missing activity transition → `IllegalArgumentException`
- Validation: fanOut without join → `IllegalArgumentException`
- Validation: empty workflow → `IllegalArgumentException`
- Scope leakage: verify `@DslMarker` prevents calling `activity {}` inside `fanOut {}`

- [ ] **Step 3: Verify tests pass, commit**

Run: `mvn test -pl WorkFlow -Dtest="dsl.WorkflowDslBuildersTest"`

---

## Task 3: FrameworkConfig & application.properties

**Files:**
- Create: `src/main/kotlin/config/FrameworkConfig.kt`
- Create: `src/main/resources/application.properties`

Quarkus `@ConfigMapping` interface. Unblocks LeaderManager and ShutdownCoordinator compilation. **Must complete before Tasks 5-6** (datasource config required for schema/repository tests).

- [ ] **Step 1: Create FrameworkConfig interface**

`config/FrameworkConfig.kt` — package `com.workflow.config`:
- `@ConfigMapping(prefix = "framework")` interface with named nested interfaces matching existing usage:
  - `WorkerConfig`: `id(): String` (default: hostname), `pollInterval(): Duration`, `concurrency(): Int`
  - `LeaderElectionConfig`: `namespace(): String`, `leaseName(): String`, `leaseDuration(): Duration`, `renewDeadline(): Duration`, `retryPeriod(): Duration`
  - `ShutdownConfig`: `globalTimeout(): Duration`, `leaderTeardownTimeout(): Duration`
  - `SweeperConfig`: `interval(): Duration`, `gracePeriod(): Duration`
- Methods: `worker(): WorkerConfig`, `leaderElection(): LeaderElectionConfig`, `shutdown(): ShutdownConfig`, `sweeper(): SweeperConfig`

Use `@WithDefault` annotations for sensible defaults.

- [ ] **Step 2: Create application.properties**

`src/main/resources/application.properties`:
- Quarkus datasource config (Oracle placeholder)
- Flyway config
- Framework config section with defaults

Test config lives in `src/test/resources/application.properties` (per CLAUDE.md: do not use `%test.*` profile lines in main `application.properties`).

- [ ] **Step 3: Verify compilation**

Run: `mvn compile -pl WorkFlow`

- [ ] **Step 4: Commit**

---

## Task 4: Runtime Domain Models

**Files:**
- Rewrite: `src/main/kotlin/engine/WorkflowModels.kt` (exists with old three-table model — must be rewritten)
- Rewrite: `src/test/kotlin/engine/WorkflowModelsTest.kt` (exists with old model tests — must be rewritten)

Status enums and entity classes for runtime state. Two-table model: workflow + task. No activity table.

**Migration note:** Existing implementation has `WorkflowStatus.PENDING`, `ActivityStatus`, `TaskType.JOIN_AGGREGATION`, `ActivityInstance`, and `WorkflowRun.currentActivityIndex` from the old three-table model. All must be replaced.

- [ ] **Step 1: Rewrite status enums and entity classes**

`engine/WorkflowModels.kt` — package `com.workflow.engine`:
- `WorkflowStatus`: `RUNNING`, `COMPLETED`, `FAILED` — no PENDING (created directly as RUNNING). Remove old `PENDING`.
- `TaskStatus`: `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED` — add helper `val isTerminal: Boolean` property (COMPLETED, FAILED)
- `WorkflowRun` data class: `id: String`, `definitionJson: String`, `currentSequence: Int`, `version: Int`, `status: WorkflowStatus`, `createdAt: Instant`, `updatedAt: Instant`
- `Task` data class: `id: String`, `workflowId: String`, `sequenceNumber: Int`, `status: TaskStatus`, `handlerKey: String`, `payloadJson: String?`, `resultJson: String?`, `claimedBy: String?`, `claimedAt: Instant?`, `completedAt: Instant?`, `retryCount: Int`, `maxRetries: Int`, `deadlineAt: Instant?`
- Remove: `ActivityStatus`, `TaskType`, `ActivityInstance` — no longer exist in two-table model. `PhaseType` lives in DSL layer (Task 1).

- [ ] **Step 2: Rewrite model tests and commit**

`engine/WorkflowModelsTest.kt` — verify enum values, `isTerminal` property, data class construction. Remove all `ActivityStatus`/`ActivityInstance`/`TaskType` tests.

---

## Task 5: Database Schema

**Files:**
- Create: `src/main/resources/db/migration/V1__create_workflow_tables.sql`
- Create: `src/test/resources/db/migration/V1__create_workflow_tables.sql`

Oracle-compatible DDL for both production and tests (Oracle Free container via Testcontainers). Indexes per Section 9 of the design doc. Two tables only.

- [ ] **Step 1: Create Oracle migration**

Two tables: `workflow`, `task`. No activity table — activity metadata lives in the serialized `WorkflowDefinition`.

`workflow` table:
- `id` (PK), `definition` (CLOB/JSON, write-once), `current_sequence` (Int), `version` (Int, default 0), `status` (Enum: RUNNING/COMPLETED/FAILED), `updated_at` (Timestamp), `created_at` (Timestamp)
- Index: `(status, updated_at)` — sweeper query

`task` table:
- `id` (PK), `workflow_id` (FK), `sequence_number` (Int), `status` (Enum), `handler_key` (String), `payload` (CLOB/JSON), `result` (CLOB/JSON), `claimed_by`, `claimed_at`, `completed_at`, `retry_count`, `max_retries`, `deadline_at`
- Index: `(workflow_id, sequence_number, status)` — lock-free probe (critical composite index, must produce index-only scan)
- Index: `(status, deadline_at)` — stale task reaper
- Index: `(status, claimed_at)` — SKIP LOCKED claiming
- NO trigger or foreign key that propagates writes to the workflow table on task status change

- [ ] **Step 2: Test migration**

`src/test/resources/db/migration/V1__create_workflow_tables.sql` — same Oracle DDL used for both production and test (Oracle Free container eliminates dialect gaps).

- [ ] **Step 3: Commit**

---

## Task 6: Repository Layer

**Files:**
- Create: `src/main/kotlin/engine/WorkflowRepository.kt`
- Create: `src/main/kotlin/engine/TaskRepository.kt`
- Test: `src/test/kotlin/engine/RepositoryTest.kt`

Two repositories only (no ActivityRepository — no activity table). All public methods use JDBI suspend extensions (`withHandleSuspend`, `inTransactionSuspend`). Each repository also exposes `*WithHandle(handle: Handle, ...)` variants for methods used by the barrier, so the barrier can call them within its single transaction handle without opening separate connections.

- [ ] **Step 1: WorkflowRepository**

Suspend methods (open own connection):
- `suspend fun insert(run: WorkflowRun)`
- `suspend fun findById(id: String): WorkflowRun?`
- `suspend fun casAdvance(id: String, expectedSequence: Int, nextSequence: Int, expectedVersion: Int): Boolean` (CAS on workflow row — the core transition)
- `suspend fun updateStatus(id: String, newStatus: WorkflowStatus): Boolean`
- `suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun>` (sweeper: RUNNING, zero non-terminal tasks at current_sequence, updated_at past grace period)

Handle methods (for barrier transaction):
- `fun casAdvanceWithHandle(handle: Handle, id: String, expectedSequence: Int, nextSequence: Int, expectedVersion: Int): Boolean`
- `fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus): Boolean`

- [ ] **Step 2: TaskRepository**

Suspend methods:
- `suspend fun insertBatch(tasks: List<Task>)`
- `suspend fun claimNext(workerId: String, limit: Int): List<Task>` (SELECT FOR UPDATE SKIP LOCKED)
- `suspend fun updateStatus(id: String, newStatus: TaskStatus, resultJson: String?)`
- `suspend fun countNonTerminal(workflowId: String, sequenceNumber: Int): Int` (lock-free probe — plain SELECT COUNT, no FOR UPDATE)
- `suspend fun countFailed(workflowId: String, sequenceNumber: Int): Int` (count FAILED tasks)
- `suspend fun countTotal(workflowId: String, sequenceNumber: Int): Int`
- `suspend fun findByWorkflowAndSequence(workflowId: String, sequenceNumber: Int): List<Task>`
- `suspend fun findExpired(now: Instant): List<Task>` (deadline reaper query)

Handle methods (for barrier transaction):
- `fun updateStatusWithHandle(handle: Handle, id: String, newStatus: TaskStatus, resultJson: String?)`
- `fun countNonTerminalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int`
- `fun countFailedWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int`
- `fun countTotalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int`
- `fun insertBatchWithHandle(handle: Handle, tasks: List<Task>)`

- [ ] **Step 3: Write repository tests**

`engine/RepositoryTest.kt` — Oracle Free container JDBI tests (Testcontainers, raw `Jdbi.create()`):
- WorkflowRepository: insert + findById, CAS advance success, CAS version mismatch → false, CAS sequence mismatch → false, findStuck with grace period
- TaskRepository: insertBatch + countNonTerminal, countFailed, claimNext via SKIP LOCKED, updateStatus

- [ ] **Step 4: Commit**

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
- `data class HandlerInput(val taskId: String, val workflowId: String, val sequenceNumber: Int, val payload: String?)`
- `data class HandlerOutput(val result: String?)` — scatter handlers serialize their payload list as the result string; the barrier interprets it based on phase type

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

This is the heart of the engine — Section 6 of the design doc. All four steps in a single `inTransactionSuspend` block. CAS target is the workflow row. Uses `*WithHandle` repository methods to keep everything on one connection.

- [ ] **Step 1: Create BarrierService**

`engine/BarrierService.kt` — `@ApplicationScoped`:
- `suspend fun onTaskCompleted(taskId: String, workflowId: String, sequenceNumber: Int, result: TaskStatus, resultJson: String?)`

Logic (within one `inTransactionSuspend`):
1. **Self-update:** `taskRepo.updateStatusWithHandle(handle, taskId, result, resultJson)`
2. **Lock-free probe:** `taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)` — plain SELECT COUNT, no FOR UPDATE
3. If count > 0: commit and return (other tasks in flight)
4. **Evaluate outcome:** Load workflow run, deserialize `WorkflowDefinition`, look up sequence metadata. Count `failed` via `taskRepo.countFailedWithHandle()` and `total` via `taskRepo.countTotalWithHandle()`. Apply policy based on phase type:
    - PARALLEL phase → evaluate `JoinPolicy` from the `JoinDefinition`
    - LINEAR / SCATTER phase → evaluate `FailurePolicy` from the `ActivityDefinition`
    - Determine target outcome: success or failure
5. **CAS:** `workflowRepo.casAdvanceWithHandle(handle, workflowId, expectedSequence, nextSequence, expectedVersion)` — if 0 rows affected, another actor won, commit and return
6. **Advance workflow:** call `advanceWorkflow(handle, ...)` for the CAS winner

- [ ] **Step 2: Implement advanceWorkflow()**

Within the same transaction handle:
- **If outcome is failure** and parent activity's FailurePolicy is ABORT → mark workflow FAILED via `workflowRepo.updateStatusWithHandle()`
- **If outcome is failure** and FailurePolicy is BEST_EFFORT → treat as success, continue to next sequence
- **If current sequence is PARALLEL with join transition** (outcome is success) → execute join handler inline within this transaction. If join handler fails → treat as failure, apply logic above.
- **If last sequence in definition** → mark workflow COMPLETED via `workflowRepo.updateStatusWithHandle()`
- **If next sequence exists** → insert tasks for next sequence:
    - SCATTER or LINEAR phase: insert 1 task with appropriate handler key and payload
    - PARALLEL phase: read scatter task's `result` column from preceding SCATTER sequence, deserialize payloads, bulk-insert N sub-tasks via `taskRepo.insertBatchWithHandle()`

- [ ] **Step 3: Write barrier unit tests (Oracle Free container)**

`engine/BarrierServiceTest.kt` — raw JDBI + Oracle Free container (Testcontainers). Tests per Section 11 of design doc:
1. Single task completes (linear): probe=0, CAS wins, next sequence's tasks inserted
2. Last-of-many completes (parallel phase): Nth task, probe=0, CAS wins, exactly one phase transition
3. Not-last task: probe > 0, no CAS attempted, only task update committed
4. **CAS race — two concurrent completions:** Two threads both complete their tasks, both see count=0, both attempt CAS. Verify exactly one wins (rows=1), one loses (rows=0), exactly one set of downstream tasks created
5. JoinPolicy ALL with 1 failure → outcome = failure
6. JoinPolicy PERCENTAGE(95) with 3/100 failed → outcome = success
7. JoinPolicy PERCENTAGE(95) with 10/100 failed → outcome = failure
8. JoinPolicy THRESHOLD(40) with 45/50 succeeded → outcome = success
9. FailurePolicy BEST_EFFORT on failed phase → workflow advances to next sequence
10. Pure barrier (join with no transition) → workflow advances immediately
11. Join with inline transition: CAS wins on parallel phase, join declares transition → join handler executed and workflow advances
12. Scatter → parallel handoff: scatter task completes with payloads in `result` → CAS winner reads result, inserts correct number of sub-tasks at next sequence

- [ ] **Step 4: Commit**

---

## Task 9: Workflow Engine (Public API)

**Files:**
- Create: `src/main/kotlin/engine/WorkflowEngine.kt`
- Test: `src/test/kotlin/engine/WorkflowEngineTest.kt`

Entry point for starting workflows. No activity instance creation — workflow created directly as RUNNING with first tasks inserted, all in one transaction.

- [ ] **Step 1: Create WorkflowEngine**

`engine/WorkflowEngine.kt` — `@ApplicationScoped`:
- `suspend fun startWorkflow(definition: WorkflowDefinition, initialPayload: String? = null): String` — returns workflow run ID
  - Serialize definition to JSON
  - Pre-compute sequence expansion from definition (linear → 1 seq, fan-out → 2 seqs: scatter + parallel)
  - Insert `WorkflowRun` (RUNNING, `current_sequence = 1`)
  - Insert initial task(s) for sequence 1
  - Return run ID
  - All in one transaction

- [ ] **Step 2: Write tests**

- Start linear workflow → verify run RUNNING, `current_sequence = 1`, one task PENDING with correct handler key
- Start fan-out workflow → verify scatter task created at sequence 1 with activity's own transition

- [ ] **Step 3: Commit**

---

## Task 10: Worker Loop

**Files:**
- Create: `src/main/kotlin/worker/WorkerLoop.kt`
- Test: `src/test/kotlin/worker/WorkerLoopTest.kt`

Poll loop that claims tasks, executes handlers, and feeds results into the barrier. Join handlers are executed inline by the barrier (CAS winner), not as separate tasks.

- [ ] **Step 1: Create WorkerLoop**

`worker/WorkerLoop.kt` — `@ApplicationScoped`, implements `ShutdownParticipant`:
- Owns a private `Channel<Unit>` for shutdown signaling. `shutdown()` sends to this channel, which triggers `takeUntilSignal()` to cancel the flow.
- Uses `indefinitelyRepeat()` + `unorderedMapAsync(concurrency)` + `takeUntilSignal()` from FlowExtension
- Each iteration: `claimNext()` → resolve handler → execute → on success/failure → `barrierService.onTaskCompleted()`
- Retry logic: on handler failure, if retryCount < maxRetries → reset task to PENDING. Else → mark FAILED → barrier.
- Deadline enforcement: separate reaper coroutine marks expired PROCESSING tasks as FAILED → barrier.

- [ ] **Step 2: Write tests**

- Claim + execute + complete happy path
- Handler failure with retries remaining → task reset to PENDING
- Handler failure with no retries → task FAILED → barrier fires
- Shutdown signal → loop exits gracefully

- [ ] **Step 3: Commit**

---

## Task 11: Leader Sweeper

**Files:**
- Create: `src/main/kotlin/engine/Sweeper.kt`
- Test: `src/test/kotlin/engine/SweeperTest.kt`

Backup path per Section 7. Runs on leader only. Detects stuck workflows (not orphaned activities).

- [ ] **Step 1: Create Sweeper**

`engine/Sweeper.kt` — `@ApplicationScoped`:
- `@Scheduled(every = "{framework.sweeper.interval}", skipExecutionIf = NotLeader::class)`
- `suspend fun patrol()`: query stuck workflows via `workflowRepo.findStuck(gracePeriod)` → for each, execute same evaluate + CAS + advance logic as BarrierService (reuse the same `*WithHandle` methods)
- Stuck criteria: `status = 'RUNNING'`, zero tasks in non-terminal state at `current_sequence`, `updated_at < now - gracePeriod`

- [ ] **Step 2: Write tests**

- Stuck workflow detected after grace period → sweeper recovers
- Within grace period → sweeper skips
- Sweeper CAS loses to worker → no duplicate downstream tasks
- Sweeper fires twice on same stuck workflow → second is no-op (idempotent via CAS version)

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
- Counters: `workflow.barrier.cas.attempts{outcome=won|lost}`, `workflow.sweeper.recoveries`, `workflow.phase.transitions{from_seq, to_seq, phase_type}`
- Gauges: `workflow.running.count`, `workflow.tasks.by_status`
- Histograms: `workflow.barrier.transaction.duration`, `workflow.phase.completion.duration`

- [ ] **Step 2: Add structured logging**

Per Section 10:
- On CAS win: log `workflow_id`, `sequence_number`, `phase_type`, `task_count`, `failed_count`, `target_outcome`, `transaction_duration_ms`
- On CAS loss: log `workflow_id` at DEBUG level
- On sweeper recovery: log at WARN level with `workflow_id`, `sequence_number`, `time_since_last_update`, `grace_period`

- [ ] **Step 3: Wire metrics into BarrierService, Sweeper, WorkerLoop**

Add timing and counter increments at appropriate points.

- [ ] **Step 4: Add health checks**

- Sweeper liveness: unhealthy if last patrol > 2x interval ago
- Stuck workflow gauge: number of workflows currently matching stuck criteria (zero is normal)

- [ ] **Step 5: Commit**

---

## Task 13: Integration Tests

**Files:**
- Create: `src/test/kotlin/engine/IntegrationTest.kt`

End-to-end workflow lifecycle tests per Section 11, tests #17-20.

- [ ] **Step 1: Linear workflow end-to-end (spec #17)**

3-sequence linear workflow. Complete tasks in order. Verify each successor auto-created and workflow reaches COMPLETED.

- [ ] **Step 2: Fan-out workflow end-to-end (spec #18)**

Scatter → parallel (50 sub-tasks) → next linear. Verify scatter produces payloads, sub-tasks created, JoinPolicy evaluated, workflow advances.

- [ ] **Step 3: Worker death simulation (spec #19)**

All sub-tasks terminal, simulate worker OOM by rolling back the CAS transaction. Verify sweeper detects stuck workflow and completes the transition.

- [ ] **Step 4: High-concurrency barrier (spec #20)**

100+ sub-tasks for a single parallel phase, completed near-simultaneously from concurrent threads. Verify exactly one phase transition, no duplicates, no lock-wait timeouts.

- [ ] **Step 5: Commit**

---

## Out of Scope (Deferred)

Per spec Section 11, tests #21-22:
- **Load test:** 10,000 concurrent task completions — requires production-scale Oracle environment, not feasible with Oracle Free container.
- **Chaos test:** Random worker kills during workflow — requires Kubernetes cluster.

These are production validation tests, not development-phase tests.

---

## Dependency Graph

```
Task 1 (DSL Models)
  └─► Task 2 (DSL Builder)
  └─► Task 4 (Runtime Models)

Task 3 (FrameworkConfig + application.properties) ── must complete before Tasks 5-6

Task 4 + Task 3
  └─► Task 5 (DB Schema — two tables: workflow + task)
        └─► Task 6 (Repositories — WorkflowRepository + TaskRepository)
              └─► Task 8 (Barrier Service — CAS on workflow row) ◄── Task 7 (Handler Interface)
              └─► Task 9 (Workflow Engine)
                    └─► Task 10 (Worker Loop) ◄── Task 7
              └─► Task 11 (Sweeper — stuck workflow detection)

Task 7 (Handler Interface) ── independent, can run after Task 1

Tasks 8-11
  └─► Task 12 (Observability)
        └─► Task 13 (Integration Tests)
```

Execution order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13
