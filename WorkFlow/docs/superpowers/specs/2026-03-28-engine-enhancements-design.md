# Engine Enhancements — Task Queues, Async Signals, Declarative Workflows, Dynamic Branching

**Date:** 2026-03-28
**Prerequisites:** All prior sessions (V1–V4 schema, dead-letter, backoff, cancel/timeout)

---

## Overview

Four enhancements to make the workflow engine feature-rich while preserving its lock-free CAS simplicity. A foundational **barrier refactoring** prepares the codebase so each enhancement plugs in with minimal diff.

**Priority order (by effort-to-value):**

| # | Enhancement | Complexity | Engine Core Impact |
|---|-------------|-----------|-------------------|
| 1 | Task Queues & Worker Routing | Low | Minimal (filter only) |
| 2 | Asynchronous Wait / External Signals | Medium-Low | New status + signal API |
| 3 | Declarative Workflows (REST + Templates) | Medium | Zero (pure ops layer) |
| 4 | Dynamic Branching & Conditional Logic | High | Fundamental (barrier, sequence model) |

**Implementation strategy:** A refactoring plan (Section 7) restructures the current codebase to absorb all four enhancements with minimal effort. The refactoring ships first. Enhancements are implemented incrementally afterward.

---

## 1. Task Queues & Worker Routing

### Problem

All workers claim from a single global pool. A long-running compute job can starve fast, lightweight tasks.

### Design

Add a `queue_name` dimension to task claiming. Workers are configured to poll a specific queue. Tasks are routed to queues via the DSL.

**Schema (V5 migration):**

```sql
ALTER TABLE task ADD queue_name VARCHAR2(100) DEFAULT 'default' NOT NULL;
CREATE INDEX idx_task_queue_status ON task (queue_name, status, not_before, claimed_at);
```

**Model changes:**

- `ActivityDefinition`: add `queue: String = "default"`
- `FanOutDefinition`: add `queue: String = "default"` (defaults to parent activity's queue if not set)
- `Task`: add `queueName: String = "default"`
- `createTaskForActivity`: propagate `activity.queue` to `task.queueName`

**DSL:**

```kotlin
activity("heavy-compute") {
    transition("ml.train")
    queue("gpu-workers")
    fanOut {
        transition("ml.infer")
        queue("gpu-workers")    // inherits from activity if omitted
    }
}
```

**Repository — `claimNext`:**

Add `AND queue_name = :queueName` to the WHERE clause. New parameter `queueName: String = "default"`.

**WorkerLoop:**

Reads `queue` from `WorkerConfig`, passes to `claimNext(workerId, batchSize, queueName)`.

**Config:**

```properties
framework.worker.queue=default
```

**What stays unchanged:** BarrierService, Sweeper, PhaseStrategies, CAS model. They don't touch queue routing.

**Backward compatibility:** Existing workflows and tasks get `queue_name = 'default'` via the column default. Existing workers poll `"default"` by default.

---

## 2. Asynchronous Wait / External Signals

### Problem

The engine expects every task to complete synchronously via a handler. There is no way to pause a workflow until an external system (webhook, human approval) signals it to continue.

### Design

A new non-terminal `TaskStatus.WAITING_FOR_SIGNAL` parks a task. The barrier naturally blocks (non-terminal count > 0). A REST endpoint receives the signal and calls the barrier to advance.

### Status & Transitions

```
PROCESSING       -> WAITING_FOR_SIGNAL   (handler returns suspend=true)
WAITING_FOR_SIGNAL -> COMPLETED          (signal: approved)
WAITING_FOR_SIGNAL -> FAILED             (signal: rejected)
WAITING_FOR_SIGNAL -> TIMED_OUT          (sweeper: deadline expired)
WAITING_FOR_SIGNAL -> CANCELLED          (workflow cancelled)
```

**Schema:** Update task status CHECK constraint to include `'WAITING_FOR_SIGNAL'`.

### HandlerOutput Change

```kotlin
data class HandlerOutput(
    val result: String? = null,
    val suspend: Boolean = false,
)
```

When `suspend = true`, the worker transitions the task to `WAITING_FOR_SIGNAL` and stores `result` as `resultJson` (intermediate state readable by the signal endpoint). The barrier is NOT called.

### WorkerLoop Change

After `handler.execute(input)`:

```kotlin
val output = handler.execute(input)
if (output.suspend) {
    taskRepo.suspendTask(task.id, output.result)
    notificationDispatcher.onTaskSuspended(task, definition)
} else {
    barrierService.onTaskCompleted(..., COMPLETED, output.result)
}
```

### Signal REST Endpoint

New file `SignalResource.kt`:

```kotlin
@Path("/api/workflows/{workflowId}/tasks/{taskId}/signal")
class SignalResource(
    private val taskRepo: TaskRepository,
    private val barrierService: BarrierService,
) {
    @POST
    suspend fun signal(
        @PathParam workflowId: String,
        @PathParam taskId: String,
        body: SignalRequest,    // { "approved": true, "payload": "..." }
    ): Response {
        // 1. Validate task is WAITING_FOR_SIGNAL and belongs to workflowId
        // 2. Determine status: COMPLETED if approved, FAILED if rejected
        // 3. Call barrierService.onTaskCompleted(taskId, workflowId, seq, status, body.payload)
        // Returns 200 on success, 404 if not found, 409 if wrong status
    }
}
```

The barrier call is identical to the worker path. The signal endpoint is an alternate "completer."

### Configurable Notification

When a task enters `WAITING_FOR_SIGNAL`, the engine can notify external systems.

**Model:**

```kotlin
data class SignalDefinition(
    val notifications: List<NotificationConfig> = emptyList(),
    val timeout: Duration? = null,   // overrides activity.deadline for the waiting phase
)

data class NotificationConfig(
    val channel: SignalChannel,
    val properties: Map<String, String>,   // channel-specific (url, to, template, etc.)
)

enum class SignalChannel { WEBHOOK, EMAIL, SLACK }
```

`SignalDefinition` is an optional field on `ActivityDefinition`:

```kotlin
data class ActivityDefinition(
    // ... existing fields ...
    val signal: SignalDefinition? = null,
)
```

**DSL:**

```kotlin
activity("human-approval") {
    transition("approval.handler")
    signal {
        notify(SignalChannel.WEBHOOK) {
            url("https://hooks.slack.com/...")
            template("approval-request")
        }
        notify(SignalChannel.EMAIL) {
            to("approvers@company.com")
            template("approval-email")
        }
        timeout(Duration.ofHours(24))
    }
}
```

**Dispatch:**

```kotlin
interface NotificationDispatcher {
    suspend fun onTaskSuspended(task: Task, signalDef: SignalDefinition)
}

interface NotificationSender {
    suspend fun send(task: Task, properties: Map<String, String>)
}
```

Each `SignalChannel` has a `NotificationSender` CDI bean. Adding a new channel = one new class. Notification is **fire-and-forget** — failure is logged but does not affect workflow state. The deadline provides the safety net.

### Sweeper Addition

New operation in `Sweeper.patrol()`:

```sql
UPDATE task SET status = 'TIMED_OUT', completed_at = :now
WHERE status = 'WAITING_FOR_SIGNAL' AND deadline_at IS NOT NULL AND deadline_at < :now
```

After timing out, the sweeper calls `barrierService.onTaskCompleted(TIMED_OUT)` which triggers the strategy's failure policy evaluation.

### Cancel Workflow Integration

Extend `cancelPendingTasksWithHandle`:

```sql
WHERE workflow_id = :workflowId AND status IN ('PENDING', 'WAITING_FOR_SIGNAL')
```

### What Stays Unchanged

- **BarrierService / PhaseStrategies**: `WAITING_FOR_SIGNAL` is non-terminal, so `countNonTerminalWithHandle` sees it and the barrier waits. Strategies only run when all tasks are terminal.
- **TaskRepository.claimNext**: Filters `status = 'PENDING'` — never claims suspended tasks.
- **Stale task reclaim**: Filters `status = 'PROCESSING'` — doesn't touch suspended tasks.

### Handler Usage Example

```kotlin
class HumanApprovalHandler : TransitionHandler {
    override suspend fun execute(input: HandlerInput): HandlerOutput {
        notificationService.sendApprovalRequest(input.taskId, input.payload)
        return HandlerOutput(result = input.payload, suspend = true)
    }
}
```

---

## 3. Declarative Workflows (REST API + External Storage)

### Problem

Workflows are defined via compiled Kotlin DSL. Creating or modifying a workflow requires recompilation and redeployment.

### Design

Store `WorkflowDefinition` JSON in an Oracle table. A REST API accepts, validates, and persists definitions. A start endpoint loads the definition and calls `WorkflowEngine.startWorkflow()`. Zero engine changes.

### Template Table (V5/V6 Migration)

```sql
CREATE TABLE workflow_template (
    name        VARCHAR2(255)   NOT NULL,
    version     NUMBER(10)      NOT NULL,
    definition  CLOB            NOT NULL,
    created_at  TIMESTAMP       NOT NULL,
    created_by  VARCHAR2(100),
    CONSTRAINT pk_workflow_template PRIMARY KEY (name, version)
);

CREATE INDEX idx_template_name ON workflow_template (name, version DESC);
```

Versioned by `(name, version)` composite key. Immutable — each change is a new version row.

### Template Repository

```kotlin
@ApplicationScoped
class WorkflowTemplateRepository(private val jdbi: Jdbi) {
    suspend fun save(name: String, version: Int, definitionJson: String, createdBy: String?)
    suspend fun findLatest(name: String): WorkflowTemplate?
    suspend fun findByVersion(name: String, version: Int): WorkflowTemplate?
    suspend fun listAll(): List<WorkflowTemplateSummary>
}
```

### Validation Service

```kotlin
@ApplicationScoped
class TemplateValidationService(
    private val objectMapper: ObjectMapper,
    private val handlerRegistry: HandlerRegistry,
) {
    fun validate(definitionJson: String): ValidationResult {
        // 1. Parse JSON -> WorkflowDefinition
        // 2. Verify all handler keys exist in HandlerRegistry
        // 3. Verify CHOICE branch conditions are syntactically valid
        // 4. Verify no empty activity lists
    }
}
```

Handler key validation is **eager** — reject at creation time if a handler doesn't exist.

### REST API

**Template management** (`TemplateResource.kt`):

| Method | Path | Action |
|--------|------|--------|
| `POST` | `/api/templates` | Create template (auto-increment version) |
| `GET` | `/api/templates/{name}` | Get latest version |
| `GET` | `/api/templates/{name}/versions/{version}` | Get specific version |
| `GET` | `/api/templates` | List all templates |
| `POST` | `/api/templates/{name}/start` | Start workflow from template |

**Workflow query** (`WorkflowResource.kt`):

| Method | Path | Action |
|--------|------|--------|
| `GET` | `/api/workflows/{id}` | Workflow status |
| `GET` | `/api/workflows/{id}/tasks` | Task list |
| `POST` | `/api/workflows/{id}/cancel` | Cancel workflow |
| `POST` | `/api/workflows/{id}/replay` | Replay failed workflow |

These wrap existing `WorkflowEngine` methods — no new engine logic.

### Running Workflows Are Decoupled From Templates

When a workflow starts, the `WorkflowDefinition` JSON is copied into the `workflow.definition` CLOB. No foreign key to `workflow_template`. Updating or deleting a template does not affect running workflows.

---

## 4. Dynamic Branching & Conditional Logic (CHOICE)

### Problem

The engine operates on a strict numerical sequence with fixed phase types (LINEAR, SCATTER, PARALLEL). There is no way for a workflow to make data-dependent routing decisions.

### Design

A new `PhaseType.CHOICE` with a dedicated `ChoicePhaseStrategy`. The CHOICE activity runs a handler that evaluates conditions against the payload and returns a branch selector. The strategy matches the selector to a branch and performs a non-linear sequence jump.

### Approach: Static Pre-Allocation

All possible branches are assigned sequence numbers at definition time via `buildSequenceMap`. The CHOICE strategy jumps to the selected branch's start sequence. Unchosen branches are never instantiated (their sequence numbers are unused).

This preserves the CAS model — `casAdvanceWithHandle` already supports non-linear jumps since `nextSeq` is a parameter.

**Alternative considered (Kestra-style dynamic resolution):** Each phase type resolves children at runtime with no global sequence map. More flexible but destroys the CAS model and requires a full rewrite. Rejected in favor of static allocation which handles 95%+ of real workflows.

### New PhaseType

```kotlin
enum class PhaseType { LINEAR, SCATTER, PARALLEL, CHOICE }
```

### Model

```kotlin
data class ActivityDefinition(
    // ... existing fields ...
    val choice: ChoiceDefinition? = null,
) {
    init {
        require((fanOut == null) || (choice == null)) {
            "Activity cannot have both fanOut and choice"
        }
    }
}

data class ChoiceDefinition(
    val branches: List<BranchDefinition>,
    val defaultBranch: String? = null,
)

data class BranchDefinition(
    val name: String,
    val condition: ChoiceCondition,
    val activities: List<ActivityDefinition>,   // can contain fanOut, even nested choice
)
```

### Condition Evaluation

Payload-based predicates using JSON pointer paths:

```kotlin
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = ChoiceCondition.Equals::class, name = "EQUALS"),
    JsonSubTypes.Type(value = ChoiceCondition.In::class, name = "IN"),
    JsonSubTypes.Type(value = ChoiceCondition.Exists::class, name = "EXISTS"),
    JsonSubTypes.Type(value = ChoiceCondition.And::class, name = "AND"),
    JsonSubTypes.Type(value = ChoiceCondition.Or::class, name = "OR"),
)
sealed interface ChoiceCondition {
    data class Equals(val path: String, val value: String) : ChoiceCondition
    data class In(val path: String, val values: List<String>) : ChoiceCondition
    data class Exists(val path: String) : ChoiceCondition
    data class And(val conditions: List<ChoiceCondition>) : ChoiceCondition
    data class Or(val conditions: List<ChoiceCondition>) : ChoiceCondition
}
```

No external dependency. The evaluator is a simple recursive function:

```kotlin
fun evaluate(condition: ChoiceCondition, payload: JsonNode): Boolean = when (condition) {
    is Equals -> payload.at(condition.path)?.asText() == condition.value
    is In -> payload.at(condition.path)?.asText() in condition.values
    is Exists -> !payload.at(condition.path).isMissingOrNull()
    is And -> condition.conditions.all { evaluate(it, payload) }
    is Or -> condition.conditions.any { evaluate(it, payload) }
}
```

### DSL

```kotlin
workflow {
    activity("ingest") { transition("file.ingest") }

    choice("route") {
        branch("csv") {
            condition { path("$.type") eq "csv" }
            activity("parse") { transition("csv.parse") }
        }
        branch("pdf") {
            condition { path("$.type") eq "pdf" }
            activity("extract") { transition("pdf.extract") }
            activity("ocr") { transition("pdf.ocr") }
        }
        branch("image") {
            condition {
                or {
                    path("$.type") eq "png"
                    path("$.type") eq "jpg"
                }
            }
            activity("resize") { transition("image.resize") }
        }
        defaultBranch("csv")
    }

    activity("finalize") { transition("file.finalize") }
}
```

The CHOICE activity uses a built-in `_choice.evaluate` passthrough handler (registered automatically). The worker claims and completes it instantly, then the `ChoicePhaseStrategy` evaluates the `ChoiceCondition` against the payload to select the branch. No user-written handler is needed for the routing decision itself.

### Sequence Allocation

```
Seq 1: LINEAR   "ingest"              nextSeq=2
Seq 2: CHOICE   "route"               nextSeq=null (strategy decides)
                 branchSequences: {"csv": 3, "pdf": 4, "image": 6}
Seq 3: LINEAR   "parse"     (csv)     nextSeq=7   <- convergence
Seq 4: LINEAR   "extract"   (pdf)     nextSeq=5
Seq 5: LINEAR   "ocr"       (pdf)     nextSeq=7   <- convergence
Seq 6: LINEAR   "resize"    (image)   nextSeq=7   <- convergence
Seq 7: LINEAR   "finalize"            nextSeq=null (end)
```

`buildSequenceMap` handles CHOICE by:

1. Assigning the CHOICE sequence number
2. Walking each branch's activities (recursing for fanOut within branches)
3. Recording each branch's start sequence in `branchSequences`
4. Computing the convergence point (next sequence after all branches)
5. Patching each branch's last activity: `nextSequence = convergenceSeq`

`SequenceInfo` carries the branch map:

```kotlin
data class SequenceInfo(
    val sequenceNumber: Int,
    val activityIndex: Int,
    val activity: ActivityDefinition,
    val phaseType: PhaseType,
    val nextSequence: Int?,
    val branchSequences: Map<String, Int>? = null,   // CHOICE only
)
```

### ChoicePhaseStrategy

```kotlin
class ChoicePhaseStrategy(
    private val objectMapper: ObjectMapper,
) : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val choiceDef = context.currentSeqInfo.activity.choice!!
        val branchSeqs = context.currentSeqInfo.branchSequences!!

        val payload = context.tasks.firstOrNull()?.resultJson
            ?: return AdvancementDecision.Abort("CHOICE has no input payload")
        val payloadNode = objectMapper.readTree(payload)

        // Evaluate branches in order -- first match wins
        val matchedBranch = choiceDef.branches.firstOrNull { branch ->
            evaluate(branch.condition, payloadNode)
        }

        val branchName = matchedBranch?.name
            ?: choiceDef.defaultBranch
            ?: return AdvancementDecision.Abort("No branch matched and no default defined")

        val targetSeq = branchSeqs[branchName]
            ?: return AdvancementDecision.Abort("Branch '$branchName' not in sequence map")

        val targetSeqInfo = context.sequenceMap[targetSeq]!!
        val task = createTaskForActivity(
            workflowId = context.workflow.id,
            sequenceNumber = targetSeq,
            activity = targetSeqInfo.activity,
            payload = payload,
            now = Instant.now().truncatedTo(ChronoUnit.MICROS),
        )

        return AdvancementDecision.Advance(targetSeq, listOf(task))
    }
}
```

### PARALLEL + CHOICE Combo

After a PARALLEL join, the current engine propagates `null` as the next payload. This blocks CHOICE evaluation.

**Fix (part of barrier refactoring):** `ParallelPhaseStrategy` collects completed results into a JSON array:

```kotlin
val results = context.tasks
    .filter { it.status == TaskStatus.COMPLETED }
    .mapNotNull { it.resultJson }
val aggregatedPayload = objectMapper.writeValueAsString(results)
```

Now CHOICE after PARALLEL receives `["result1", "result2", ...]` and can evaluate conditions against the aggregated output.

**Sequence map example (SCATTER -> PARALLEL -> CHOICE):**

```
Seq 1: SCATTER   "split-data"          nextSeq=2
Seq 2: PARALLEL  "split-data" (join)   nextSeq=3
Seq 3: CHOICE    "route-by-results"    branchSequences: {all-ok: 4, has-errors: 5}
Seq 4: LINEAR    "celebrate"  (all-ok)   nextSeq=6
Seq 5: LINEAR    "remediate"  (errors)   nextSeq=6
Seq 6: LINEAR    "report"               nextSeq=null
```

**Fan-out within a CHOICE branch** is supported. Branch activities with `fanOut` expand into SCATTER + PARALLEL sequences within the branch's allocated range, with the last PARALLEL's `nextSequence` pointing to the convergence point.

**Nested CHOICE (CHOICE inside CHOICE):** Structurally supported by recursive `List<ActivityDefinition>` in `BranchDefinition`. The sequence allocator recurses. Deferred until there is a real use case.

### Sweeper / Recovery

`recoverStuckWorkflow` uses the same strategy pattern. A stuck CHOICE sequence calls `ChoicePhaseStrategy.resolve()` — same logic as `onTaskCompleted`. No special recovery code.

---

## 5. Barrier Service Redesign (Architectural Foundation)

All four enhancements benefit from a redesigned barrier. The current barrier has hardcoded `when (phaseType)` switches and rigid `nextSequence = seq + 1` arithmetic. The redesign extracts phase-specific logic into a strategy pattern and makes sequence progression explicit.

### Part A: Public PhaseType and SequenceInfo

Move from `private` types inside `BarrierService` to a shared file:

```kotlin
// SequenceModel.kt
enum class PhaseType { LINEAR, SCATTER, PARALLEL }

data class SequenceInfo(
    val sequenceNumber: Int,
    val activityIndex: Int,
    val activity: ActivityDefinition,
    val phaseType: PhaseType,
    val nextSequence: Int?,                        // null = last sequence
    val branchSequences: Map<String, Int>? = null,  // reserved for CHOICE
)

fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo>
```

`nextSequence` replaces implicit `seq + 1` arithmetic with explicit data. For current LINEAR workflows, `nextSequence` is `seq + 1` (or `null` for the last). For CHOICE branches, it points to the convergence point. For SCATTER, `nextSequence` points to its PARALLEL join sequence — eliminating the `seq - 1` adjacency assumption in fan-out task creation (the scatter result is available in the strategy's `context.tasks`).

### Part B: PhaseStrategy Interface

```kotlin
interface PhaseStrategy {
    fun resolve(context: PhaseContext): AdvancementDecision
}

data class PhaseContext(
    val handle: Handle,
    val workflow: WorkflowRun,
    val definition: WorkflowDefinition,
    val currentSeqInfo: SequenceInfo,
    val sequenceMap: Map<Int, SequenceInfo>,
    val failedCount: Int,
    val totalCount: Int,
    val tasks: List<Task>,
)

sealed interface AdvancementDecision {
    /** Advance the workflow to the next sequence, inserting the given tasks. */
    data class Advance(val nextSequence: Int, val tasks: List<Task>) : AdvancementDecision
    /** All sequences completed — mark the workflow as COMPLETED. */
    data object Complete : AdvancementDecision
    /**
     * Abort the workflow — mark it as FAILED and cancel pending tasks.
     * Only returned when the phase failed AND [FailurePolicy.ABORT] is set.
     * BEST_EFFORT failures return [Advance] or [Complete] instead.
     */
    data class Abort(val reason: String) : AdvancementDecision
}
```

### Part C: Strategy Implementations

Extracted from existing `when (phaseType)` branches in `evaluateOutcome`, `advanceWorkflow`, and `insertTasksForSequence`.

**Failure policy lives in the strategies, not in `executeDecision`.** Each strategy evaluates its own success/failure condition and checks the activity's `failurePolicy`. When the outcome is a failure:
- `ABORT` → return `Abort(reason)` — `executeDecision` marks the workflow FAILED
- `BEST_EFFORT` → return `Advance` or `Complete` with null payload (treat as success)

This keeps `executeDecision` simple (no task-creation path for BEST_EFFORT) and avoids a structural gap where `Abort` + BEST_EFFORT would need to build tasks that only the strategy knows how to create.

Shared helper to avoid duplication across strategies:

```kotlin
fun PhaseContext.failOrAdvance(failedCount: Int, payload: String?): AdvancementDecision? {
    if (failedCount == 0) return null  // no failure — caller continues to normal advance
    return when (currentSeqInfo.activity.failurePolicy) {
        ABORT -> AdvancementDecision.Abort("$failedCount task(s) failed at sequence ${currentSeqInfo.sequenceNumber}")
        BEST_EFFORT -> advanceOrComplete(payload = null)
    }
}

fun PhaseContext.advanceOrComplete(payload: String?): AdvancementDecision {
    val nextSeq = currentSeqInfo.nextSequence ?: return AdvancementDecision.Complete
    val nextSeqInfo = sequenceMap[nextSeq]!!
    val task = createTaskForActivity(workflow.id, nextSeq, nextSeqInfo.activity, payload, Instant.now().truncatedTo(ChronoUnit.MICROS))
    return AdvancementDecision.Advance(nextSeq, listOf(task))
}
```

**`LinearPhaseStrategy`:**
- Calls `context.failOrAdvance(failedCount, payload)` — returns early on failure (ABORT → `Abort`, BEST_EFFORT → `Advance` with null)
- If `nextSequence == null` → `Complete`
- Else → builds single task for next activity, returns `Advance(nextSequence, tasks)`

**`ScatterPhaseStrategy`:**
- Same `failOrAdvance` check as LINEAR
- If succeeded → reads scatter result from `context.tasks` (the completed scatter task's `resultJson`), deserializes the payload array, looks up the PARALLEL sequence info via `context.sequenceMap[nextSequence]`, and creates fan-out tasks
- Returns `Advance(parallelSeq, fanOutTasks)`

**`ParallelPhaseStrategy`:**
- Evaluates `JoinPolicy` (All / Threshold / Percentage) against failed/total counts
- On failure → calls `failOrAdvance` (ABORT → `Abort`, BEST_EFFORT → `Advance` with null payload)
- On success → collects completed results into JSON array for payload propagation (fix for current `null` payload)
- If `nextSequence == null` → `Complete`
- Else → builds task for next sequence with aggregated payload, returns `Advance`

### Part D: PhaseStrategyRegistry

```kotlin
@ApplicationScoped
class PhaseStrategyRegistry {
    private val strategies = ConcurrentHashMap<PhaseType, PhaseStrategy>()

    init {
        register(PhaseType.LINEAR, LinearPhaseStrategy())
        register(PhaseType.SCATTER, ScatterPhaseStrategy())
        register(PhaseType.PARALLEL, ParallelPhaseStrategy())
    }

    fun register(type: PhaseType, strategy: PhaseStrategy)
    fun resolve(type: PhaseType): PhaseStrategy
}
```

New phases register here. BarrierService never changes.

### Part E: Refactored BarrierService

**Query change:** The current `onTaskCompleted` uses three separate count queries (`countNonTerminal`, `countFailed`, `countTotal`) and never loads task objects. The refactored version must load tasks for `PhaseContext.tasks` because strategies need actual `Task` objects (ScatterPhaseStrategy reads `resultJson`, ParallelPhaseStrategy collects results). Replace the three count queries with a single `findByWorkflowAndSequenceWithHandle` call and compute counts in-memory:

```kotlin
val tasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, sequenceNumber)
val nonTerminal = tasks.count { !it.status.isTerminal }
val failedCount = tasks.count { it.status == TaskStatus.FAILED || it.status == TaskStatus.TIMED_OUT || it.status == TaskStatus.DEAD_LETTER }
val totalCount = tasks.size
```

Net effect: fewer DB round-trips (1 query replaces 3), one slightly larger result set. For LINEAR/SCATTER (1 task), trivial. For large PARALLEL fan-outs, unavoidable since ParallelPhaseStrategy needs the results.

```kotlin
@ApplicationScoped
class BarrierService(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val strategyRegistry: PhaseStrategyRegistry,
) {
    suspend fun onTaskCompleted(...) {
        jdbi.inTransactionSuspend { handle ->
            // 1. Self-update (unchanged)
            // 2. Lock-free probe (unchanged)
            // 3. Load workflow + build sequence map (unchanged)
            // 4. Load tasks for sequence, compute counts in-memory
            // 5. Delegate to strategy
            val strategy = strategyRegistry.resolve(seqInfo.phaseType)
            val context = PhaseContext(handle, workflow, definition, seqInfo, sequenceMap, failedCount, totalCount, tasks)
            val decision = strategy.resolve(context)
            // 6. Execute decision
            executeDecision(handle, workflow, seqInfo, decision)
        }
    }

    private fun executeDecision(handle, workflow, seqInfo, decision) {
        when (decision) {
            is Advance -> {
                val casWon = workflowRepo.casAdvanceWithHandle(
                    handle, workflow.id, seqInfo.sequenceNumber, decision.nextSequence, workflow.version
                )
                if (casWon) taskRepo.insertBatchWithHandle(handle, decision.tasks)
            }
            is Complete -> {
                workflowRepo.updateStatusWithHandle(handle, workflow.id, COMPLETED, RUNNING)
            }
            is Abort -> {
                workflowRepo.updateStatusWithHandle(handle, workflow.id, FAILED, RUNNING)
                taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
            }
        }
    }
}
```

`recoverStuckWorkflow` follows the same pattern: load workflow, build context, delegate to strategy, execute decision.

---

## 6. Kestra / Temporal Comparison

Design decisions were informed by studying Kestra and Temporal's internal architecture.

### Temporal: Event-Sourced Replay

The workflow code IS the state machine. State is reconstructed by replaying an append-only event history through deterministic workflow code. Workers produce Commands (not state mutations); the History Service converts them to Events. The "barrier" for parallel work is implicit in `Promise.allOf()` within user code. No explicit barrier data structure exists on the server.

**Not applicable to this project.** Temporal's model requires event sourcing, deterministic replay, and SDK-intercepted function calls. Adopting it would be a full architectural rewrite.

### Kestra: FlowableTask Strategy

Closer to this project. The Executor is a stateless loop: `Execution + Event -> Execution'`. The key abstraction is `FlowableTask` with:

- `resolveNexts(execution)` — "which tasks should I create next?"
- `resolveState(execution)` — "given all children are terminal, what's my state?"

Each orchestration primitive (Sequential, Parallel, If/Switch, Pause) is a FlowableTask. The graph is resolved **dynamically at runtime** — no pre-computed sequence map. Each flowable knows its own children and resolves them on demand.

### Design Choice: Static Pre-Allocation (vs. Kestra's Dynamic Resolution)

| Dimension | Static (chosen) | Dynamic (Kestra) |
|-----------|-----------------|-------------------|
| Scope of change | Refactor BarrierService, add `nextSequence` | Rewrite BarrierService, TaskRepository, schema, Sweeper |
| CAS model | Preserved — non-linear jumps | Destroyed — need new concurrency control |
| Debugging | "Workflow at sequence 6" | "Active paths: [route.csv.parse-csv]" |
| Nested CHOICE | Recursive sequence allocation | Natural nesting |
| Risk | Low — evolutionary | High — revolutionary rewrite |
| Coverage | 95%+ of real workflows | 100% including exotic nesting |

Static allocation was chosen because it preserves the lock-free CAS model while supporting CHOICE branching. The 5% of exotic nested cases can use child workflows.

---

## 7. Refactoring Plan — Preparing the Foundation

Structural changes to the current codebase that add no features but make the architecture ready to absorb all four enhancements with minimal diff. Each refactoring is independently shippable.

### Refactoring 1: Extract PhaseType and SequenceInfo

**What:** Move `PhaseType`, `SequenceInfo`, and `buildSequenceMap` from private BarrierService internals to a shared `SequenceModel.kt`. Add explicit `nextSequence: Int?` field. Add `branchSequences: Map<String, Int>? = null` (reserved).

**Why:** Makes these types public, testable, and reusable by future strategies.

**Behavior change:** None.

### Refactoring 2: Extract PhaseStrategy Interface and Implementations

**What:** Create `PhaseStrategy` interface, `PhaseContext`, `AdvancementDecision`, and `PhaseStrategyRegistry`. Extract `LinearPhaseStrategy`, `ScatterPhaseStrategy`, `ParallelPhaseStrategy` from existing `when (phaseType)` branches. Refactor `BarrierService` into a coordinator that delegates to strategies.

**Depends on:** Refactoring 1.

**Why:** Eliminates all `when (phaseType)` switches from BarrierService. Adding CHOICE later = one new strategy class + one `registry.register()` call.

**Behavior change:** None — same logic distributed across strategy classes.

**Critical sub-tasks (highest complexity here):**

1. **Dismantle `advanceWorkflow`.** This private method (lines 125–166) contains `nextSeq = currentSeq + 1` arithmetic, failure-policy evaluation, last-sequence detection via `sequenceMap.containsKey(nextSeq)`, and task insertion dispatch. All of this must move into the strategies and the new `executeDecision` coordinator. Specifically:
   - Last-sequence detection → strategy returns `Complete` when `nextSequence == null`
   - Failure-policy evaluation → **moves into each strategy** via shared `failOrAdvance` helper (ABORT → `Abort`, BEST_EFFORT → `Advance`/`Complete` with null payload). `Abort` always means abort — `executeDecision` stays simple with no task-creation path.
   - `insertTasksForSequence` → each strategy builds its own task list and returns it in `Advance.tasks`

2. **Dismantle `recoverStuckWorkflow`.** This method (lines 82–123) has its own `seq + 1` at line 105 and a `when (phaseType)` payload-resolution switch at lines 113–119. It must delegate to the same strategy `resolve()` call as `onTaskCompleted`. After refactoring, both paths become: load workflow → build context → `strategy.resolve()` → `executeDecision`.

3. **Move fan-out task creation into `ScatterPhaseStrategy`.** Currently `insertTasksForSequence` reads the scatter result from `sequenceNumber - 1` (line 191). After refactoring, `ScatterPhaseStrategy.resolve()` already has the scatter result in `context.tasks` (completed scatter task). It reads `resultJson`, deserializes the payload array, looks up the PARALLEL sequence info via `context.sequenceMap[nextSequence]`, and creates fan-out tasks directly. No `seq - 1` adjacency assumption survives.

### Refactoring 3: Fix PARALLEL Payload Propagation

**What:** In `ParallelPhaseStrategy`, collect completed task results into a JSON array instead of propagating `null`.

**Depends on:** Refactoring 2 (done inside ParallelPhaseStrategy).

**Why:** Independently valuable bug fix. Activities after fan-out joins currently lose all context. Also unblocks CHOICE-after-PARALLEL.

**Behavior change:** Activities after a PARALLEL join now receive a JSON array of completed results instead of `null`.

### Refactoring 4: Verify CAS Supports Non-Linear Jumps

**What:** Audit `casAdvanceWithHandle` SQL to confirm no hidden `seq + 1` assumption. The method signature already accepts arbitrary `nextSeq`.

**Depends on:** Nothing.

**Behavior change:** None (verification only).

### Refactoring 5: Add `queue_name` Column

**What:** V5 migration adding `queue_name` column + index. Add field to `Task`, `ActivityDefinition`, `FanOutDefinition`. Add `queueName` parameter to `claimNext` (default `"default"`). Add `queue` to `WorkerConfig`.

**Depends on:** Nothing.

**Why:** Nearly zero risk, fully backward compatible. Immediately useful.

### Refactoring 6: Add `WAITING_FOR_SIGNAL` Status

**What:** Update CHECK constraint. Add status to `TaskStatus` enum (non-terminal). Add transitions. No WorkerLoop changes, no signal endpoint, no notification.

**Depends on:** Nothing.

**Why:** The status must exist in schema + model before any signal feature can use it.

### Dependency Graph

```
Refactoring 1 (extract types)
    |
    v
Refactoring 2 (strategy pattern)
    |
    v
Refactoring 3 (PARALLEL payload)

Refactoring 4 (CAS verification)       -- independent
Refactoring 5 (queue_name)             -- independent
Refactoring 6 (WAITING_FOR_SIGNAL)     -- independent
```

1 -> 2 -> 3 is the critical chain. 4, 5, 6 are independent and can parallelize.

### After Refactoring, Each Enhancement Becomes:

| Enhancement | Remaining work |
|---|---|
| **Task Queues** | DSL builder + pass `queueName` through WorkerLoop |
| **Async Signals** | `HandlerOutput.suspend` + WorkerLoop check + `SignalResource` + `NotificationDispatcher` + sweeper timeout query |
| **Declarative Workflows** | New table + repos + REST endpoints (fully decoupled) |
| **Dynamic Branching** | `ChoiceDefinition` model + `ChoiceCondition` evaluator + `ChoicePhaseStrategy` + DSL builders + `buildSequenceMap` CHOICE allocation |
