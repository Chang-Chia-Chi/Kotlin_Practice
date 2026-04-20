# Deferrable Triggers — Design Spec

**Date:** 2026-04-03  
**Status:** Approved  
**Context:** Extends the workflow engine to support long-running external tasks (K8s Jobs, SQL execution) without consuming worker slots, using an Airflow-inspired Deferrable + Trigger Loop model.

---

## 1. Problem Statement

The current engine requires every task to execute within a `TransitionHandler.execute()` suspend function. The handler holds a worker coroutine slot for the entire duration. This works for in-process logic but breaks down for:

- **K8s Jobs** — may run for hours. Holding a coroutine slot blocks other tasks from claiming that concurrency slot.
- **Long-running SQL** — ETL procedures or heavy queries against external databases similarly saturate the worker pool.

With `concurrency=8`, eight long-running external tasks would fully saturate the worker, blocking all other workflow tasks.

### Why not poll in the handler?

A handler that loops with `delay()` between K8s API checks holds the coroutine slot for the entire Job duration. This is Airflow's old `mode="poke"` — doesn't scale.

### Why not a simple sweeper?

A sweep loop that executes SQL itself would stall on slow queries, degrading all other DEFERRED task monitoring. Per-Job K8s API polling at scale (500 tasks × 10s interval = 3000 req/min) hits API server rate limits.

---

## 2. Core Design Decisions

### 2.1 Airflow Deferrable Operator pattern

Adopted from Airflow 2.2+. The handler runs on a worker, decides it needs to wait, returns a `Defer` result. Worker slot freed immediately. A separate **Trigger Loop** (leader-only) monitors external conditions asynchronously. When the condition is met, the task is settled through the normal phase gate.

**Rejected alternatives:**
- `mode="reschedule"` — task bounces between PENDING → PROCESSING → check → re-PENDING. Consumes brief worker slots per check. Imprecise polling interval tied to claim latency.
- External callback/webhook — requires the K8s Job to know about the engine's API. Contract burden on every Job.

### 2.2 Pluggable TriggerDriver SPI

The Trigger Loop delegates to `TriggerDriver` implementations (CDI beans). Adding a new trigger type = new CDI bean, zero Trigger Loop changes. Avoids a growing god-class.

### 2.3 K8s Job + SQL at launch

Two trigger types cover the two major patterns: long-running external process (K8s Job) and long-running query (SQL). Both exercise the full TriggerDriver lifecycle. Additional types (HTTP poll, message queue wait) are straightforward to add later.

### 2.4 Leader-only Trigger Loop

Gated by `LeaderGuard` (same pattern as `WorkflowWatchdog`). The Trigger Loop is lightweight — one K8s Watch connection per namespace, one bounded SQL thread pool. Leader failover means a brief gap in monitoring (seconds), same recovery semantics as the watchdog.

### 2.5 Handler creates K8s Job, Trigger Loop watches

The handler owns Job creation (business logic: image, args, volumes, secrets). The Trigger Loop only monitors completion. Clean separation — handler knows the domain, Trigger Loop knows the infrastructure.

### 2.6 Named datasource registry via existing DataSourceProvider

`SqlExecTriggerDriver` uses the existing `DataSourceProvider.resolve(name)` to get datasources. Named datasources are configured via Quarkus Agroal in `application.properties` with env var expansion. No new registry component needed.

### 2.7 Two-layer retry

- **Trigger-level** (transient): Watch disconnects, SQL connection refused. Handled internally by the driver. Invisible to the workflow.
- **Task-level** (execution failure): K8s Job exits non-zero, SQL procedure error. Flows through existing `retryCount/maxRetries` on Task. Handler re-runs from scratch on retry.

---

## 3. New & Changed Types

### 3.1 `HandlerResult` (new — replaces `HandlerOutput`)

```kotlin
sealed interface HandlerResult {
    data class Completed(val result: String?) : HandlerResult
    data class Defer(
        val triggerType: String,
        val triggerMeta: String,
    ) : HandlerResult
}
```

`HandlerOutput` is removed. All existing handlers change `HandlerOutput(result)` → `HandlerResult.Completed(result)`.

### 3.2 `TaskStatus` (changed)

```kotlin
enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED,
    TIMED_OUT, DEAD_LETTER, CANCELLED, SKIPPED,
    DEFERRED,   // NEW: waiting for external trigger
}
```

`DEFERRED` is **not terminal**. The barrier probe correctly treats it as in-flight.

### 3.3 `Task` (changed)

```kotlin
data class Task(
    // ... existing fields ...
    val triggerType: String? = null,
    val triggerMeta: String? = null,
)
```

DB columns: `trigger_type VARCHAR2(50)`, `trigger_meta CLOB`. Both nullable.

### 3.4 `TriggerDriver` (new — SPI)

```kotlin
interface TriggerDriver {
    fun type(): String
    suspend fun start(tasks: List<DeferredTaskRef>)
    suspend fun poll(): List<TriggerResult>
    suspend fun cancel(taskId: String)
    suspend fun close()
}

data class DeferredTaskRef(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val triggerMeta: String,
    val deadlineAt: Instant?,
)

sealed interface TriggerResult {
    val taskId: String
    data class Succeeded(override val taskId: String, val result: String?) : TriggerResult
    data class Failed(override val taskId: String, val reason: String) : TriggerResult
}
```

**Lifecycle contract:**
- `start(tasks)` — called each sweep cycle with the **full** set of DEFERRED tasks for this driver's type. Driver diffs internally against its tracked set.
- `poll()` — returns results since last call. Non-blocking.
- `cancel(taskId)` — best-effort cleanup (delete K8s Job, cancel SQL coroutine).
- `close()` — shutdown cleanup.

### 3.5 `TriggerTypes` and helper functions (new)

```kotlin
object TriggerTypes {
    const val K8S_JOB = "k8s-job"
    const val SQL_EXEC = "sql-exec"
}

fun deferK8sJob(jobName: String, namespace: String): HandlerResult.Defer

fun deferSqlExec(
    objectMapper: ObjectMapper,
    datasource: String,
    sql: String,
    params: Map<String, Any?> = emptyMap(),
): HandlerResult.Defer
```

### 3.6 `TriggerLoopConfig` (new)

```kotlin
@ConfigMapping(prefix = "trigger.loop")
interface TriggerLoopConfig {
    @WithDefault("5s")
    fun sweepInterval(): Duration

    @WithDefault("5")
    fun sqlMaxConcurrent(): Int
}
```

---

## 4. TriggerLoop Component

### 4.1 Structure

```kotlin
@ApplicationScoped
class TriggerLoop(
    private val taskRepo: TaskRepository,
    private val drivers: Instance<TriggerDriver>,
    private val phaseGate: DefaultPhaseGate,
    private val leaderGuard: LeaderGuard,
    private val meterRegistry: MeterRegistry,
    private val triggerLoopConfig: TriggerLoopConfig,
) : ShutdownParticipant
```

### 4.2 Main loop

```
every sweepInterval (e.g., 5s):
  if not leader → skip

  1. LOAD: query all DEFERRED tasks from DB
  2. DISPATCH: group by triggerType, call driver.start(tasks) for each driver
  3. POLL: call driver.poll() for each driver
  4. SETTLE: for each TriggerResult:
       Succeeded → phaseGate.onTaskCompleted(status = COMPLETED, resultJson = result)
       Failed    → taskRepo.resetForRetry() if retries remain,
                   else phaseGate.onTaskCompleted(status = FAILED)
  5. TIMEOUT: for each task past deadlineAt:
       → driver.cancel(taskId)
       → phaseGate.onTaskCompleted(status = TIMED_OUT)
```

Stateless across iterations — leader failover picks up where the old leader left off.

### 4.3 Shutdown

```kotlin
override val shutdownOrder: Int = SHUTDOWN_ORDER_TRIGGER  // 5 (between leader=1 and worker=10)
```

On shutdown: stop sweep loop → `driver.close()` for all drivers → in-flight SQL gets grace period. DEFERRED tasks remain DEFERRED in DB for next leader.

### 4.4 Metrics

| Metric | Type | Description |
|---|---|---|
| `trigger_deferred_tasks` | Gauge | Current DEFERRED count by trigger type |
| `trigger_poll_total` | Counter | Sweep cycles executed |
| `trigger_settled_total` | Counter | Tasks settled by type + outcome |
| `trigger_sweep_duration_seconds` | Timer | Time per sweep cycle |

---

## 5. TriggerDriver Implementations

### 5.1 `K8sJobTriggerDriver`

```kotlin
@ApplicationScoped
class K8sJobTriggerDriver(
    private val kubernetesClient: KubernetesClient,
) : TriggerDriver {
    override fun type(): String = "k8s-job"
}
```

**triggerMeta JSON:** `{"jobName": "...", "namespace": "..."}`

**Mechanics:**
- `start()` — for new tasks, starts a Watch on the Job via label selector `workflow.engine/task-id`. Reuses existing namespace-scoped Watches.
- `poll()` — drains `ConcurrentLinkedQueue<TriggerResult>` populated by Watch callbacks. O(1).
- Watch reconnection handled by Fabric8 built-in retry (trigger-level transient retry).
- Job terminal condition `Complete` → `Succeeded`. Job condition `Failed` → `Failed`.
- **Result extraction convention:** K8s Job writes output to ConfigMap `{jobName}-output`. Driver reads on success. Absent ConfigMap → `result = null`.
- `cancel()` — deletes Job with `propagationPolicy = Background`.

**Scale:** One Watch connection per namespace. Thousands of concurrent Jobs.

### 5.2 `SqlExecTriggerDriver`

```kotlin
@ApplicationScoped
class SqlExecTriggerDriver(
    private val dataSourceProvider: DataSourceProvider,
) : TriggerDriver {
    override fun type(): String = "sql-exec"
}
```

**triggerMeta JSON:** `{"datasource": "warehouse", "sql": "CALL run_etl(:taskId)", "params": {"taskId": "..."}}`

**Mechanics:**
- `start()` — for new tasks, submits SQL to bounded thread pool (`Dispatchers.IO.limitedParallelism(sqlMaxConcurrent)`).
- `poll()` — checks completed coroutine jobs, collects results. Rows serialized as JSON.
- Transient retry: connection failures retry with backoff (1s, 2s, 4s), max 3 attempts.
- `cancel()` — cancels the coroutine Job.

**Scale:** Bounded by `sqlMaxConcurrent` (default 5). Protects target database.

---

## 6. WorkerLoop Integration

### 6.1 `processTask()` change

```kotlin
when (val result = handler.execute(input)) {
    is HandlerResult.Completed -> {
        phaseGate.onTaskCompleted(
            taskId = task.id,
            status = TaskStatus.COMPLETED,
            resultJson = result.result,
            // ... existing params
        )
    }
    is HandlerResult.Defer -> {
        taskRepo.defer(
            taskId = task.id,
            triggerType = result.triggerType,
            triggerMeta = result.triggerMeta,
        )
    }
}
```

### 6.2 TaskRepository additions

```kotlin
suspend fun findDeferred(): List<DeferredTaskRef>
suspend fun defer(taskId: String, triggerType: String, triggerMeta: String)
```

`defer()` SQL:
```sql
UPDATE task
SET status = 'DEFERRED', trigger_type = :triggerType, trigger_meta = :triggerMeta
WHERE id = :taskId AND status = 'PROCESSING'
```

---

## 7. Error Handling

### 7.1 Two-layer retry matrix

| Layer | Trigger | Example | Behavior | Limit |
|---|---|---|---|---|
| Trigger-level | K8s Job | Watch disconnects, API 503 | Fabric8 auto-reconnects. Invisible to workflow. | Unlimited |
| Trigger-level | SQL Exec | Connection refused | Backoff retry (1s, 2s, 4s). | 3 attempts |
| Task-level | K8s Job | Job exits non-zero | `resetForRetry()` → handler re-runs, creates new Job | `maxRetries` |
| Task-level | SQL Exec | Procedure error | Same path | `maxRetries` |
| Exhausted | Both | All retries spent | `phaseGate.onTaskCompleted(FAILED)` → failure policy | — |

### 7.2 Deadline enforcement

TriggerLoop checks `deadlineAt` each sweep. Past deadline → `driver.cancel()` + `phaseGate.onTaskCompleted(TIMED_OUT)`. WorkflowWatchdog does NOT handle DEFERRED tasks.

### 7.3 Workflow cancellation

`cancelPendingTasksWithHandle` query broadened:
```sql
UPDATE task SET status = 'CANCELLED'
WHERE workflow_id = :workflowId AND status IN ('PENDING', 'DEFERRED')
```

Next sweep: driver diffs, drops cancelled tasks, cleans up (delete Job, cancel SQL).

### 7.4 Watchdog interaction

No change. DEFERRED is non-terminal → counted as in-flight → workflow not flagged as stuck.

### 7.5 Dead-letter replay

No interaction. DEFERRED tasks are not DEAD_LETTER. If a deferred task exhausts retries → DEAD_LETTER → replay resets to PENDING → handler re-runs from scratch.

---

## 8. DSL Ergonomics

### 8.1 No workflow DSL changes

Deferrable behavior is handler-level. The workflow definition is unchanged:

```kotlin
val mlPipeline = workflow {
    start("prepare-data")
    activity("prepare-data") {
        transition("PrepareDataHandler")
        next("train")
    }
    activity("train") {
        transition("TrainingJobHandler")  // defers internally
        deadline(Duration.ofHours(4))
        retries(2)
        next("evaluate")
    }
    activity("evaluate") {
        transition("EvaluateHandler")
    }
}
```

### 8.2 Handler examples

**K8s Job handler:**
```kotlin
override suspend fun execute(input: HandlerInput): HandlerResult {
    val jobName = "training-${input.taskId}"
    kubernetesClient.batch().v1().jobs()
        .inNamespace("ml-jobs")
        .resource(buildTrainingJob(jobName, input))
        .create()
    return deferK8sJob(jobName, "ml-jobs")
}
```

**SQL handler:**
```kotlin
override suspend fun execute(input: HandlerInput): HandlerResult {
    return deferSqlExec(
        datasource = "warehouse",
        sql = "CALL run_daily_etl(:taskId, :batchDate)",
        params = mapOf("taskId" to input.taskId, "batchDate" to "2026-04-03"),
    )
}
```

---

## 9. Testing Strategy

### 9.1 Unit tests — TriggerDriver SPI

1. `K8sJobTriggerDriver.start()` with new tasks → Watch started
2. `K8sJobTriggerDriver.start()` with already-tracked tasks → no duplicate Watch
3. `K8sJobTriggerDriver.poll()` after Job completes → `Succeeded` with ConfigMap result
4. `K8sJobTriggerDriver.poll()` after Job fails → `Failed` with reason
5. `K8sJobTriggerDriver.cancel()` → Job deleted
6. Watch disconnects and reconnects → no `Failed` emitted
7. `SqlExecTriggerDriver.start()` → query submitted to pool
8. `SqlExecTriggerDriver.poll()` after query completes → `Succeeded` with rows JSON
9. `SqlExecTriggerDriver.poll()` after query error → `Failed`
10. SQL transient failure → retried 3 times before `Failed`
11. `sqlMaxConcurrent` respected — excess tasks queued

### 9.2 Unit tests — TriggerLoop

12. Sweep loads DEFERRED tasks, dispatches to correct driver by type
13. `TriggerResult.Succeeded` → `phaseGate.onTaskCompleted(COMPLETED)`
14. `TriggerResult.Failed` with retries remaining → `taskRepo.resetForRetry()`
15. `TriggerResult.Failed` with retries exhausted → `phaseGate.onTaskCompleted(FAILED)`
16. DEFERRED task past `deadlineAt` → `driver.cancel()` + `TIMED_OUT`
17. Not leader → sweep skipped
18. Shutdown → `driver.close()` called for all drivers

### 9.3 Unit tests — HandlerResult contract

19. Handler returns `Completed` → WorkerLoop calls phaseGate (backwards compatibility)
20. Handler returns `Defer` → WorkerLoop calls `taskRepo.defer()`
21. `taskRepo.defer()` only succeeds when status = PROCESSING

### 9.4 Integration tests (Oracle, OracleTestContainer)

22. Handler defers → task row has DEFERRED status + trigger columns populated
23. TriggerLoop settles DEFERRED task → workflow advances to next activity
24. DEFERRED task timeout → TIMED_OUT in DB, workflow follows failure policy
25. `cancelWorkflow()` with DEFERRED tasks → tasks CANCELLED in DB
26. Workflow with mix of normal + deferrable activities → COMPLETED
27. DEFERRED task fails, retries, handler re-defers → eventually COMPLETED
28. Leader failover mid-DEFERRED → new leader picks up, settles task

### 9.5 K8s testing approach

`K8sJobTriggerDriver` unit tests use mock `KubernetesClient` (existing pattern). Watch callbacks simulated by invoking the event handler directly. No real cluster needed.

`SqlExecTriggerDriver` integration tests use `OracleTestContainer` — "warehouse" datasource points to the same Oracle instance with a different schema.

---

## 10. What Changes, What Doesn't

### Changes

| Component | Change |
|---|---|
| `HandlerOutput` | Removed. Replaced by `HandlerResult` sealed interface |
| `TransitionHandler.execute()` | Return type → `HandlerResult` |
| All existing handlers (3) | `HandlerOutput(result)` → `HandlerResult.Completed(result)` |
| `WorkerLoop.processTask()` | `when` on `HandlerResult` — add `Defer` branch |
| `TaskStatus` | Add `DEFERRED` |
| `Task` | Add `triggerType`, `triggerMeta` fields |
| `task` table | Add `trigger_type VARCHAR2(50)`, `trigger_meta CLOB` columns |
| `TaskRepository` | Add `defer()`, `findDeferred()` methods |
| `cancelPendingTasksWithHandle` SQL | Broaden to `IN ('PENDING', 'DEFERRED')` |
| `application.properties` | Add `trigger.loop.*` config keys |

### New components

| Component | Package |
|---|---|
| `HandlerResult` | `worker.usecase.port.inbound.execution` |
| `TriggerDriver`, `DeferredTaskRef`, `TriggerResult` | `worker.usecase.port.inbound.trigger` |
| `TriggerTypes`, `deferK8sJob()`, `deferSqlExec()` | `worker.usecase.port.inbound.trigger` |
| `TriggerLoop` | `worker.usecase.service.trigger` |
| `TriggerLoopConfig` | `worker.config` |
| `K8sJobTriggerDriver` | `worker.adapter.trigger` |
| `SqlExecTriggerDriver` | `worker.adapter.trigger` |

### Unchanged

| Component | Why |
|---|---|
| Phase gate (`onTaskCompleted`) | TriggerLoop calls it the same way WorkerLoop does |
| WorkflowEngine (start/cancel/replay) | Cancel query broadened, but engine logic untouched |
| Workflow DSL | Deferrable is handler-level, invisible to DSL |
| WorkflowWatchdog / sweeper | DEFERRED is non-terminal, correctly counted as in-flight |
| Dead-letter replay | Doesn't interact with DEFERRED |
| Metrics, health probes, shutdown coordinator | TriggerLoop participates via existing `ShutdownParticipant` |
| HandlerRegistry | Deferrable handlers are just `TransitionHandler` |
| `DataSourceProvider` | Reused as-is by `SqlExecTriggerDriver` |
