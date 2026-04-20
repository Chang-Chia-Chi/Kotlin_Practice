# Lock-Free Workflow Engine

A Kotlin/Quarkus workflow engine that eliminates row-lock contention by replacing mutable counters with MVCC-based barrier detection and a two-transaction commit design. Workers wake instantly via event-driven dispatch. Backed by Oracle, deployed on Kubernetes.

`Kotlin` · `Quarkus` · `JDBI` · `Oracle` · `Kubernetes`

---

## Architecture

### Declarative DSL

A type-safe Kotlin DSL produces immutable `WorkflowDefinition` data classes — pure data, zero behaviour, JSON-serializable. `@DslMarker` prevents scope leakage; build-phase validation enforces required fields and mutual-exclusion rules (e.g. `on()` and `next()` cannot coexist on the same activity).

**Linear pipeline** — activities execute in sequence, one task each:

```kotlin
val etl = workflow {
    activity("extract") { transition("etl.extract") }
    activity("transform") { transition("etl.transform") }
    activity("load") { transition("etl.load") }
}
```

**Conditional branching** — activity result carries a `"branch"` field; `on()` routes to different successors based on its value:

```kotlin
val payment = workflow {
    activity("validate") {
        transition("payment.validate")
        on("OK")      { next("charge") }
        on("INVALID") { next("reject") }
    }
    activity("charge") { transition("payment.charge") }
    activity("reject") { transition("payment.reject") }
}
```

The handler completing `validate` must return `{"branch": "OK"}` or `{"branch": "INVALID"}`. The engine extracts the `"branch"` key from `resultJson` and evaluates each `Edge` label at routing time. Non-taken branches receive `SKIPPED` tasks and the BFS propagates through them in the same transaction.

**Fan-out** — scatter activity produces N payloads; engine creates N parallel tasks for the named handler. After all parallel tasks complete, the next activity can pull resolved inputs from prior results:

```kotlin
val pipeline = workflow {
    deadline(Duration.ofHours(2))
    activity("split") {
        transition("batch.prepare")
        retries(3)
        fanOut { transition("batch.execute"); retries(2) }
        next("report")
    }
    activity("report") {
        transition("batch.report")
        inputs { "batchId" from "split.batchId" }
    }
}
```

`split` returns a JSON array. The engine fans out one `batch.execute` task per item. Once all parallel tasks complete, `report` runs with `batchId` resolved from `split`'s result. Fan-out config (transition, retries, deadline, backoff, queue) lives inside the `fanOut {}` block.

**Queue routing & backoff:**

```kotlin
activity("fetch") {
    transition("crawl.fetch")
    queue("io-bound")
    retries(5)
    backoffBase(Duration.ofSeconds(2))
    backoffCap(Duration.ofMinutes(5))
    fanOut { transition("crawl.process"); queue("cpu-bound") }
    next("aggregate")
}
```

### Engine Core

Each activity compiles to one or two sequence numbers via `buildSequenceMap` (topological sort):

| Activity type | Phases assigned |
|---|---|
| Normal | `LINEAR` (seq N) |
| Fan-out activity | `SCATTER` (seq N) + `PARALLEL` (seq N+1) |

`PhaseDecision` is the routing discriminant:

- **`Abort`** — task failed, or SCATTER result was empty/missing → workflow → `FAILED`
- **`ScatterExpand`** — SCATTER succeeded → insert N `PARALLEL` tasks
- **`Normal`** — fall through to successor BFS

**Two-transaction design** (`DefaultPhaseGate`):

- **TX1** commits the task status + `resultJson` so it is visible to all concurrent readers under READ COMMITTED.
- **TX2** runs a fast-path non-terminal count (no lock). If all tasks at the sequence are terminal, it acquires a `SELECT FOR UPDATE` on the workflow row, recounts under lock, builds a `GateSnapshot`, and delegates to pure routing functions.

**BFS successor dispatch** (`DagRouter.bfsDispatch`) uses Kahn's indegree algorithm:

1. Seed from the just-completed sequence's direct successors.
2. A successor is dequeued only when all its predecessor sequences are resolved (terminal in DB or decided-SKIPPED in this loop).
3. `isAnyEdgeTaken` checks `resultBranch == edgeLabel` (or `DEFAULT_BRANCH` for unconditional edges).
4. Taken → insert real `Task`. Not taken → insert `SKIPPED` task, mark resolved, enqueue *its* successors.

All inserts are batched into a single `insertBatch` call at the end of TX2.

**`ActivityInputResolver`** resolves `inputs {}` declarations at handler execution time. Linear/SCATTER sources return a single field value; PARALLEL sources aggregate into a JSON array across all completed parallel tasks.

**Two-Table Model:**

- **`workflow`** — one mutable row per execution. Columns: `id`, `definition_json` (CLOB), `version`, `status`, `deadline_at`, timestamps.
- **`task`** — queue rows claimed via `SELECT FOR UPDATE SKIP LOCKED`. Columns include `result_json`, `fan_out_payloads_json`, `retry_count`, `not_before` (exponential backoff), `enqueued_at` (FIFO ordering), `sequence_number`, `queue_name`.

### Worker Layer

Workers use event-driven dispatch. The pipeline:

```
indefinitelyRepeat(Unit)
  .takeUntilSignal(stopChannel)
  .unorderedMapAsync(concurrency) { pollAndProcess(...) }
  .collect {}
```

On each tick the loop rotates through configured queues (round-robin) and claims tasks via `SELECT FOR UPDATE SKIP LOCKED`. If no tasks are found it suspends on `WorkerNotifier.awaitWork()` until signaled or the fallback poll interval expires.

Three signal sources:

- **Local signal** — emitted after task insertion, then broadcasts to all peer pods via HTTP POST.
- **Remote signal** — `WorkerNotifyResource` receives the HTTP broadcast and emits to the local flow (no re-broadcast).
- **Fallback probe** — `awaitWork()` times out after `fallback-poll-interval` (default 5s).

Peer discovery uses a Kubernetes Endpoints Watch (`PeerRegistry`) for real-time pod list updates. Outside Kubernetes, signaling stays in-process.

**Correctness invariant:** Signals are performance hints, never correctness requirements. Removing the notification layer degrades to fallback-poll mode but never affects claiming or routing.

### Resilience

**Leader sweeper.** A K8s-lease-elected leader runs `WorkflowWatchdog`, which periodically calls `recoverStuckWorkflow`. Recovery seeds the indegree-BFS from every resolved sequence — the same pure routing logic as normal completion — making it inherently idempotent.

**Graceful shutdown.** `ShutdownCoordinator` orchestrates teardown in order: leader (order 1) stops sweeper patrols first, then workers (order 10) drain in-flight tasks, all within a configurable global timeout.

**Health probes.** `WorkerLoopHealthCheck` (worker activity freshness) and `LeaderHealthCheck` (leader heartbeat freshness) serve as Kubernetes liveness probes.

---

## Project Structure

```
src/main/kotlin/
  dispatch/                   Category-based dispatch algorithm (DSL, models, use cases)
  infrastructure/
    coroutine/                Flow utilities (indefinitelyRepeat, takeUntilSignal, unorderedMapAsync)
    leader/                   K8s Lease-based leader election + health check
    persistence/              JDBI extensions
    queryexporter/            Config-driven SQL → Prometheus metric exporter
    shutdown/                 ShutdownCoordinator + ShutdownParticipant SPI
  worker/
    adapter/http/             PeerRegistry, HttpWorkerNotifier, WorkerNotifyResource
    adapter/trigger/          K8sJobTriggerDriver
    usecase/service/
      execution/              WorkerLoop, HandlerRegistry, MeteredTransitionHandler
      trigger/                TriggerLoop
      TaskSettler
  workflow/
    dsl/                      WorkflowDslBuilders (workflow {}, activity {}, on {}, fanOut {})
    model/                    WorkflowDefinition, ActivityDefinition, Edge, SequenceModel,
                                Task, TaskCompletionEvent, TaskStatus, PhaseType
    usecase/service/orchestration/
                              WorkflowEngine, DefaultPhaseGate, DagRouter,
                                ActivityInputResolver, DefinitionCache, WorkflowWatchdog
```

---

## Getting Started

**Prerequisites:** JDK 21, Docker Desktop (for Testcontainers), Maven 3.9+

```bash
# Build
mvn package

# Run tests (requires Docker for Oracle Testcontainer)
mvn test

# Dev mode
mvn quarkus:dev
```

---

## Configuration

All properties are under the `framework.*` prefix in `application.properties`.

| Group | Property | Default | Description |
|-------|----------|---------|-------------|
| worker | `fallback-poll-interval` | 5s | Poll probe frequency when no dispatch signal received |
| worker | `concurrency` | 4 | Max concurrent handler executions |
| worker | `max-batch-size` | 16 | Upper bound on adaptive batch sizing |
| worker | `pod-ip` | localhost | This pod's IP (excluded from peer broadcast) |
| sweeper | `interval` | 30s | Patrol frequency (leader-only) |
| sweeper | `grace-period` | 2m | Stuck workflow detection threshold |
| sweeper | `stale-task-threshold` | 10m | Stale PROCESSING task reclaim age |
| leader-election | `lease-duration` | 15s | K8s Lease hold time |
| leader-election | `health-threshold` | 45s | Leader liveness probe staleness cutoff |
| shutdown | `global-timeout` | 30s | Total graceful shutdown budget |

---

## Documentation

- **[Feature Specs](docs/superpowers/specs/)** — design specs for cancel/timeout, metrics, dead-letter replay, engine enhancements, and more.
