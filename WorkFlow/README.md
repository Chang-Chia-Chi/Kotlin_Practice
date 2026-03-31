# Lock-Free Workflow Engine

A Kotlin/Quarkus workflow engine that eliminates row-lock contention by replacing mutable counters with MVCC-based barrier detection and optimistic CAS transitions. Workers wake instantly via event-driven dispatch. Backed by Oracle, deployed on Kubernetes.

`Kotlin` · `Quarkus` · `JDBI` · `Oracle` · `Kubernetes`

---

## Architecture

### Declarative DSL

A type-safe Kotlin DSL produces immutable `WorkflowDefinition` data classes — pure data, zero behaviour, JSON-serializable. `@DslMarker` prevents scope leakage; build-phase validation enforces required fields.

**Linear pipeline** — activities execute in sequence, one task each:

```kotlin
val etl = workflow {
    activity("extract") {
        transition("etl.extract")
    }
    activity("transform") {
        transition("etl.transform")
    }
    activity("load") {
        transition("etl.load")
    }
}
```

**Fan-out with inputs** — scatter activity produces N payloads, engine creates N parallel tasks for the target activity. After the join policy passes, the next activity receives resolved inputs from prior activities:

```kotlin
val pipeline = workflow {
    deadline(Duration.ofHours(2))
    activity("split") {
        transition("batch.prepare")
        retries(3)
        deadline(Duration.ofMinutes(10))
        fanOut("process")
    }
    activity("process") {
        transition("batch.execute")
        retries(2)
        deadline(Duration.ofMinutes(5))
        failurePolicy(FailurePolicy.BEST_EFFORT)
        joinPolicy(JoinPolicy.Percentage(95))
    }
    activity("report") {
        transition("batch.report")
        inputs {
            "batchId" from "split.batchId"
        }
    }
}
```

Reading: `split` returns a JSON array. The engine fans out one `batch.execute` task per item to the `process` activity. Once >= 95% succeed, `report` runs with `batchId` resolved from `split`'s result.

**Advanced** — queue routing, custom join policy, exponential backoff:

```kotlin
val advanced = workflow {
    deadline(Duration.ofHours(4))
    activity("discover") {
        transition("crawl.discover")
        queue("io-bound")
        retries(5)
        backoffBase(Duration.ofSeconds(2))
        backoffCap(Duration.ofMinutes(5))
        fanOut("process")
    }
    activity("process") {
        transition("crawl.process")
        queue("cpu-bound")
        retries(3)
        deadline(Duration.ofMinutes(15))
        joinPolicy(JoinPolicy.Threshold(10))
        failurePolicy(FailurePolicy.BEST_EFFORT)
    }
    activity("aggregate") {
        transition("crawl.aggregate")
        inputs {
            "results" from "process.output"
            "config" from "discover.crawlConfig"
        }
    }
}
```

Fan-out is an independent, named activity — not a nested block. `joinPolicy` and `failurePolicy` live on the fan-out target. `inputs {}` declares cross-activity data passing: the `ActivityInputResolver` determines single-value vs aggregate resolution from the workflow definition (linear activity = single task result, parallel activity = array of results).

### Engine Core

The engine is sequence-driven. Each activity maps to a sequence number; fan-out targets are `PARALLEL` phases, everything else is `LINEAR`. The `AdvancementStrategyRegistry` dispatches to the appropriate strategy:

- **`LinearAdvancementStrategy`** — single task per sequence. Advances on completion, aborts on failure (unless `BEST_EFFORT`).
- **`ParallelAdvancementStrategy`** — N tasks per sequence. Evaluates `JoinPolicy` (All, Threshold, or Percentage) against completion counts. Advances only when the policy is satisfied.

Both strategies resolve to an `AdvancementDecision`: `Advance(nextSequence)`, `Complete`, or `Abort(reason)`. The barrier evaluates this after every task completion via a read-only MVCC aggregate query (zero locks during task execution) and advances the workflow via optimistic CAS on the single workflow row. At most one actor wins the CAS per phase — this is the only serialization point in the entire design.

**Two-Table Model.** The entire runtime state lives in two tables:

- **`workflow`** — one mutable row per execution, sole CAS target. Columns: `id`, `definition` (CLOB), `current_sequence`, `version`, `status`, `deadline_at`, timestamps.
- **`task`** — queue rows claimed via `SELECT FOR UPDATE SKIP LOCKED`. Each task belongs to a workflow at a specific sequence number. Includes retry/backoff fields (`not_before` for exponential backoff, `enqueued_at` for FIFO ordering).

### Worker Layer

Workers use event-driven dispatch. Instead of fixed-interval polling, workers suspend on a per-queue `SharedFlow` and wake instantly when signaled. Three signal sources:

- **Local signal** — `DispatchNotifier.signal()` emits to the in-process flow after task insertion, then broadcasts to all peer pods via HTTP POST.
- **Remote signal** — `DispatchNotifyResource` receives the HTTP broadcast and emits to the local flow (no re-broadcast, preventing loops).
- **Fallback probe** — `awaitWork()` times out after `fallback-poll-interval` (default 5s), triggering a poll to catch missed signals.

Peer discovery uses a Kubernetes Endpoints Watch (`PeerRegistry`) for real-time pod list updates. Outside Kubernetes, signaling stays in-process.

**Correctness invariant:** Notifications are performance hints, never correctness requirements. Removing the entire notification layer degrades to fallback-poll mode but never affects task claiming.

### Resilience

**Leader sweeper.** A K8s-lease-elected leader polls at low frequency to detect workflows stuck due to worker death between CAS success and transaction commit. The sweeper executes the same CAS + fan-out logic, which is inherently idempotent. Both workers and the sweeper share the same CAS predicate — at most one actor wins per phase.

**Graceful shutdown.** `ShutdownCoordinator` orchestrates ordered component teardown: leader (order 1) stops sweeper patrols first, then workers (order 10) drain in-flight tasks, all within a configurable global timeout.

**Health probes.** `WorkerLoopHealthCheck` (worker activity freshness) and `LeaderHealthCheck` (leader heartbeat freshness) serve as Kubernetes liveness probes.

---

## Project Structure

```
src/main/kotlin/
  config/         FrameworkConfig (SmallRye @ConfigMapping)
  dsl/            WorkflowDefinition data classes + type-safe builders
  engine/         DefaultPhaseGate, AdvancementStrategyRegistry, ActivityInputResolver,
                    WorkflowWatchdog, WorkflowEngine, repositories, models
  extension/      Coroutine flow utilities (unorderedMapAsync, takeUntilSignal)
  leader/         K8s Lease-based leader election + health check
  queryexporter/  Config-driven SQL → Prometheus metric exporter
  shutdown/       ShutdownCoordinator + ShutdownParticipant interface
  worker/         WorkerLoop, HandlerRegistry, TransitionHandler,
                    DispatchNotifier, PeerRegistry, health check
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
| worker | `batch-size` | 1 | Tasks per claim cycle |
| worker | `max-batch-size` | 16 | Upper bound on adaptive batch sizing |
| worker | `pod-ip` | localhost | This pod's IP (for peer exclusion in dispatch broadcast) |
| sweeper | `interval` | 30s | Patrol frequency (leader-only) |
| sweeper | `grace-period` | 2m | Stuck workflow detection threshold |
| sweeper | `stale-task-threshold` | 10m | Stale PROCESSING task reclaim age |
| leader-election | `lease-duration` | 15s | K8s Lease hold time |
| leader-election | `health-threshold` | 45s | Leader liveness probe staleness cutoff |
| shutdown | `global-timeout` | 30s | Total graceful shutdown budget |

---

## Documentation

- **[Design Document](docs/design.md)** — full algorithm details, data model, state machines, failure propagation, indexing strategy, decision log, and implementer checklist.
- **[Feature Specs](docs/superpowers/specs/)** — design specs for cancel/timeout, metrics, dead-letter replay, engine enhancements, and more.
