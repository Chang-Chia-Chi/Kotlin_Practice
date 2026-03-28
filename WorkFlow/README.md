# Lock-Free Workflow Engine

A Kotlin/Quarkus workflow engine that eliminates row-lock contention by replacing mutable counters with MVCC-based barrier detection and optimistic CAS transitions. Backed by Oracle, deployed on Kubernetes.

`Kotlin` · `Quarkus` · `JDBI` · `Oracle` · `Kubernetes`

---

## Architecture

### Dual-Path Progression

Two independent mechanisms guarantee that every workflow phase eventually completes, even under arbitrary node failure.

**Primary path — Lock-free barrier.** Workers derive completion from a read-only aggregate query (MVCC snapshot, zero locks) and advance the workflow via optimistic CAS on a single workflow row. Under normal operation, this path handles 100% of transitions with zero contention during task execution.

**Backup path — Leader sweeper.** A K8s-lease-elected leader polls at low frequency to detect workflows stuck due to worker death between CAS success and transaction commit. The sweeper executes the same CAS + fan-out logic, which is inherently idempotent.

**Key invariant:** Both paths use the same CAS predicate. At most one actor (worker or leader) can ever win the transition for a given phase. This is the single serialization point in the entire design — and it fires at most once per phase, not once per task.

### Declarative DSL

A type-safe Kotlin DSL produces immutable `WorkflowDefinition` data classes — pure data, zero behaviour, JSON-serializable. `@DslMarker` prevents scope leakage; build-phase validation enforces required fields.

```kotlin
val pipeline = workflow {
    deadline(Duration.ofHours(2))
    activity("split") {
        transition("batch.prepare")
        retries(3)
        deadline(Duration.ofMinutes(10))
        fanOut {
            transition("batch.execute")
            retries(2)
            deadline(Duration.ofMinutes(5))
            failurePolicy(FailurePolicy.BEST_EFFORT)
            joinPolicy(JoinPolicy.Percentage(95))
        }
    }
    activity("notify") {
        transition("batch.notify")
    }
}
```

Reading: `split` produces N payloads. The engine fans out N `batch.execute` tasks to the worker pool. After all reach a terminal state, the last worker evaluates JoinPolicy. If >= 95% succeeded, the engine advances to `notify`.

### Two-Table Model

The entire runtime state lives in two tables — no activity table exists.

- **`workflow`** — one mutable row per execution, sole CAS target. Columns: `id`, `definition` (CLOB), `current_sequence`, `version`, `status`, `deadline_at`, timestamps.
- **`task`** — standard queue rows. Each task belongs to a workflow at a specific sequence number. Includes retry/backoff fields, `not_before` for exponential backoff, `enqueued_at` for FIFO ordering.

Sequence expansion: linear activity = 1 sequence, fan-out = 2 (scatter + parallel). The engine is sequence-agnostic — it runs the barrier for whatever `current_sequence` is.

---

## Project Structure

```
src/main/kotlin/
  engine/         BarrierService, Sweeper, WorkflowEngine, repositories, models
  dsl/            WorkflowDefinition data classes + type-safe builders
  worker/         WorkerLoop, HandlerRegistry, TransitionHandler, health check
  leader/         K8s Lease-based leader election + health check
  queryexporter/  Config-driven SQL -> Prometheus metric exporter
  shutdown/       Graceful shutdown coordinator
  config/         FrameworkConfig (SmallRye @ConfigMapping)
  extension/      Coroutine flow utilities (unorderedMapAsync, takeUntilSignal)
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
| worker | `poll-interval` | 1s | Task claim poll frequency |
| worker | `concurrency` | 4 | Max concurrent handler executions |
| worker | `batch-size` | 1 | Tasks per claim cycle |
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
