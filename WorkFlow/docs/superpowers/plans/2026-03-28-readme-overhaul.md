# README Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 550-line design-document README with a concise project README and relocate the design doc to `docs/design.md` with inaccuracies corrected.

**Architecture:** Two deliverables — a new `README.md` (~120-150 lines) and a corrected `docs/design.md`. No code changes. The new README covers project overview, architecture summary, project structure, getting started, configuration, and doc links. The design doc gets 8 targeted fixes for DSL syntax, data model, state machines, observability, and missing features.

**Tech Stack:** Markdown only. References Kotlin DSL source, `FrameworkConfig.kt`, `WorkflowModels.kt`, `query-exporter.yaml` for accuracy.

---

### Task 1: Write new README.md

**Files:**
- Create: `README.md` (overwrite existing)

**Reference files (read-only, for accuracy):**
- `src/main/kotlin/dsl/WorkflowDslBuilders.kt` — actual DSL API
- `src/main/kotlin/config/FrameworkConfig.kt` — config property defaults
- `src/main/kotlin/engine/WorkflowModels.kt` — status enums

- [ ] **Step 1: Write `README.md`**

Write the following content to `README.md`:

```markdown
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

Reading: `split` produces N payloads. The engine fans out N `batch.execute` tasks to the worker pool. After all reach a terminal state, the last worker evaluates JoinPolicy. If ≥ 95% succeeded, the engine advances to `notify`.

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
  queryexporter/  Config-driven SQL → Prometheus metric exporter
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
```

- [ ] **Step 2: Verify the README renders correctly**

Visually inspect the markdown: check that the Kotlin code block is properly fenced, the config table renders, and relative links (`docs/design.md`, `docs/superpowers/specs/`) point to real paths.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: replace design-doc README with concise project README"
```

---

### Task 2: Create `docs/design.md` from old README and apply Fix 1 (DSL examples) + Fix 2 (DSL data model) + Fix 3 (DSL builder diagram)

**Files:**
- Create: `docs/design.md` (copy of old README content, then modify)

- [ ] **Step 1: Copy the current README content to `docs/design.md`**

Use git to preserve the old content:

```bash
git show HEAD~1:WorkFlow/README.md > docs/design.md
```

Or simply write the old README content (the 550-line design document, Sections 1–13) to `docs/design.md`.

- [ ] **Step 2: Apply Fix 1 — Replace DSL examples (Section 3.2)**

Replace the fan-out example (lines ~39-57 of old README) with:

```kotlin
val pipeline = workflow {
    activity("split") {
        transition("dsl.split")
        retries(3)
        deadline(Duration.ofMinutes(10))
        fanOut {
            transition("dsl.process-chunk")
            retries(2)
            deadline(Duration.ofMinutes(5))
            failurePolicy(FailurePolicy.BEST_EFFORT)
            joinPolicy(JoinPolicy.Percentage(95))
        }
    }
}
```

Update the reading paragraph below it to remove the `merge` handler reference:

> Reading: `split` produces N payloads. The engine fans out N `process-chunk` tasks to the worker pool. After all reach a terminal state, the last worker evaluates JoinPolicy. If ≥ 95% succeeded, the engine advances to the next activity.

Replace the pure barrier example (lines ~64-80) with:

```kotlin
val pipeline = workflow {
    activity("dispatch") {
        transition("batch.prepare")
        fanOut {
            transition("batch.execute")
            retries(1)
            joinPolicy(JoinPolicy.All)
        }
    }
    activity("notify") {
        transition("batch.notify")
    }
}
```

Add a note after the examples:

> **Note:** Join transitions (inline aggregation handlers executed by the CAS winner) are planned but not yet implemented. Currently, all joins act as pure barriers — the engine advances to the next sequence immediately after JoinPolicy evaluation succeeds.

- [ ] **Step 3: Apply Fix 2 — Update DSL data model tables (Section 3.3)**

In the **ActivityDefinition** table, add two rows after `fanOut`:

| backoffBase   | Duration          | 1 sec      | Base delay for exponential retry backoff              |
| backoffCap    | Duration          | 300 sec    | Maximum delay cap for exponential retry backoff       |

In the **FanOutDefinition** table:
- Remove the `join` row (JoinDefinition field).
- Add these rows:

| joinPolicy    | JoinPolicy     | ALL        | Success gate applied after all sub-tasks complete  |
| backoffBase   | Duration       | 1 sec      | Base delay for exponential retry backoff           |
| backoffCap    | Duration       | 300 sec    | Maximum delay cap for exponential retry backoff    |

**Delete the entire JoinDefinition table** and its preceding header. Replace with:

> `joinPolicy` is a direct field on `FanOutDefinition`. No separate `JoinDefinition` type exists.

Add a new paragraph after the WorkflowDefinition description:

> **WorkflowDefinition** also carries a `deadline` field (Duration, default 1 hour) — the wall-clock timeout for the entire workflow execution.

- [ ] **Step 4: Apply Fix 3 — Update DSL builder diagram (Section 3.4)**

Replace the builder hierarchy text:

```
workflow { }                → WorkflowBuilder    → WorkflowDefinition
  deadline(...)
  activity("name") { }     → ActivityBuilder     → ActivityDefinition
    transition(...)
    retries(...)
    failurePolicy(...)
    deadline(...)
    backoffBase(...)
    backoffCap(...)
    fanOut { }              → FanOutBuilder       → FanOutDefinition
      transition(...)
      retries(...)
      failurePolicy(...)
      deadline(...)
      joinPolicy(...)
      backoffBase(...)
      backoffCap(...)
```

Update the constraints paragraph: remove all references to `JoinBuilder`. The new text:

> Constraints: `@DslMarker` prevents scope leakage. Build-phase validation enforces: activity `transition` is required; if `fanOut` is declared, its `transition` is required. The produced `WorkflowDefinition` is immutable after build.

- [ ] **Step 5: Commit**

```bash
git add docs/design.md
git commit -m "docs: move design doc to docs/design.md, fix DSL examples and data model (Fixes 1-3)"
```

---

### Task 3: Apply Fix 4 (data model) + Fix 5 (state machines) to `docs/design.md`

**Files:**
- Modify: `docs/design.md` — Sections 4 and 5

- [ ] **Step 1: Apply Fix 4 — Update data model tables (Section 4)**

In the **Workflow Table (4.1)**, add a row after `status`:

| deadline_at      | Timestamp | Wall-clock timeout; workflow marked TIMED_OUT when exceeded |

In the **Task Table (4.2)**:
- Change the `status` description to: `PENDING / PROCESSING / COMPLETED / FAILED / TIMED_OUT / DEAD_LETTER / CANCELLED`
- Add these rows after `deadline_at`:

| not_before      | Timestamp | Earliest eligible claim time; enforces exponential backoff delay |
| backoff_base    | Int       | Base delay in seconds for exponential backoff                    |
| backoff_cap     | Int       | Maximum delay cap in seconds for exponential backoff             |
| enqueued_at     | Timestamp | FIFO ordering key for task claiming                              |

- [ ] **Step 2: Apply Fix 5 — Update state machines (Section 5)**

Replace the **Workflow Lifecycle (5.1)** diagram:

```
RUNNING ──(last sequence barrier fires, success)──► COMPLETED
    │
    ├──(failure propagates to workflow level)──► FAILED
    ├──(workflow deadline exceeded)──► TIMED_OUT
    └──(cancel API called)──► CANCELLED
```

Replace the **Task Lifecycle (5.2)** diagram:

```
PENDING ──(claimed via SKIP LOCKED)──► PROCESSING ──(handler succeeds)──► COMPLETED
    │                                       │
    │                                       ├──(handler fails, retries left)──► PENDING (with backoff delay)
    │                                       │
    │                                       ├──(handler fails, no retries)──► FAILED
    │                                       │
    │                                       ├──(deadline exceeded)──► TIMED_OUT (by sweeper)
    │                                       │
    │                                       └──(stale, retries exhausted)──► DEAD_LETTER (by sweeper)
    │
    └──(workflow cancelled)──► CANCELLED
```

Update the **Failure Propagation (5.3)** section. Add a bullet at the end:

- Workflow `deadline_at` exceeded → sweeper transitions workflow to `TIMED_OUT`, cascading cancellation to PENDING tasks.
- Cancel API called → workflow transitions to `CANCELLED`, cascading cancellation to PENDING tasks.

- [ ] **Step 3: Commit**

```bash
git add docs/design.md
git commit -m "docs: update data model and state machines in design doc (Fixes 4-5)"
```

---

### Task 4: Apply Fix 6 (observability) + Fix 7 (new features section) + Fix 8 (join caveat) to `docs/design.md`

**Files:**
- Modify: `docs/design.md` — Sections 8 and 10, new section before 11

- [ ] **Step 1: Apply Fix 8 — Add join transition caveat (Section 8)**

In the "Join — Inline Execution" subsection of Section 8, add this note at the end:

> **Implementation status:** Join transitions (inline aggregation handlers executed by the CAS winner) are designed but not yet implemented. Currently all joins operate as pure barriers — the engine advances to the next sequence immediately after JoinPolicy evaluation succeeds.

- [ ] **Step 2: Apply Fix 6 — Replace observability section (Section 10)**

Replace the entire content of Section 10 with:

```markdown
## 10. Observability

Zero embedded metrics in the engine hot path (BarrierService, WorkerLoop core logic). Aggregate metrics are config-driven via a reusable query exporter component. Per-pod operational metrics use Micrometer directly. Event-level data is captured through structured logging.

### Hot-Path Metrics (MeterRegistry, per-pod)

| Metric | Type | Tags | Source |
|--------|------|------|--------|
| `taskqueue_worker_in_flight_tasks` | Gauge | pod | WorkerLoop |
| `taskqueue_worker_concurrency_limit` | Gauge | pod | WorkerLoop |
| `taskqueue_claim_total` | Counter | pod, outcome | WorkerLoop |
| `taskqueue_handler_duration_seconds` | Histogram | handler, status | MeteredTransitionHandler |
| `leader_election_is_leader` | Gauge | — | LeaderManager |
| `leader_election_epoch` | Gauge | — | LeaderManager |
| `leader_election_heartbeat_age_seconds` | Gauge | — | LeaderManager |
| `taskqueue_shutdown_state` | Gauge | — | ShutdownCoordinator |
| `taskqueue_shutdown_duration_seconds` | Counter | pod | ShutdownCoordinator |

### Query Exporter

A reusable, config-driven component that periodically executes SQL queries and publishes results as Prometheus metrics via Micrometer. Decoupled from the workflow engine — works with any JDBI/JDBC + Micrometer project. Queries and metric mappings defined in `query-exporter.yaml`.

| Metric | Type | Schedule | Purpose |
|--------|------|----------|---------|
| `workflow_by_status` | Gauge | 30s | Workflow status distribution |
| `task_by_status` | Gauge | 30s | Task queue health |
| `workflow_stuck_count` | Gauge | 60s | Stuck workflow alert |
| `task_past_deadline` | Gauge | 30s | Tasks past deadline |
| `task_stuck_processing` | Gauge | 30s | Stale PROCESSING tasks |
| `task_retry_pressure` | Gauge | 30s | Active retries |
| `workflow_high_version` | Gauge | 60s | CAS churn indicator |
| `task_backlog_depth` | Gauge | 30s | Per-handler pending depth |
| `workflow_deep_backlog_count` | Gauge | 60s | Anomalous backlog |
| `task_outcome_failed` / `task_outcome_completed` | Gauge | 30s | 1h sliding window success rate |
| `task_completion_duration_seconds` | Summary | 30s | Claimed-to-completed latency |

### Structured Logging

Event-level data captured by existing services — no Micrometer injection required.

- **CAS win** (BarrierService, INFO): `workflow_id`, `sequence_number`, `phase_type`, `task_count`, `failed_count`, `target_outcome`, `transaction_duration_ms`.
- **CAS loss** (BarrierService, DEBUG): `workflow_id` — expected behaviour, not an error.
- **Sweeper recovery** (Sweeper, WARN): `workflow_id`, `sequence_number`, `time_since_last_update`, `grace_period`.
- **Task claim** (WorkerLoop): MDC context includes `worker_id`, `task_id`, `handler_key`, `attempt`.
```

- [ ] **Step 3: Apply Fix 7 — Add new features section**

Insert a new section between the updated Section 10 (Observability) and Section 11 (Testing Strategy). Number it Section 11 and renumber subsequent sections (Testing → 12, Decisions → 13, Checklist → 14):

```markdown
## 11. Features Added Post-Design

The following capabilities were implemented after the original design document was written:

- **Cancel API:** `WorkflowEngine.cancelWorkflow()` transitions RUNNING → CANCELLED, cascades to PENDING tasks.
- **Workflow-level deadline:** `deadline` on `WorkflowDefinition`, enforced by sweeper via `expireOverdueWorkflows()`.
- **Exponential backoff:** `backoffBase`/`backoffCap` on activities and fan-outs. Retry delay = `min(base × 2^retryCount, cap)`, enforced via `not_before` timestamp.
- **Graceful shutdown:** `ShutdownCoordinator` orchestrates ordered component teardown with per-component and global timeouts.
- **Health probes:** `WorkerLoopHealthCheck` (worker activity freshness) and `LeaderHealthCheck` (leader heartbeat freshness) as Kubernetes liveness probes.
- **Metered handlers:** `MeteredTransitionHandler` decorator records per-handler execution duration histograms.
- **State machine guards:** `WorkflowStatus.requireTransition()` and `TaskStatus.requireTransition()` enforce legal state transitions at runtime.
- **FIFO task ordering:** `enqueued_at` column ensures consistent claim ordering.
- **Dead-letter replay:** `WorkflowEngine.replayWorkflow()` transitions FAILED → RUNNING and replays dead-letter tasks.
```

- [ ] **Step 4: Commit**

```bash
git add docs/design.md
git commit -m "docs: update observability, add post-design features, join caveat (Fixes 6-8)"
```

---

### Task 5: Final verification

**Files:**
- Read: `README.md`, `docs/design.md`

- [ ] **Step 1: Verify README links resolve**

Check that `docs/design.md` and `docs/superpowers/specs/` exist:

```bash
ls docs/design.md
ls docs/superpowers/specs/
```

Both should exist.

- [ ] **Step 2: Verify the DSL example in README matches actual API**

Cross-reference the README's Kotlin code block against `src/main/kotlin/dsl/WorkflowDslBuilders.kt`. Confirm:
- `workflow { }` top-level builder exists
- `deadline()` on `WorkflowBuilder` exists
- `activity("name") { }` on `WorkflowBuilder` exists
- `transition()`, `retries()`, `deadline()` on `ActivityBuilder` exist
- `fanOut { }` on `ActivityBuilder` exists
- `transition()`, `retries()`, `deadline()`, `failurePolicy()`, `joinPolicy()` on `FanOutBuilder` exist

- [ ] **Step 3: Verify config table matches `FrameworkConfig.kt`**

Cross-reference each row in the README config table against `src/main/kotlin/config/FrameworkConfig.kt` defaults. Confirm all property names and default values match.

- [ ] **Step 4: Scan `docs/design.md` for stale `JoinBuilder` / `JoinDefinition` references**

```bash
grep -n "JoinBuilder\|JoinDefinition\|join {" docs/design.md
```

Expected: zero matches (all removed by Fixes 1-3).

- [ ] **Step 5: Verify section numbering in `docs/design.md`**

Confirm sections are numbered 1–14 consecutively after inserting the new Section 11 and renumbering.
