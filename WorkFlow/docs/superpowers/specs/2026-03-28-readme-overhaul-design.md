# README Overhaul — Design Spec

## Goal

Replace the current 550-line design-document README with a concise project README aimed at team engineers and future onboarding. Move the design document to `docs/design.md` with inaccuracies fixed.

## Deliverables

Two files:

1. **`README.md`** — new project README (~120-150 lines)
2. **`docs/design.md`** — relocated and corrected design document

---

## 1. New README.md

### Section 1 — Title + One-liner

```
# Lock-Free Workflow Engine
```

One-paragraph description: Kotlin/Quarkus workflow engine that eliminates row-lock contention by replacing mutable counters with MVCC-based barrier detection and optimistic CAS transitions. Backed by Oracle.

Stack line: `Kotlin · Quarkus · JDBI · Oracle · Kubernetes`

### Section 2 — Architecture

Three subsections, each a short paragraph:

**Dual-Path Progression:**
- Primary path: lock-free barrier. Workers derive completion from a read-only MVCC aggregate query and advance the workflow via optimistic CAS on a single workflow row.
- Backup path: leader sweeper. A K8s-lease-elected leader polls at low frequency to detect workflows stuck due to worker death between CAS success and commit.
- Key invariant: both paths share the same CAS predicate — at most one actor wins per phase.

**Declarative DSL:**
- Type-safe Kotlin DSL producing immutable `WorkflowDefinition` data classes.
- `@DslMarker` prevents scope leakage; build-phase validation enforces required fields.
- One code example: a fan-out workflow using the current API (`joinPolicy()` on `FanOutBuilder`):

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

**Two-Table Model:**
- `workflow` table: one mutable row per execution, sole CAS target. Columns: `id`, `definition` (CLOB), `current_sequence`, `version`, `status`, `deadline_at`, timestamps.
- `task` table: standard queue rows. Each task belongs to a workflow at a specific sequence number. Columns include retry/backoff fields, `not_before` for exponential backoff, `enqueued_at` for FIFO ordering.
- Sequence expansion: linear activity = 1 sequence, fan-out = 2 (scatter + parallel). No activity table.

### Section 3 — Project Structure

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

### Section 4 — Getting Started

Prerequisites: JDK 21, Docker Desktop (Testcontainers), Maven 3.9+

Commands:
- Build: `mvn package`
- Test: `mvn test`
- Dev mode: `mvn quarkus:dev`

### Section 5 — Configuration

Table of key `framework.*` properties from `FrameworkConfig`:

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

### Section 6 — Documentation

Links to:
- `docs/design.md` — full design document (algorithm, data model, state machines, decision log, implementer checklist)
- `docs/superpowers/specs/` — feature design specs

---

## 2. docs/design.md — Fixes to Apply

The current README.md content is moved to `docs/design.md`. The following corrections are applied:

### Fix 1: DSL Examples (Section 3.2)

**Before:** `join { policy(JoinPolicy.PERCENTAGE(95)); transition("dsl.merge") }`

**After:** Replace with the actual flat API. The fan-out example becomes:

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

The pure barrier example becomes:

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

Add a note after examples: "Join transitions (inline aggregation handlers) are planned but not yet implemented. Currently, all joins act as pure barriers."

### Fix 2: DSL Data Model (Section 3.3)

**ActivityDefinition table:** Add rows for `backoffBase` (Duration, default 1s) and `backoffCap` (Duration, default 300s).

**FanOutDefinition table:** Add rows for `backoffBase` (Duration, default 1s) and `backoffCap` (Duration, default 300s). Remove `join` field. Add `joinPolicy` (JoinPolicy, default ALL).

**Remove JoinDefinition table** entirely. Replace with a note that `joinPolicy` is a direct field on `FanOutDefinition`.

**WorkflowDefinition:** Add `deadline` field (Duration, default 1h, workflow-level wall-clock timeout).

### Fix 3: DSL Builder Diagram (Section 3.4)

**Before:**
```
fanOut { }              → FanOutBuilder       → FanOutDefinition
  ...
  join { }              → JoinBuilder         → JoinDefinition
    policy(...)
    transition(...)     // optional
```

**After:**
```
fanOut { }              → FanOutBuilder       → FanOutDefinition
  transition(...)
  retries(...)
  failurePolicy(...)
  deadline(...)
  joinPolicy(...)
  backoffBase(...)
  backoffCap(...)
```

Remove all references to `JoinBuilder`.

### Fix 4: Data Model (Section 4)

**Workflow table (4.1):** Add `deadline_at` column (Timestamp — workflow-level wall-clock timeout).

**Task table (4.2):**
- Add `TIMED_OUT` and `CANCELLED` to status enum description.
- Add columns: `not_before` (Timestamp — earliest eligible claim time, for exponential backoff), `backoff_base` (Int seconds — base delay), `backoff_cap` (Int seconds — max delay), `enqueued_at` (Timestamp — FIFO ordering key).

### Fix 5: State Machines (Section 5)

**Workflow lifecycle (5.1):**
```
RUNNING ──(last sequence barrier fires, success)──► COMPLETED
    │
    ├──(failure propagates to workflow level)──► FAILED
    ├──(workflow deadline exceeded)──► TIMED_OUT
    └──(cancel API called)──► CANCELLED
```

**Task lifecycle (5.2):**
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

### Fix 6: Observability (Section 10)

Replace the observability section with two subsections:

**Hot-Path Metrics (MeterRegistry-based, per-pod):**

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

**Query Exporter Metrics (config-driven SQL, in `query-exporter.yaml`):**

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

### Fix 7: New Section — Features Added Post-Design

Add a new section (after Observability, before Testing Strategy) listing features implemented since the original design doc:

- **Cancel API:** `WorkflowEngine.cancelWorkflow()` transitions RUNNING → CANCELLED, cascades to PENDING tasks.
- **Workflow-level deadline:** `deadline` on `WorkflowDefinition`, enforced by sweeper via `expireOverdueWorkflows()`.
- **Exponential backoff:** `backoffBase`/`backoffCap` on activities and fan-outs. Retry delay = `min(base * 2^retryCount, cap)`, enforced via `not_before` timestamp.
- **Graceful shutdown:** `ShutdownCoordinator` orchestrates ordered component teardown with per-component and global timeouts.
- **Health probes:** `WorkerLoopHealthCheck` (worker activity freshness) and `LeaderHealthCheck` (leader heartbeat freshness) as Kubernetes liveness probes.
- **Metered handlers:** `MeteredTransitionHandler` decorator records per-handler execution duration histograms.
- **State machine guards:** `WorkflowStatus.requireTransition()` and `TaskStatus.requireTransition()` enforce legal state transitions at runtime.
- **FIFO task ordering:** `enqueued_at` column ensures consistent claim ordering.
- **Dead-letter replay:** `WorkflowEngine.replayWorkflow()` transitions FAILED → RUNNING and replays dead-letter tasks.

### Fix 8: Execution Semantics — Join Transition Caveat (Section 8)

Add a note to the "Join — Inline Execution" subsection:

> **Implementation status:** Join transitions (inline aggregation handlers executed by the CAS winner) are designed but not yet implemented. Currently all joins operate as pure barriers — the engine advances to the next sequence immediately after JoinPolicy evaluation succeeds.

---

## Out of Scope

- Rewriting the testing strategy section (Section 11) — tests exist and match the described approach.
- Rewriting the design decisions table (Section 12) — still accurate.
- Rewriting the implementer checklist (Section 13) — still useful.
- Updating design specs in `docs/superpowers/specs/` — separate effort.
