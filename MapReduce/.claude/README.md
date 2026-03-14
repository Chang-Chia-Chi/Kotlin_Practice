# Task Queue & Map-Reduce Framework — Design Document

**Version:** 2.0 Draft  
**Date:** 2026-03-13  
**Status:** Proposal

---

## 1. Motivation

Our system has multiple workloads that need reliable, distributed task execution: dispatching rules that fan out and reduce to Parquet, one-off report generation, scheduled data syncs, notification delivery, and more.

Rather than building bespoke infrastructure for each, we introduce a **two-layer architecture**:

1. **Layer 1 — A generic task queue** backed by Oracle, providing claiming, retry, dead-lettering, and bulkhead-controlled parallel execution. Any unit of work can be enqueued and reliably executed.
2. **Layer 2 — Orchestration patterns** built on top of the queue. Map-Reduce is the first pattern: atomic fan-out, barrier detection, and exactly-once reduce. Future patterns (chaining, fan-out-only, scheduled recurrence) reuse the same queue without modification.

The task queue is the foundation. Map-Reduce is a tenant, not the owner.

---

## 2. Goals & Non-Goals

### Goals

- **Universal work queue.** Any unit of work — standalone, scheduled, or part of an orchestrated job — flows through the same table and claiming mechanism.
- **Reliability first.** Task execution survives pod crashes, leader failovers, and partial failures without human intervention.
- **Exactly-once reduce.** For map-reduce jobs, the reduce phase executes once and only once.
- **Zero framework changes per new workload.** Adding a new task type means implementing a handler interface. Adding a new orchestration pattern means writing an orchestrator — the queue itself never changes.
- **Horizontal scalability.** More pods = more throughput. All pods are identical.

### Non-Goals

- Sub-second dispatch latency. This is a batch-oriented queue, not a streaming system.
- Cross-job dependency graphs or DAG scheduling (future consideration).
- Multi-cluster federation.

---

## 3. Layered Architecture Overview

```
┌─────────────────────────────────────────────────┐
│  Layer 2: Orchestration Patterns                │
│                                                 │
│  ┌──────────────┐  ┌────────────┐  ┌─────────┐  │
│  │  Map-Reduce  │  │  Scheduled │  │  Chain  │  │
│  │  orchestrator│  │  trigger   │  │  (TBD)  │  │
│  └──────┬───────┘  └─────┬──────┘  └────┬────┘  │
│         │                │              │       │
├─────────▼────────────────▼──────────────▼───────┤
│  Layer 1: Generic Task Queue                    │
│                                                 │
│  ┌────────────────────────────────────────────┐  │
│  │  task table (Oracle)                        │  │
│  │  handler-based routing                      │  │
│  │  SELECT FOR UPDATE SKIP LOCKED claiming     │  │
│  │  retry / dead-letter                        │  │
│  │  bulkhead-controlled parallel execution     │  │
│  │  delayed execution (scheduled_at)           │  │
│  └────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

---

## 4. Layer 1 — Generic Task Queue

### 4.1 Core Concepts

**Task** — the universal unit of work. Every task has a `handler` string (routing key), a `payload` (serialized JSON, opaque to the framework), and lifecycle state. The framework claims tasks, invokes the handler, manages retries, and records outcomes. It never inspects the payload.

**Handler** — the only thing a developer implements for Layer 1. A handler is a named function that receives a payload and returns a result. The framework resolves `handler` → handler implementation at runtime via CDI, the same way Sidekiq resolves a `class` string to a Ruby class.

**Queue** — a logical partition for claiming. Pods subscribe to one or more queues. A task belongs to exactly one queue. This allows workload isolation (e.g., `"default"`, `"heavy"`, `"priority"`) without separate infrastructure.

### 4.2 Handler Contract

The handler interface is minimal:

| Method | Purpose |
|--------|---------|
| **handle(payload)** | Execute the work. Return a result indicating success, failure, or retry-with-delay. |

A handler is a pure worker-side concept. The framework discovers all handlers at startup via CDI, building a `handler string → implementation` registry. If a task arrives with an unrecognized handler, it is immediately dead-lettered.

### 4.3 Task Lifecycle

```
                    ┌─────────────────────────────────────┐
                    │           stale reclaim              │
                    │          (leader janitor)            │
                    │                                     │
  ┌─────────┐  SKIP LOCKED  ┌─────────┐  success  ┌──────┴────┐
  │ PENDING ├──────────────►│ CLAIMED ├──────────►│ COMPLETED │
  └────▲────┘               └────┬────┘           └───────────┘
       │                         │
       │  retry < max            │ error
       │                         ▼
       │                    ┌─────────┐  retry >= max  ┌─────────────┐
       └────────────────────┤ FAILED  ├───────────────►│ DEAD_LETTER │
                            └─────────┘                └─────────────┘
```

This state machine applies uniformly to every task — standalone fire-and-forget, map tasks, reduce tasks, scheduled jobs. The framework treats them all identically.

### 4.4 Task Table

```
task
┌──────────────┬────────────────────────────────────────────────┐
│ Column       │ Purpose                                        │
├──────────────┼────────────────────────────────────────────────┤
│ task_id      │ Primary key (UUID)                             │
│ handler      │ Routing key — resolves to a handler at runtime │
│              │ e.g. "dispatch.map", "report.generate",        │
│              │      "dispatch.reduce", "email.send"           │
│ queue        │ Logical queue for partitioned claiming         │
│ payload      │ Serialized JSON — opaque to the framework      │
│ status       │ PENDING / CLAIMED / COMPLETED / FAILED /       │
│              │ DEAD_LETTER                                    │
│ priority     │ Optional — higher priority tasks claimed first │
│ group_id     │ Optional — links tasks to a parent context     │
│              │ (job_id for map-reduce, null for standalone)   │
│ metadata     │ Optional JSON — pattern-specific context       │
│              │ (task_index, phase, correlation IDs, etc.)     │
│ claimed_by   │ Pod identity                                   │
│ claimed_at   │ Stale detection timestamp                      │
│ scheduled_at │ Optional — task not claimable until this time  │
│ retry_count  │ Current attempt number                         │
│ max_retries  │ Configurable per task at enqueue time          │
│ error_message│ Last failure reason                            │
│ created_at   │ Enqueue timestamp                              │
│ completed_at │ Completion timestamp                           │
└──────────────┴────────────────────────────────────────────────┘
```

Key design choices:

- **`handler`** is the routing key. Not a class name (we're not Ruby), but a dot-separated string that CDI resolves to a handler bean. Convention: `"{domain}.{action}"` — e.g., `"dispatch.map"`, `"email.send"`, `"report.generate"`.
- **`queue`** defaults to `"default"`. Claiming queries filter by queue, so pods can subscribe to specific queues or all queues.
- **`group_id`** is nullable. It exists so orchestration patterns (Layer 2) can correlate tasks without the queue framework needing to understand what a "job" is. For standalone tasks, it's null.
- **`metadata`** is a nullable JSON column for pattern-specific bookkeeping. The queue framework never reads it — it's for the orchestrator and the handler to coordinate.
- **`scheduled_at`** enables delayed execution. The claim query includes `AND (scheduled_at IS NULL OR scheduled_at <= systimestamp)`. This covers delayed retries, cron-triggered tasks, and future-scheduled work.
- **`payload`** is fully opaque. The framework serializes and deserializes nothing — it passes the raw JSON string to the handler.

### 4.5 Claiming Mechanism

Pods claim tasks using `SELECT FOR UPDATE SKIP LOCKED`:

1. Filter by queues the pod subscribes to.
2. Filter by `status = PENDING` and `scheduled_at` (if set) not in the future.
3. Order by `priority DESC, created_at ASC`.
4. Lock one row, update to `CLAIMED`, set `claimed_by` and `claimed_at`.

The pod's worker loop repeats this up to its bulkhead limit (see §6.3).

### 4.6 Enqueue API

Any pod can enqueue a task — it's a simple INSERT:

| Parameter | Required | Description |
|-----------|----------|-------------|
| handler | Yes | Routing key |
| payload | Yes | Serialized JSON |
| queue | No | Default: `"default"` |
| max_retries | No | Default: 3 |
| priority | No | Default: 0 |
| group_id | No | For orchestrated tasks |
| metadata | No | Pattern-specific context |
| scheduled_at | No | For delayed execution |

This is the only integration point for Layer 2 patterns. An orchestrator enqueues tasks — the queue handles the rest.

---

## 5. Layer 2 — Map-Reduce Pattern

Map-Reduce is the first orchestration pattern built on the generic task queue. It does not modify the task table or the claiming mechanism. It adds one table (`mr_job`) and one orchestration loop on the leader.

### 5.1 Additional Concepts

**Job** — a single map-reduce execution cycle. Tracks overall lifecycle, task counters, failure policy, and output path. Stored in the `mr_job` table.

**Output Record** — intermediate results produced by map tasks. Stored in `mr_output` and streamed to the reduce handler.

**MapReduce Definition** — a higher-level SPI that generates handlers. A developer implements four methods:

| Method | Executed by | Purpose |
|--------|-------------|---------|
| **Split** | Leader | Given job parameters, produce the list of task inputs |
| **Map** | Worker (via handler) | Given a single task input, produce intermediate outputs |
| **Reduce** | Worker (via handler) | Given all intermediate outputs as a stream, produce the final result |
| **OnCompleted** | Worker (via handler) | Publish, upload, or notify — whatever "done" means |

Under the hood, the framework registers two handlers per definition:

- `"{jobType}.map"` → calls the definition's Map method, persists Output Records.
- `"{jobType}.reduce"` → calls the definition's Reduce and OnCompleted methods.

The developer writes the definition. The framework generates the handlers. Workers execute them like any other task — they don't know they're part of a map-reduce job.

### 5.2 Job Table

```
mr_job
┌─────────────────────┬─────────────────────────────────────────┐
│ Column              │ Purpose                                 │
├─────────────────────┼─────────────────────────────────────────┤
│ job_id              │ Primary key (UUID) — also used as       │
│                     │ group_id in the task table               │
│ job_type            │ Definition routing key                  │
│ status              │ CREATED / RUNNING / REDUCING /          │
│                     │ COMPLETED / FAILED                      │
│ job_params          │ Serialized JSON — input to Split        │
│ total_tasks         │ Number of map tasks (set at fan-out)    │
│ completed_tasks     │ Atomically incremented by map handlers  │
│ failed_tasks        │ Atomically incremented on dead-letter   │
│ failure_policy      │ FAIL_JOB / THRESHOLD / BEST_EFFORT     │
│ failure_threshold   │ Max allowed failure ratio (for THRESHOLD│
│                     │ policy)                                 │
│ reducing_fence_token│ Leader's fencing token                  │
│ result_metadata     │ Serialized reduce result summary        │
│ version             │ Optimistic lock                         │
│ created_at          │ Timestamp                               │
│ updated_at          │ Timestamp                               │
└─────────────────────┴─────────────────────────────────────────┘
```

### 5.3 Output Record Table

```
mr_output
┌──────────────┬────────────────────────────────────────────────┐
│ Column       │ Purpose                                        │
├──────────────┼────────────────────────────────────────────────┤
│ output_id    │ Primary key (UUID)                             │
│ job_id       │ Partition key for reduce queries               │
│ task_id      │ Provenance — which map task produced this      │
│ output_data  │ Serialized intermediate result (JSON)          │
│ created_at   │ Timestamp                                      │
└──────────────┴────────────────────────────────────────────────┘
```

### 5.4 Job State Machine

```
                          ┌──────────────────────────────┐
                          │         leader recovery       │
                          │                              │
  ┌─────────┐   atomic   ┌▼────────┐   CAS    ┌──────────┴─┐   success  ┌───────────┐
  │ CREATED ├────────────►│ RUNNING ├─────────►│  REDUCING  ├──────────►│ COMPLETED │
  └─────────┘  fan-out    └────┬────┘  barrier  └─────┬──────┘           └───────────┘
                               │       met            │
                               │                      │  reduce
                               ▼                      ▼  dead-lettered
                           ┌────────┐            ┌────────┐
                           │ FAILED │◄───────────│ FAILED │
                           └────────┘            └────────┘
                         (policy threshold)
```

Key transitions:

- **CREATED → RUNNING**: Atomic. Leader inserts the job row and all map tasks (into the generic task table with `group_id = job_id`) in one transaction.
- **RUNNING → REDUCING**: By the leader, after its monitoring loop detects that all map tasks have resolved. CAS ensures exactly-once. Leader then enqueues a reduce task.
- **REDUCING → COMPLETED**: By the leader, after it observes that the reduce task has completed.
- **REDUCING → RUNNING**: By the leader, only for recovery after a crash before the reduce task was enqueued.
- **→ FAILED**: When failure policy threshold is exceeded or the reduce task is dead-lettered.

### 5.5 Detailed Flow

#### Phase 1 — Fan-Out

The leader submits a job by providing a job type and parameters:

1. Calls the definition's **Split** method to produce N task inputs.
2. In a **single Oracle transaction**:
    - Inserts a `mr_job` row (status = RUNNING, total_tasks = N).
    - Inserts N rows into the generic `task` table, each with `handler = "{jobType}.map"`, `group_id = job_id`, `queue = "mr"`, and the serialized input as payload.
3. Returns the Job ID.

Atomicity matters: pods will never see a partial set of tasks.

#### Phase 2 — Map (Distributed Execution)

From the queue's perspective, these are just tasks. Any pod's worker loop claims them via `SKIP LOCKED` and invokes the `"{jobType}.map"` handler, which:

1. Deserializes the task input from the payload.
2. Calls the definition's **Map** method.
3. Batch-inserts Output Records into `mr_output`.
4. Increments `mr_job.completed_tasks` (in the same transaction as the task completion).

If map throws, normal task retry/dead-letter applies. On dead-letter, `mr_job.failed_tasks` is incremented instead.

#### Phase 3 — Barrier Detection (Leader Monitors)

The leader's monitoring loop periodically reads job counters from `mr_job`:

When `completed_tasks + failed_tasks == total_tasks`, the leader performs a compare-and-swap: transition the job from RUNNING to REDUCING. It then enqueues a single reduce task into the generic `task` table with `handler = "{jobType}.reduce"` and `group_id = job_id`.

Workers are unaware of the barrier. They simply complete tasks — the leader interprets the counters.

#### Phase 4 — Reduce

The reduce task is claimed by any pod's worker loop like any other task. The `"{jobType}.reduce"` handler:

1. Streams all Output Records for the job from `mr_output` (cursor-based).
2. Calls the definition's **Reduce** method, passing the stream.
3. On success, calls **OnCompleted** (e.g., upload Parquet).
4. Marks the task as COMPLETED.

The leader observes the reduce task completion and transitions the job to COMPLETED.

The reduce task gets all standard queue guarantees for free: stale detection, retry, dead-lettering. No special recovery logic.

### 5.6 Sequence Diagram

```
  Pod C (leader)       Oracle            Pod A            Pod B
    │                    │                  │                │
    │  ══ SUBMIT ═══     │                  │                │
    ├── mr_job +  ──────►│                  │                │
    │   task rows        │                  │                │
    │   (1 txn)          │                  │                │
    │                    │                  │                │
    │  ══ MAP ══════     │                  │                │
    │                    │◄── claim task ───┤                │
    │                    │── "dispatch.map"►│                │
    │                    │◄── claim task ───┼────────────────┤
    │                    │── "dispatch.map"►┼───────────────►│
    │                    │                  │                │
    │  (Pod C can also   │◄── outputs ──────┤                │
    │   claim map tasks) │◄── COMPLETED ────┤                │
    │                    │◄── outputs ──────┼────────────────┤
    │                    │◄── COMPLETED ────┼────────────────┤
    │                    │                  │                │
    │  ══ BARRIER ══     │                  │                │
    ├── read counters ──►│                  │                │
    │◄── 2/2 done ───────┤                  │                │
    ├── CAS: REDUCING ──►│                  │                │
    │                    │                  │                │
    │  ══ REDUCE ═══     │                  │                │
    ├── enqueue ────────►│                  │                │
    │   "dispatch.reduce"│                  │                │
    │                    │◄── claim task ───┤                │
    │                    │──"dispatch.reduce"│                │
    │                    │                  │  reduce()      │
    │                    │                  │  onCompleted() │
    │                    │◄── COMPLETED ────┤                │
    │                    │                  │                │
    │  ══ COMPLETE ══    │                  │                │
    ├── CAS: COMPLETED ─►│                  │                │
    │                    │                  │                │
```

---

## 6. System Architecture

### 6.1 Component Overview

All pods run the same application binary. Every pod serves the configuration API and runs a worker loop. One pod additionally holds the Kubernetes Lease and runs orchestration loops. Leadership is a **role**, not a separate deployment.

```
                     ┌──────────────────┐
                     │   K8s Service    │
                     │  (config API)    │
                     └────────┬─────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
         ┌────▼────┐    ┌────▼────┐    ┌─────▼──────┐
         │  Pod A  │    │  Pod B  │    │   Pod C    │
         │         │    │         │    │  (leader)  │
         │ config  │    │ config  │    │ config API │
         │ API ✓   │    │ API ✓   │    │ ✓          │
         │         │    │         │    │            │
         │ claim   │    │ claim   │    │ orchestrate│
         │ handle  │    │ handle  │    │ + claim    │
         │         │    │         │    │ + handle   │
         │ bulkhead│    │ bulkhead│    │            │
         │ = N     │    │ = N     │    │ bulkhead=N │
         └─────────┘    └─────────┘    └────────────┘
              │               │               │
              └───────────────┼───────────────┘
                              │
         ┌────────────────────▼────────────────────────────┐
         │                 Oracle Database                  │
         │                                                 │
         │  ┌──────────────────────────────────────────┐   │
         │  │  task (generic work queue)                │   │
         │  └──────────────────────────────────────────┘   │
         │                                                 │
         │  ┌──────────────┐  ┌─────────────────────────┐  │
         │  │  mr_job       │  │  mr_output              │  │
         │  │  (map-reduce  │  │  (intermediate results) │  │
         │  │   pattern)    │  │                         │  │
         │  └──────────────┘  └─────────────────────────┘  │
         │                                                 │
         │  ┌──────────────────────────────────────────┐   │
         │  │  configuration tables                     │   │
         │  │  (rules, parameters, schedules)           │   │
         │  └──────────────────────────────────────────┘   │
         └─────────────────────────────────────────────────┘
```

### 6.2 Pod Identity — Homogeneous Deployment

Every pod is identical at startup. On boot, each pod:

1. Starts serving the **configuration API** (REST endpoints for rule CRUD).
2. Starts the **worker loop** (poll the task queue, invoke handlers within bulkhead limits).
3. Attempts to **acquire the Kubernetes Lease**. Exactly one pod wins and becomes the leader.

The leader pod continues running its worker loop — the orchestration overhead (lightweight counter polling) does not compete meaningfully with task execution. If the leader pod dies, another pod acquires the lease. No pod is special; leadership is a runtime role.

### 6.3 Bulkhead (Parallel Execution)

Every pod (including the leader) runs a worker loop that can process multiple tasks concurrently. Each pod is configured with a **bulkhead** — a concurrency limit:

```
Pod (any pod, including leader)
┌──────────────────────────────────┐
│  bulkhead = 4                    │
│                                  │
│  ┌──────┐ ┌──────┐              │
│  │ slot │ │ slot │  ← active    │
│  │  1   │ │  2   │              │
│  └──────┘ └──────┘              │
│  ┌──────┐ ┌──────┐              │
│  │ slot │ │ slot │  ← idle      │
│  │  3   │ │  4   │              │
│  └──────┘ └──────┘              │
│                                  │
│  claim loop: if active < 4,     │
│    claim another task            │
└──────────────────────────────────┘
```

The claim loop checks how many slots are active before attempting another `SKIP LOCKED` claim. Total cluster-wide parallelism is `number of pods × bulkhead`.

Combined with `SKIP LOCKED`, the system naturally balances load — a slow pod simply claims fewer tasks over time while fast pods absorb the slack.

### 6.4 Configuration API

The configuration API (CRUD for dispatching rules, job parameters, schedules) is a stateless HTTP layer backed by Oracle. Any pod can serve it. A single Kubernetes Service load-balances API traffic across all pods.

There is no in-memory coupling between configuration and job execution. The leader reads configurations from Oracle at Split time. A user can update a rule on Pod A; the next time the leader calls Split, it reads the latest state. Natural consistency, no cache invalidation.

Configuration API availability is decoupled from leader availability. Even during a leader election, users can read and write configurations without interruption.

### 6.5 Roles (Logical)

Although all pods are identical, two logical roles are useful to describe:

**Worker** (every pod) — claims tasks from the queue, invokes the resolved handler, reports completion or failure. Workers are unaware of orchestration patterns — a map task and a reduce task and a standalone task all look the same: claim, handle, report.

**Leader** (one pod, via K8s Lease) — the orchestrator that drives pattern-specific state machines but **never executes business logic**:

1. **Submit** — call Split, enqueue map tasks atomically.
2. **Monitor** — poll job counters to detect barriers.
3. **Transition** — drive job state machines (RUNNING → REDUCING → COMPLETED).
4. **Dispatch** — enqueue reduce tasks when barriers are met.
5. **Recover** — reclaim stale tasks, re-dispatch stuck reduces after failover.

---

## 7. Reliability Guarantees

### 7.1 Task-Level (Layer 1 — applies to all tasks)

**Pod crash mid-task.** The task stays in CLAIMED. The leader's stale-task reaper detects tasks claimed beyond a configurable timeout and flips them back to PENDING (incrementing retry count). Another pod picks them up. Handlers should be **idempotent** to safely handle re-execution after a partial commit.

**Duplicate execution.** `SKIP LOCKED` prevents double-claiming under normal conditions. In the stale-reclaim scenario, the original pod might still be running (slow, not dead). Idempotent handlers ensure correctness.

**Poison tasks.** Tasks that fail repeatedly are dead-lettered after exhausting max retries, preventing infinite retry loops.

### 7.2 Map-Reduce Level (Layer 2)

**Worker crash during reduce.** The reduce task is just a task — it gets the same stale-detection treatment. If the worker crashes, the reaper reclaims it and another pod picks it up. Reduce implementations should be idempotent (deterministic output path with overwrite semantics).

**Leader crash.** If the leader dies, another pod acquires the Kubernetes Lease. The new leader's recovery loop inspects all in-flight jobs:

- **Jobs in RUNNING**: Resume monitoring. If the barrier is already met, transition to REDUCING and enqueue the reduce task.
- **Jobs in REDUCING without a reduce task**: The previous leader crashed after the CAS but before enqueueing. The new leader enqueues it.
- **Jobs in REDUCING with a reduce task in progress**: No action needed. Normal monitoring resumes.

**Duplicate reduce.** The reduce task is a single row in the task table. Only one pod can claim it via `SKIP LOCKED`. If stale reclaim causes re-execution, idempotent reduce ensures no harm.

**Partial failure.** When some map tasks are dead-lettered, the barrier still fires (`completed + failed == total`). A configurable failure policy determines the outcome:

| Policy | Behavior |
|--------|----------|
| **Fail job** | Any dead-lettered map task fails the entire job. No reduce. |
| **Threshold** | If `failed / total > X%`, fail the job. Otherwise, reduce with partial data. |
| **Best effort** | Always reduce with whatever completed. |

The policy is declared per job type in the definition.

### 7.3 Fencing

The leader holds a Kubernetes Lease with a fencing token. The token is included in all leader writes to Oracle, preventing a zombie leader from interfering with a newly elected leader's operations.

---

## 8. Coordination Mechanisms

The framework uses exactly three Oracle-level primitives. No external queues, no distributed locks, no consensus protocols beyond the K8s Lease for leader election.

| Mechanism | Where Used | Why |
|-----------|-----------|-----|
| **SELECT FOR UPDATE SKIP LOCKED** | Task claiming (all pods) | Lock-free work distribution — pods never block each other |
| **Atomic counter increment** | Map task completion (Layer 2) | Workers increment job counters; leader reads them to detect barriers |
| **Compare-and-swap (conditional UPDATE)** | Job status transitions (leader) | Exactly-once semantics for RUNNING→REDUCING and recovery resets |

---

## 9. Patterns Enabled by the Generic Queue

The task queue is pattern-agnostic. Map-Reduce is the first pattern; others require no queue changes:

| Pattern | How it uses the queue |
|---------|----------------------|
| **Map-Reduce** | Enqueue N map tasks with `group_id`, leader monitors counters, enqueue 1 reduce task |
| **Fire-and-forget** | Enqueue one task, no `group_id`, no orchestration |
| **Scheduled / cron** | Enqueue task with `scheduled_at` in the future. A recurring scheduler re-enqueues on completion. |
| **Fan-out (no reduce)** | Enqueue N tasks with a `group_id` for correlation. Leader monitors completion but no reduce step. |
| **Chained tasks** | Handler enqueues the next task in `onCompleted`. No orchestrator needed. |
| **Delayed retry** | Handler returns `Retry(delay)` → framework sets `scheduled_at` and flips back to PENDING. |
| **Priority dispatch** | Enqueue with `priority > 0`. Claim query orders by priority descending. |

---

## 10. Extensibility

### 10.1 Adding a New Standalone Task

1. Implement the handler interface.
2. Annotate it with the handler routing key.
3. Enqueue tasks from wherever needed (API endpoint, scheduler, another handler).

No table changes. No framework changes. No orchestrator involvement.

### 10.2 Adding a New Map-Reduce Job Type

1. Define data types for the input, intermediate output, and final result.
2. Implement the MapReduce Definition (Split, Map, Reduce, OnCompleted + serde hooks).
3. Register the job type string.

The framework auto-generates the `"{jobType}.map"` and `"{jobType}.reduce"` handlers. No table changes. No framework changes.

### 10.3 Adding a New Orchestration Pattern

1. Define a pattern-specific state table (like `mr_job` for map-reduce).
2. Write an orchestrator loop that runs on the leader.
3. Enqueue tasks into the generic `task` table with appropriate `handler`, `group_id`, and `metadata`.

The task queue itself is untouched. The new pattern is a Layer 2 tenant.

---

## 11. Operational Concerns

### 11.1 Observability

Key metrics to expose via Micrometer/Prometheus:

- **Queue depth** by queue and handler — are pods keeping up?
- **Task claim-to-completion latency** by handler — performance per task type.
- **Stale task reclaims per interval** — indicator of pod health issues.
- **Dead-letter rate** by handler — indicator of handler bugs or transient failures.
- **Map-reduce specific**: barrier-to-reduce-complete latency, job completion rate.

### 11.2 Backpressure & Throughput Tuning

Each pod's bulkhead (§6.3) is the primary throughput control. Operators tune parallelism by adjusting the bulkhead value per pod or the number of replicas. Total cluster-wide parallelism is `replicas × bulkhead`.

For map-reduce jobs with large fan-out, the claim query can include a subquery counting in-flight tasks per `group_id` to enforce a per-job concurrency cap, preventing one large job from starving others.

### 11.3 Output Storage Strategy

For map-reduce job types where intermediate outputs are small (a few KB per task), storing serialized JSON in `mr_output` is sufficient. For job types producing large intermediates, the handler should write to a staging area (a dedicated table, temp file, or object storage) and store a **reference/pointer** as the output. The framework is agnostic to what the blob contains.

### 11.4 Cleanup

Completed tasks should be retained for a configurable period for auditability, then purged. A scheduled cleanup job — itself a task on the queue — can handle this.

---

## 12. Comparison with Alternatives

| Approach | Pros | Cons (for our context) |
|----------|------|------------------------|
| **This framework** | No new infra; Oracle-native; K8s-native; generic queue + composable patterns; homogeneous deployment | Poll-based latency; Oracle throughput ceiling |
| **Sidekiq** | Proven; simple API | Redis-backed (not Oracle); Ruby ecosystem |
| **Jobrunr** | JVM; SQL-backed | Tightly coupled to Java method signatures; no orchestration |
| **Temporal** | Full workflow orchestration | Requires its own server cluster; massive operational overhead |
| **External queue (NATS/Kafka)** | Lower latency; built-in pub/sub | Additional infra; dual-write consistency issues |
| **K8s Jobs** | Native retry; pod-per-task | Cold-start overhead; no shared state; orchestration still needed |

The framework is right-sized: a Sidekiq-style task queue implemented on Oracle, with map-reduce orchestration as the first composable pattern on top.