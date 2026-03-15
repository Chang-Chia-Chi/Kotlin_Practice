# Chained Tasks Pattern — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0  
**Layer:** 2 (Orchestration Pattern)

---

## 1. Problem Statement

Some workflows are inherently sequential: step A must complete before step B begins, whose output feeds step C. Examples:

- **ETL pipeline:** Extract from DB2 → Transform (cleanse, join) → Load to Iceberg via Trino.
- **Document processing:** Validate metadata → Generate PDF → Upload to MinIO → Send notification.
- **Onboarding flow:** Create account → Provision resources → Send welcome email.

Today, the only way to sequence tasks is to have each handler explicitly enqueue the next task at the end of its execution. This works but is ad hoc:

- The handler must know what comes next. Business logic and orchestration are tangled.
- There's no visibility into the chain as a whole. You can't see "this 4-step pipeline is on step 3."
- Error handling is per-handler. If step 3 fails, there's no framework-level way to fail the entire chain or resume from step 3 after a fix.
- Adding or reordering steps requires code changes in handlers.

The Chained Tasks pattern makes sequential pipelines a first-class concept.

---

## 2. Core Concept

A chain is an ordered sequence of task steps. Each step has a handler, and the output of one step becomes the input of the next. The framework manages the sequencing — handlers don't know they're part of a chain.

```
┌─────────────────────────────────────────────────────────┐
│  Chain: "etl-pipeline"                                  │
│                                                         │
│  Step 1          Step 2          Step 3          Step 4 │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌───────┐│
│  │ extract │───►│transform│───►│  load   │───►│notify ││
│  │ (DB2)   │    │(cleanse)│    │(Iceberg)│    │(email)││
│  └─────────┘    └─────────┘    └─────────┘    └───────┘│
│                                                         │
│  Output of each step is passed as payload to the next   │
└─────────────────────────────────────────────────────────┘
```

Key distinction from Map-Reduce: there's no parallelism within a chain. Steps are strictly sequential. The value is in reliable sequencing, output passing, and chain-level lifecycle tracking — not in fan-out.

---

## 3. Design: Handler-Driven Sequencing

### 3.1 No Leader Orchestration Needed

Unlike Map-Reduce (where the leader monitors barriers and dispatches reduce), chained tasks don't need leader-side orchestration. The sequencing is handler-driven:

1. Step 1's task completes.
2. The framework inspects the chain definition to determine the next step.
3. The framework enqueues step 2's task with step 1's output as the payload.
4. Repeat until all steps are done or a step fails.

This happens in the worker that completed the current step — no leader polling, no barrier detection, no CAS transitions. The chain advances synchronously within the task completion transaction.

### 3.2 Why Not Leader-Orchestrated?

Leader orchestration would mean the leader polls for completed chain steps and enqueues the next one. This adds latency (poll interval) and leader load for no benefit. Sequential chains have no coordination complexity — the "barrier" is trivially met when one task completes. The worker can advance the chain immediately.

---

## 4. Chain Definition

A chain definition specifies the sequence of steps. It can be static (code-defined) or dynamic (configuration-defined).

### 4.1 Definition Structure

| Field | Purpose |
|-------|---------|
| chainType | Unique identifier (e.g., `"etl-pipeline"`, `"document-flow"`) |
| steps | Ordered list of step definitions |
| failurePolicy | What to do when a step fails (see §7) |

Each step within the definition:

| Field | Purpose |
|-------|---------|
| stepIndex | Position in the chain (0-based) |
| handler | Task handler routing key for this step |
| queue | Queue to enqueue on (default: `"default"`) |
| maxRetries | Per-step retry limit |
| payloadTransform | How to derive this step's payload from the previous step's output (see §5) |

### 4.2 Registration

Chain definitions are registered at startup via CDI, similar to Map-Reduce definitions. The framework builds a registry of `chainType → definition`. When a chain task completes, the framework looks up the definition to find the next step.

---

## 5. Payload Passing

Each handler returns a `TaskResult` that can include an output payload (serialized JSON). The framework passes this output as the next step's input payload.

```
Step 1 handler receives: { "source": "DB2", "query": "SELECT ..." }
Step 1 handler returns:  TaskResult.success(output = { "rows": 15000, "tempFile": "/tmp/extract.csv" })
                                          │
                                          ▼
Step 2 handler receives: { "rows": 15000, "tempFile": "/tmp/extract.csv" }
Step 2 handler returns:  TaskResult.success(output = { "cleanedFile": "/tmp/cleaned.parquet" })
                                          │
                                          ▼
Step 3 handler receives: { "cleanedFile": "/tmp/cleaned.parquet" }
...
```

**Handler contract extension:** `TaskResult.success()` gains an optional `output` field (serialized JSON). Handlers that are chain-unaware can omit it — the framework passes the original payload unchanged to the next step if no output is provided.

**Payload transform:** The chain definition can optionally specify a payload transform per step. This is a simple merge or extraction strategy:

| Strategy | Behavior |
|----------|----------|
| PASS_OUTPUT (default) | Next step receives the previous step's output as-is |
| MERGE_WITH_ORIGINAL | Next step receives the original chain input merged with the previous step's output |
| STATIC | Next step receives a statically defined payload from the definition (ignoring previous output) |

---

## 6. Chain Lifecycle

### 6.1 Chain Job Table

Like Map-Reduce has `mr_job`, Chained Tasks has `chain_job`:

```
chain_job
┌─────────────────────┬─────────────────────────────────────────┐
│ Column              │ Purpose                                 │
├─────────────────────┼─────────────────────────────────────────┤
│ chain_id            │ Primary key (UUID) — also used as       │
│                     │ group_id in the task table               │
│ chain_type          │ Definition routing key                  │
│ status              │ RUNNING / COMPLETED / FAILED            │
│ current_step        │ Index of the step currently executing   │
│ total_steps         │ Total number of steps                   │
│ chain_params        │ Original input parameters (JSON)        │
│ failure_policy      │ FAIL_CHAIN / RETRY_STEP / SKIP_STEP    │
│ last_step_output    │ Output of the most recently completed   │
│                     │ step (used as next step's input)        │
│ error_message       │ Error from the failed step (if any)     │
│ version             │ Optimistic lock                         │
│ created_at          │ Timestamp                               │
│ updated_at          │ Timestamp                               │
└─────────────────────┴─────────────────────────────────────────┘
```

### 6.2 State Machine

```
  ┌─────────┐   enqueue step 0   ┌─────────┐   last step done   ┌───────────┐
  │ CREATED ├────────────────────►│ RUNNING ├───────────────────►│ COMPLETED │
  └─────────┘                     └────┬────┘                    └───────────┘
                                       │
                                       │ step fails
                                       │ (policy: FAIL_CHAIN)
                                       ▼
                                  ┌─────────┐
                                  │ FAILED  │
                                  └─────────┘
```

### 6.3 Step Advancement (the core mechanism)

When a task completes successfully and its metadata indicates it's part of a chain:

1. Read the `chain_job` row for this chain.
2. Determine the next step from the chain definition.
3. If there is a next step:
   a. Construct the next step's payload (from the current step's output + transform strategy).
   b. INSERT a new task into the task table with the next step's handler, the chain's group_id, and metadata indicating the step index.
   c. UPDATE `chain_job`: increment `current_step`, store `last_step_output`.
   d. Both in one transaction.
4. If there is no next step (this was the last step):
   a. UPDATE `chain_job`: status = COMPLETED.

This is done by the framework's post-completion logic, not by the handler. The handler doesn't know about chains.

### 6.4 Task Metadata for Chain Tasks

Each task in a chain carries metadata linking it to the chain:

| Field (in metadata JSON) | Purpose |
|--------------------------|---------|
| chainId | FK to chain_job |
| chainType | Definition type (for lookup) |
| stepIndex | Which step this task represents |

---

## 7. Failure Handling

When a step's task is dead-lettered (retries exhausted):

| Policy | Behavior |
|--------|----------|
| **FAIL_CHAIN** (default) | Mark the chain as FAILED. No subsequent steps are enqueued. The dead letter processor can replay the failed step's task, which resumes the chain from that point. |
| **SKIP_STEP** | Advance to the next step, passing the previous step's output (skipping the failed step's contribution). Useful for optional steps (e.g., notification sending). |

There is intentionally no automatic retry-from-beginning policy. Chains may have side effects (file uploads, external API calls) that make full replay unsafe. Recovery is always from the failed step forward.

---

## 8. Starting a Chain

Chains are started by enqueuing a "chain start" request. This can be done by:

- An API endpoint: `POST /api/chains` with chainType and input parameters.
- Another handler (a chain can be started at the end of a standalone task).
- The scheduled trigger (a cron schedule that starts a chain periodically).

The start logic (runs on the enqueuing pod, no leader required):

1. Create a `chain_job` row with status = RUNNING, current_step = 0.
2. Enqueue the first step's task into the task table.
3. Both in one transaction.

---

## 9. Resuming a Failed Chain

When a chain is in FAILED status, it can be resumed from the failed step via the dead letter processor's replay mechanism:

1. Replay the dead-lettered task for the failed step (the task is in the task table with status = DEAD_LETTER).
2. The dead letter processor moves it back to PENDING.
3. A worker claims and executes it.
4. On success, the chain advancement logic (§6.3) continues to the next step.
5. The chain continues from where it left off.

No special "resume chain" API is needed — replaying the dead-lettered task naturally resumes the chain because the advancement logic runs on every successful step completion.

The chain's status transitions from FAILED back to RUNNING when the replayed step's task is moved to PENDING.

---

## 10. Observability

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `taskqueue.chain.started` | Counter | chain_type | Chains initiated |
| `taskqueue.chain.completed` | Counter | chain_type | Chains finished successfully |
| `taskqueue.chain.failed` | Counter | chain_type, failed_step | Chains failed (which step) |
| `taskqueue.chain.step_duration` | Histogram | chain_type, step_index | Per-step latency |
| `taskqueue.chain.total_duration` | Histogram | chain_type | End-to-end chain latency |

---

## 11. Comparison with Map-Reduce

| Dimension | Map-Reduce | Chained Tasks |
|-----------|-----------|---------------|
| Execution model | Parallel (fan-out) + barrier + reduce | Sequential (step-by-step) |
| Leader involvement | Heavy (barrier monitoring, CAS transitions) | None (handler-driven advancement) |
| Data flow | N inputs → N intermediates → 1 output | 1 input → transform → transform → 1 output |
| Failure impact | Partial data (policy-dependent) | Chain stops at failed step |
| Recovery | Replay dead-lettered map tasks | Replay dead-lettered step task |
| Use case | Batch processing, aggregation | Pipelines, workflows, multi-step processes |

---

## 12. Testing Strategy

| Test | Validates |
|------|-----------|
| Start 3-step chain, verify all 3 tasks execute in order | Basic sequencing |
| Step 1 output becomes step 2 input | Payload passing |
| Step 2 fails, verify chain status is FAILED and step 3 never enqueues | Failure policy: FAIL_CHAIN |
| Step 2 fails with SKIP_STEP policy, verify step 3 enqueues with step 1 output | Failure policy: SKIP_STEP |
| Replay dead-lettered step 2 task, verify chain resumes from step 2 through step 3 | Recovery via replay |
| Start chain, kill worker mid-step, verify reaper reclaims and chain eventually completes | Crash recovery |
| Start chain via scheduled trigger, verify chain_job created and first step enqueued | Schedule integration |
