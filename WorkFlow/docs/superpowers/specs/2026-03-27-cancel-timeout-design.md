# Session 9 — Workflow Cancel & DAG-Level Timeout

**Tier:** 4 (architecture improvements)
**Prerequisites:** Session 2 (DEAD_LETTER status), Session 5 (enqueued_at column)
**Date:** 2026-03-27

---

## Overview

Add explicit state machine enforcement, workflow cancel API, DAG-level wall-clock timeout, and distinct TIMED_OUT/CANCELLED statuses for both workflows and tasks.

---

## 1. State Machine Infrastructure

### WorkflowStatus

```
RUNNING ──→ COMPLETED     (all sequences done)
   │
   ├──→ FAILED            (ABORT failure policy triggered)
   ├──→ TIMED_OUT         (deadline_at exceeded)
   └──→ CANCELLED         (cancel API)

Future (workflow reclaim):
   FAILED ──→ RUNNING
   TIMED_OUT ──→ RUNNING
   CANCELLED ──→ RUNNING
```

### TaskStatus

```
PENDING ──→ PROCESSING      (worker claims)
  │
  └──→ CANCELLED            (workflow abort/timeout/cancel cascades)

PROCESSING ──→ COMPLETED    (handler success)
   │
   ├──→ FAILED              (handler failure)
   ├──→ TIMED_OUT           (deadline expired)
   ├──→ PENDING             (stale reclaim, retry)
   └──→ DEAD_LETTER         (stale + retries exhausted)

Future (retry-on-failure):
   FAILED ──→ PENDING
   FAILED ──→ DEAD_LETTER
```

### Implementation

Both enums carry a `companion object` with:
- `allowed: Set<Pair<Status, Status>>` — the complete transition table (including future transitions)
- `requireTransition(from, to)` — throws `IllegalArgumentException` on illegal transitions
- `isTerminal` property — `WorkflowStatus`: anything not `RUNNING`; `TaskStatus`: `COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED`

### WorkflowRepository.updateStatusWithHandle (generalized)

```kotlin
fun updateStatusWithHandle(
    handle: Handle, id: String,
    newStatus: WorkflowStatus,
    expectedStatus: WorkflowStatus,
): Boolean
```

- Calls `requireTransition(expectedStatus, newStatus)` before SQL
- SQL: `UPDATE workflow SET status = :newStatus, updated_at = :now WHERE id = :id AND status = :expectedStatus`
- Returns `true` if 1 row updated, `false` if race lost (another writer transitioned first)
- **All callers must check the return value before proceeding with side effects (e.g., cancelling tasks)**

---

## 2. Schema Migration (V5__cancelled_and_timeout.sql)

```sql
-- Task: add TIMED_OUT, CANCELLED statuses
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status
    CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED'));

-- Workflow: add TIMED_OUT, CANCELLED statuses
ALTER TABLE workflow DROP CONSTRAINT chk_workflow_status;
ALTER TABLE workflow ADD CONSTRAINT chk_workflow_status
    CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'CANCELLED'));

-- Workflow: add deadline_at for DAG-level timeout (default 1 hour)
ALTER TABLE workflow ADD deadline_at TIMESTAMP;
UPDATE workflow SET deadline_at = created_at + INTERVAL '1' HOUR WHERE deadline_at IS NULL;
ALTER TABLE workflow MODIFY deadline_at NOT NULL;

-- Index for sweeper to find timed-out workflows
CREATE INDEX idx_workflow_deadline ON workflow (status, deadline_at);
```

---

## 3. DSL & Model Changes

### WorkflowDefinition

```kotlin
data class WorkflowDefinition(
    val activities: List<ActivityDefinition>,
    val deadline: Duration = Duration.ofHours(1),  // DAG-level wall-clock timeout
)
```

### WorkflowBuilder

```kotlin
class WorkflowBuilder {
    private val activities = mutableListOf<ActivityDefinition>()
    private var deadline: Duration = Duration.ofHours(1)

    fun deadline(d: Duration) { deadline = d }

    fun build(): WorkflowDefinition {
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
        WorkflowDefinition(activities, deadline)
    }
}
```

### WorkflowRun

```kotlin
data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val currentSequence: Int,
    val version: Int,
    val status: WorkflowStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val deadlineAt: Instant,  // NOT nullable — every workflow has a deadline
)
```

### WorkflowEngine.startWorkflow

Computes `deadline_at = now + definition.deadline` at workflow creation time. Binds to INSERT.

### WorkflowRepository

- `insertWithHandle`: binds `deadline_at` (always non-null, no bindNull needed)
- Row mapper: handles `deadline_at` with Oracle TIMESTAMP reflection

---

## 4. Cancel API

### WorkflowEngine.cancelWorkflow

```kotlin
suspend fun cancelWorkflow(workflowId: String): Boolean {
    return jdbi.inTransactionSuspend { handle ->
        val updated = workflowRepo.updateStatusWithHandle(
            handle, workflowId, WorkflowStatus.CANCELLED, expectedStatus = WorkflowStatus.RUNNING
        )
        if (updated) {
            taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
        }
        updated
    }
}
```

### TaskRepository.cancelPendingTasksWithHandle

```kotlin
fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int {
    return handle.createUpdate("""
        UPDATE task SET status = 'CANCELLED', completed_at = :now
        WHERE workflow_id = :workflowId AND status = 'PENDING'
    """)
        .bind("workflowId", workflowId)
        .bind("now", LocalDateTime.now(ZoneOffset.UTC))
        .execute()
}
```

### Behaviors

- **Atomic**: workflow status + task cancellation in one transaction
- **Race-safe**: `expectedStatus = RUNNING` — returns `false` if already transitioned
- **PROCESSING tasks untouched**: in-flight tasks complete, hit barrier CAS, get silently rejected
- **Return value**: `true` if cancelled, `false` if not found or not RUNNING

---

## 5. Barrier ABORT Path

In `BarrierService.advanceWorkflow`, when failure policy is ABORT:

```kotlin
FailurePolicy.ABORT -> {
    val updated = workflowRepo.updateStatusWithHandle(
        handle, workflow.id, WorkflowStatus.FAILED, expectedStatus = WorkflowStatus.RUNNING
    )
    if (updated) {
        taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
    }
    return
}
```

Same guard pattern — check boolean before cancelling. If another writer already transitioned the workflow, no double-cancel.

---

## 6. Sweeper Timeout Enforcement

### WorkflowRepository.findTimedOut (new, separate from findStuck)

```kotlin
suspend fun findTimedOut(): List<WorkflowRun>
```

```sql
SELECT * FROM workflow WHERE status = 'RUNNING' AND deadline_at < :now
```

Separate from `findStuck` — different semantics (stuck = barrier didn't advance; timed out = wall clock exceeded).

### Sweeper.expireOverdueWorkflows (new)

```kotlin
private suspend fun expireOverdueWorkflows() {
    val timedOut = workflowRepo.findTimedOut()
    for (workflow in timedOut) {
        try {
            jdbi.inTransactionSuspend { handle ->
                val updated = workflowRepo.updateStatusWithHandle(
                    handle, workflow.id, WorkflowStatus.TIMED_OUT, expectedStatus = WorkflowStatus.RUNNING
                )
                if (updated) {
                    taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                    log.warnf("Workflow %s timed out (deadline was %s)", workflow.id, workflow.deadlineAt)
                }
            }
        } catch (e: Exception) {
            log.error("Failed to time out workflow {}", workflow.id, e)
        }
    }
}
```

### Sweeper.expireOverdueTasks (renamed from failExpiredTasks)

Existing logic, but now sets task status to `TIMED_OUT` instead of `FAILED`.

### Sweep cycle order

```kotlin
expireOverdueTasks()         // task deadline enforcement
reclaimStaleTasks()          // stale reclaim
deadLetterExhaustedTasks()   // DLQ exhausted retries
recoverStuckWorkflows()      // stuck barrier recovery
expireOverdueWorkflows()     // workflow deadline enforcement (NEW)
```

---

## 7. Race Analysis

| Writer 1 | Writer 2 | Outcome |
|---|---|---|
| Barrier: COMPLETED | Cancel API: CANCELLED | One wins CAS, other gets `false`. No corruption. |
| Barrier: FAILED (ABORT) | Sweeper: TIMED_OUT | One wins, other no-ops. |
| Cancel API: CANCELLED | Sweeper: TIMED_OUT | One wins, other no-ops. |
| Worker: claims PENDING task | Cancel: CANCELLED | Claim wins → task PROCESSING, cancel skips it. Cancel wins → task CANCELLED, claim finds nothing. |

All races resolve safely via `expectedStatus` guard + boolean check before side effects.

---

## 8. Tests

### Test 1 — State machine validation
- Assert all allowed transitions succeed via `requireTransition`
- Assert illegal transitions throw `IllegalArgumentException`
- Covers both `WorkflowStatus` and `TaskStatus`

### Test 2 — ABORT cancels sibling tasks (parallel fan-out)
- Create workflow with PARALLEL fan-out of 5 tasks
- Fail one task with `failurePolicy = ABORT`
- Assert workflow status is `FAILED`
- Assert remaining 4 PENDING tasks are `CANCELLED`
- Assert no worker claims them after cancellation

### Test 3 — Cancel API
- Start a multi-step workflow
- Call `cancelWorkflow(id)` mid-execution
- Assert workflow status is `CANCELLED`
- Assert remaining PENDING tasks are `CANCELLED`
- Assert `cancelWorkflow` on already-cancelled returns `false`
- Assert `cancelWorkflow` on COMPLETED returns `false`

### Test 4 — Workflow deadline enforcement
- Create workflow with `deadline = Duration.ofSeconds(1)`
- Register a handler that blocks long enough for sweeper to detect
- Trigger sweeper cycle (or Awaitility)
- Assert workflow status is `TIMED_OUT`
- Assert pending tasks are `CANCELLED`

### Test 5 — Task deadline (TIMED_OUT)
- Create a task with short `deadline_at`
- Let it stay in PROCESSING past deadline
- Trigger sweeper's `expireOverdueTasks()`
- Assert task status is `TIMED_OUT` (not `FAILED`)

### Test 6 — Race safety
- Start a workflow, have it complete naturally
- Simultaneously attempt `cancelWorkflow(id)`
- Assert cancel returns `false`
- Assert workflow stays `COMPLETED`, no tasks cancelled

All async assertions use `Awaitility.await().untilAsserted(...)`.
