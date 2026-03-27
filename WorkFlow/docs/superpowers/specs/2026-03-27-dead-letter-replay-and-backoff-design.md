# Session 10 — Dead-Letter Replay & Exponential Backoff

**Date:** 2026-03-27
**Tier:** 4 (architecture improvements)
**Prerequisites:** Session 2 (DEAD_LETTER status)

---

## Overview

Two additions to the workflow engine:

1. **Dead-letter replay API** — programmatic way to replay failed tasks and workflows
2. **Exponential backoff via `not_before` column** — DB-level per-task retry delay that prevents poison-pill handlers from saturating the worker pool

---

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Backoff mechanism | DB-level `not_before` only | Dominant failure mode is downstream (not pod-local). Simpler, global, survives restarts. Sweeper already handles stuck PROCESSING tasks from bad pods. |
| Pod-level circuit breaker | Not included | Adds complexity for a rare failure case. YAGNI — can add later if monitoring shows need. |
| Replay advancement control | No flag — workflow status is the gate | FAILED workflows won't auto-advance (barrier CAS checks RUNNING). Operator calls `replayWorkflow` to re-enable. |
| Backoff formula | Fixed exponential: `2^retryCount` capped at 300s | Simple, no per-handler config needed. Option A from discussion. |
| `enqueued_at` column | Not in this session | Belongs to Session 5 (schema & query performance). Existing `ORDER BY claimed_at NULLS FIRST, id` suffices. |

---

## R4.4 — Dead-Letter Replay API

### Operations

**1. `TaskRepository.replayDeadLetterTask(taskId: String): Boolean`**

Resets a single DEAD_LETTER task to PENDING.

```sql
UPDATE task
SET status = 'PENDING',
    retry_count = 0,
    claimed_by = NULL,
    claimed_at = NULL,
    completed_at = NULL,
    result = NULL,
    not_before = NULL
WHERE id = :taskId AND status = 'DEAD_LETTER'
```

- Returns `true` if 1 row updated, `false` if task not found or not in DEAD_LETTER
- Idempotent: replaying an already-replayed task (now PENDING) returns `false`
- Sets `not_before = NULL` so the task is immediately claimable
- Resets `retry_count = 0` for a fresh set of retries

**2. `TaskRepository.replayDeadLetterBatch(workflowId: String): Int`**

Same reset for all DEAD_LETTER tasks in a workflow.

```sql
UPDATE task
SET status = 'PENDING',
    retry_count = 0,
    claimed_by = NULL,
    claimed_at = NULL,
    completed_at = NULL,
    result = NULL,
    not_before = NULL
WHERE workflow_id = :workflowId AND status = 'DEAD_LETTER'
```

Returns count of replayed tasks.

**3. `TaskRepository.replayDeadLetterBatchWithHandle(handle, workflowId): Int`**

Same as above but accepts an existing `Handle` for use within a transaction.

**4. `WorkflowEngine.replayWorkflow(workflowId: String): Boolean`**

Transactional workflow-level replay:

```kotlin
suspend fun replayWorkflow(workflowId: String): Boolean {
    return jdbi.inTransactionSuspend { handle ->
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: return@inTransactionSuspend false
        if (workflow.status != WorkflowStatus.FAILED) return@inTransactionSuspend false

        workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.RUNNING)
        taskRepo.replayDeadLetterBatchWithHandle(handle, workflowId)
        true
    }
}
```

- Verifies workflow is FAILED before proceeding
- Resets workflow to RUNNING so barrier CAS can advance again
- Replays all dead-lettered tasks atomically in one transaction
- Returns `false` if workflow not found or not FAILED

### Advancement Behavior

No special flag or suppression mechanism. The workflow status itself gates advancement:

- **Workflow is FAILED:** Replayed tasks execute and complete, but `barrierService.onTaskCompleted` checks `workflow.status == RUNNING` before attempting CAS advance. Results are stored but no advancement occurs.
- **Workflow is RUNNING (via `replayWorkflow`):** Normal advancement resumes. Replayed tasks complete → barrier probes → CAS advances if all tasks at that sequence are terminal.

**Required fix:** `BarrierService.onTaskCompleted` currently does NOT check workflow status before CAS advance (unlike `recoverStuckWorkflow` which does at line 88). Add `if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend` after loading the workflow. Without this, replaying a task while the workflow is FAILED would auto-advance — breaking the cautious replay workflow.

Operator workflow for cautious replay:
1. `replayDeadLetterTask(taskId)` — replay individual tasks while workflow stays FAILED
2. Verify results in the `result` column
3. `replayWorkflow(workflowId)` — reset workflow to RUNNING and replay remaining dead letters

---

## R4.7 — Exponential Backoff via `not_before` Column

### Schema Change

Migration `V3__add_not_before.sql`:

```sql
ALTER TABLE task ADD not_before TIMESTAMP;
CREATE INDEX idx_task_not_before ON task (status, not_before);
```

- Nullable — `NULL` means immediately claimable (default for new tasks)
- Only set on retry via `resetForRetry`

### Claim Query Change

Inner SELECT in `TaskRepository.claimNext` adds one filter:

```sql
SELECT id FROM task
WHERE status = 'PENDING'
  AND (deadline_at IS NULL OR deadline_at > :now)
  AND (not_before IS NULL OR not_before < :now)
ORDER BY claimed_at NULLS FIRST, id
FETCH FIRST :limit ROWS ONLY
```

Tasks with `not_before` in the future are invisible to the claim query — they stay in PENDING but aren't claimable until the backoff expires.

### Backoff Computation in `resetForRetry`

```sql
UPDATE task
SET status = 'PENDING',
    claimed_by = NULL,
    claimed_at = NULL,
    retry_count = :newRetryCount,
    not_before = SYSTIMESTAMP + NUMTODSINTERVAL(LEAST(POWER(2, :newRetryCount), 300), 'SECOND')
WHERE id = :id
```

Backoff progression by retry count:

| retry_count | Backoff |
|-------------|---------|
| 1 | 2s |
| 2 | 4s |
| 3 | 8s |
| 4 | 16s |
| 5 | 32s |
| 6 | 64s |
| 7 | 128s |
| 8 | 256s |
| 9+ | 300s (cap) |

### WorkerLoop Impact

**None.** WorkerLoop is unchanged. The backoff is entirely DB-side:
- `resetForRetry` sets `not_before`
- `claimNext` filters by `not_before`
- WorkerLoop never sees backed-off tasks

### Replay Clears Backoff

All replay SQL sets `not_before = NULL` so replayed tasks are immediately claimable. A replayed task gets a fresh start — no residual backoff from its previous life.

### Sweeper Impact

Sweeper's `reclaimStaleTasks` also calls a form of retry reset. It should also set `not_before` with backoff so reclaimed stale tasks don't immediately re-saturate.

---

## Files to Modify

| File | Change |
|------|--------|
| `src/main/resources/db/migration/V3__add_not_before.sql` | New migration: add `not_before` column + index |
| `src/main/kotlin/engine/TaskRepository.kt` | Add `not_before` filter to `claimNext`; update `resetForRetry` to set `not_before`; add `replayDeadLetterTask`, `replayDeadLetterBatch`, `replayDeadLetterBatchWithHandle` |
| `src/main/kotlin/engine/BarrierService.kt` | Add `if (workflow.status != WorkflowStatus.RUNNING) return` guard in `onTaskCompleted` after loading workflow (line ~48) — matches existing guard in `recoverStuckWorkflow` |
| `src/main/kotlin/engine/WorkflowEngine.kt` | Add `replayWorkflow` |
| `src/main/kotlin/engine/WorkflowModels.kt` | Add `notBefore: Instant?` field to `Task` data class |
| `src/main/kotlin/engine/Sweeper.kt` | Update stale reclaim SQL to set `not_before` with backoff |
| `src/test/kotlin/engine/RepositoryTest.kt` | Tests for replay (single, batch, idempotency) and `not_before` filtering in claim |
| `src/test/kotlin/worker/WorkerLoopTest.kt` | Test that backed-off tasks are not claimed; poison-pill handler doesn't starve healthy handlers |

---

## Testing Plan

### Dead-Letter Replay Tests

1. **Single replay:** Create task → exhaust retries → verify DEAD_LETTER → replay → assert PENDING, retry_count=0, claim fields null, not_before null
2. **Idempotency:** Replay an already-replayed task (now PENDING) → assert returns false
3. **Batch replay:** Create workflow with multiple dead-lettered tasks → batch replay → assert all PENDING
4. **Workflow replay:** Create FAILED workflow with dead-lettered tasks → `replayWorkflow` → assert workflow RUNNING, all tasks PENDING
5. **Workflow replay guard:** Call `replayWorkflow` on a RUNNING workflow → assert returns false

### Backoff Tests

6. **`not_before` set on retry:** Fail a task → assert `not_before` is set to approximately `now + 2^retryCount` seconds
7. **Claim filters `not_before`:** Insert task with `not_before` in the future → `claimNext` returns empty → advance time past `not_before` → `claimNext` returns the task
8. **Poison-pill isolation:** Register a failing handler and a succeeding handler → run both → assert the succeeding handler's tasks are not starved (they remain claimable while failing handler's tasks are backed off)
9. **Replay clears backoff:** Dead-letter a task (has `not_before` set) → replay → assert `not_before` is NULL, task is immediately claimable
