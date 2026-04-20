# Session 2 — Dead-Letter Status & Status Guards

**Tier:** 1 (correctness — silent data issues)
**Prerequisites:** Session 1
**Estimated scope:** Schema migration + model changes + repository changes + tests

---

## Items

### R1.2 — Add `DEAD_LETTER` status

**Problem:** No `DEAD_LETTER` status exists. Tasks that exhaust retries strand in PROCESSING indefinitely — repeatedly found by `findExpired` with no action taken, never marked terminal, accumulating in the hot table.

**Schema change (migration `V2__add_dead_letter.sql`):**
```sql
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status
  CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'DEAD_LETTER'));
```

**Model change (`WorkflowModels.kt`):**
```kotlin
enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED, DEAD_LETTER;
    val isTerminal: Boolean get() = this == COMPLETED || this == FAILED || this == DEAD_LETTER
}
```

**Repository changes (`TaskRepository.kt`):**
- `updateStatusWithHandle` (terminal): add `'DEAD_LETTER'` to NOT IN list
- `countNonTerminalWithHandle`: add `'DEAD_LETTER'` to NOT IN list
- `countFailedWithHandle`: `status IN ('FAILED', 'DEAD_LETTER')`
- New `deadLetterExhaustedTasks(staleThreshold)`: bulk UPDATE PROCESSING → DEAD_LETTER for tasks where `retry_count >= max_retries`

**Sweeper change (`Sweeper.kt`):**
- `reclaimStaleTasks`: replace `findStale` + per-task barrier loop with bulk `deadLetterExhaustedTasks`. The barrier still fires for these tasks when `recoverStuckWorkflows` detects all tasks are terminal.

---

### R1.5 — Add status guard to non-terminal update branch

**Problem:** The non-terminal branch of `updateStatusWithHandle` issues `UPDATE task SET status = :status WHERE id = :id` with no status guard. A delayed retry acknowledgment could overwrite a COMPLETED row.

**Fix (`TaskRepository.kt`):**
```sql
UPDATE task SET status = :status, result = :result
WHERE id = :id AND status = 'PROCESSING'
```

---

### Zombie guard via `(claimed_by, claimed_at)`

**Problem:** When the reaper reclaims a task and another pod re-executes it, the original zombie handler could overwrite the new handler's in-progress state because the terminal WHERE clause only checks status, not ownership.

**Approach:** Instead of a dedicated `claim_id` fencing token (UUID column + RAW(16) conversion), we use the existing `(claimed_by, claimed_at)` tuple as a lightweight fence. The pair is unique per claim because the reaper resets both to NULL, and any re-claim writes fresh values. Even if the same pod re-claims, `claimed_at` differs (separated by the reaper timeout — minutes apart, microsecond precision).

**Fix (`TaskRepository.updateStatusWithHandle` terminal branch):**
```sql
WHERE id = :id
  AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
  AND (claimed_by = :claimedBy AND claimed_at = :claimedAt OR :claimedBy IS NULL)
```

When `claimedBy = null` (Sweeper paths), the fence is bypassed by design.

**Caller changes:**
- `BarrierService.onTaskCompleted`: accepts optional `claimedBy`/`claimedAt`, passes to `updateStatusWithHandle`
- `WorkerLoop.processTask` / `reportTaskFailed`: passes `task.claimedBy` and `task.claimedAt`
- `Sweeper.failExpiredTasks`: passes null (fence bypass)

---

## Verification

1. `mvn test` passes
2. Oracle container integration tests verify migration `V2` applies cleanly
3. Tests for DEAD_LETTER lifecycle, status guard, and zombie guard
