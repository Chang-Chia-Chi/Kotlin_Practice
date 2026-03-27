# Session 2 — Fencing Token & Dead-Letter Status

**Tier:** 1 (correctness — silent data issues)
**Prerequisites:** Session 1
**Estimated scope:** Schema migration + model changes + repository changes + tests

---

## Items

### R1.1 — Add `claim_id` fencing token

**Problem:** No fencing token exists to detect zombie handlers. When the reaper reclaims a task and another pod re-executes it, the original handler completes and applies side effects a second time. The `updateStatusWithHandle` WHERE guard prevents double-barrier fire but cannot prevent duplicate external side effects (API calls, DB writes, messages).

**Schema change (new migration `V2__add_claim_id_and_dead_letter.sql`):**
```sql
ALTER TABLE task ADD claim_id RAW(16);
```

**Files to modify:**
- `src/main/resources/db/migration/V2__add_claim_id_and_dead_letter.sql` — new file
- `src/main/kotlin/engine/WorkflowModels.kt` — add `claimId: UUID? = null` to `Task` data class
- `src/main/kotlin/engine/TaskRepository.kt`:
  - `claimNext`: generate `val claimId = UUID.randomUUID()` before UPDATE, bind as `claim_id` in the UPDATE, return `.copy(claimId = claimId)`
  - `updateStatusWithHandle`: add `AND (claim_id = :claimId OR :claimId IS NULL)` to WHERE clause for terminal transitions. Note: `claimId = null` bypasses the fence (by design, for Sweeper paths).
  - `resetStaleTasks`: clear `claim_id` (set NULL) when resetting to PENDING
  - `resetForRetry`: clear `claim_id`
  - `insertBatchWithHandle`: add `claim_id` to INSERT column list, bind `task.claimId` as RAW(16) or null
  - `mapTaskRow`: add `claimId = ci["CLAIM_ID"]?.let { readRawUuid(it) }` to map RAW(16) to UUID
- `src/main/kotlin/worker/TransitionHandler.kt` — add `claimId: UUID` to `HandlerInput`
- `src/main/kotlin/worker/WorkerLoop.kt` — pass `task.claimId` through to `HandlerInput`; on completion, pass `claimId` to barrier
- `src/main/kotlin/engine/BarrierService.kt` — `onTaskCompleted` accepts `claimId: UUID?` parameter, passes to `updateStatusWithHandle`
- `src/main/kotlin/engine/RowMapperUtils.kt` — add `readRawUuid(value: Any): UUID` to convert RAW(16) bytes to UUID

**Behavior change:**
- Claim path: generate fresh `claim_id`, write to DB alongside `claimed_at`, return in mapped Task
- Completion path: `updateStatusWithHandle` includes `AND (claim_id = :claimId OR :claimId IS NULL)` — if reaper has reset the task (clearing `claim_id`), the WHERE fails, `updated = false`, zombie handler aborts silently. When `claimId = null` (Sweeper paths), the fence is bypassed by design.
- Reaper path: `resetStaleTasks` sets `claim_id = NULL` as part of the reset

**Test:**
1. Claim a task, get `claimId = X`
2. Simulate reaper reset (clear `claim_id`, set status = PENDING)
3. Re-claim, get `claimId = Y`
4. Attempt completion with `claimId = X` — assert `updated = false`
5. Attempt completion with `claimId = Y` — assert `updated = true`

---

### R1.2 — Add `DEAD_LETTER` status

**Problem:** No `DEAD_LETTER` status exists. Tasks that exhaust retries strand in PROCESSING indefinitely — repeatedly found by `findExpired` with no action taken, never marked terminal, accumulating in the hot table.

**Schema change (same migration `V2__add_claim_id_and_dead_letter.sql`):**
```sql
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status
  CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'DEAD_LETTER'));
```

**Files to modify:**
- `src/main/kotlin/engine/WorkflowModels.kt`:
  ```kotlin
  enum class TaskStatus {
      PENDING, PROCESSING, COMPLETED, FAILED, DEAD_LETTER;
      val isTerminal: Boolean get() = this == COMPLETED || this == FAILED || this == DEAD_LETTER
  }
  ```
- `src/main/kotlin/engine/Sweeper.kt` — `reclaimStaleTasks` becomes `resetStaleTasks(threshold)` + `deadLetterExhaustedTasks(threshold)`. The existing `findStale` + per-task barrier loop is removed and replaced by the bulk `deadLetterExhaustedTasks` UPDATE.
- `src/main/kotlin/engine/TaskRepository.kt` — add `deadLetterExhaustedTasks` method (see SQL Changes table)
- `src/main/kotlin/engine/BarrierService.kt` — `updateStatusWithHandle` WHERE clause: add `'DEAD_LETTER'` to the NOT IN list

**Test:**
1. Create a task with `max_retries = 2`, `retry_count = 2`, `status = PROCESSING`, `claimed_at` past threshold
2. Run sweeper patrol
3. Assert task status is `DEAD_LETTER`, not stranded in PROCESSING
4. Assert `isTerminal` returns true for `DEAD_LETTER`

---

### R1.5 — Add status guard to non-terminal update branch

**Problem:** The non-terminal branch of `updateStatusWithHandle` issues `UPDATE task SET status = :status WHERE id = :id` with no status guard. A delayed retry acknowledgment could overwrite a COMPLETED row.

**Files to modify:**
- `src/main/kotlin/engine/TaskRepository.kt` — non-terminal branch (lines 193-197)

**Fix:**
```sql
UPDATE task SET status = :status, result = :result
WHERE id = :id AND status = 'PROCESSING'
```

**Test:** Set a task to COMPLETED, then attempt a non-terminal update — assert 0 rows affected.

---

## New Migration (SQL)

**File:** `src/main/resources/db/migration/V2__add_claim_id_and_dead_letter.sql`

```sql
-- R1.1: fencing token
ALTER TABLE task ADD claim_id RAW(16);

-- R1.2: dead-letter status
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status
  CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'DEAD_LETTER'));
```

---

## Changed Data Classes (Kotlin)

### `TaskStatus` (WorkflowModels.kt)
```kotlin
enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED, DEAD_LETTER;
    val isTerminal: Boolean get() = this == COMPLETED || this == FAILED || this == DEAD_LETTER
}
```

### `Task` (WorkflowModels.kt)
```kotlin
data class Task(
    val id: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val status: TaskStatus,
    val handlerKey: String,
    val payloadJson: String?,
    val resultJson: String?,
    val claimedBy: String?,
    val claimedAt: Instant?,
    val completedAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
    val claimId: UUID? = null,           // ← R1.1: explicit default null
)
```

### `HandlerInput` (TransitionHandler.kt)
```kotlin
data class HandlerInput(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val payload: String?,
    val claimId: UUID,                   // ← R1.1: non-null, always set at claim time
)
```

---

## Changed Method Signatures (Kotlin)

| Method | File | Change |
|---|---|---|
| `updateStatusWithHandle` | `TaskRepository.kt` | Add param `claimId: UUID? = null` |
| `onTaskCompleted` | `BarrierService.kt` | Add param `claimId: UUID? = null` |
| `recoverStuckWorkflow` | `BarrierService.kt` | Passes `claimId = null` to `updateStatusWithHandle` (Sweeper path, fence bypass by design) |

---

## New Methods (Kotlin)

### `TaskRepository.deadLetterExhaustedTasks`
```kotlin
suspend fun deadLetterExhaustedTasks(staleThreshold: Instant): Int
```
Bulk-moves exhausted stale tasks to `DEAD_LETTER`. See SQL Changes table.

### `RowMapperUtils.readRawUuid`
```kotlin
internal fun readRawUuid(value: Any): UUID
```
Converts Oracle `RAW(16)` bytes to `java.util.UUID`.

---

## SQL Changes (Comprehensive)

| Operation | Current SQL | New SQL | Item |
|---|---|---|---|
| `claimNext` UPDATE | `SET status, claimed_by, claimed_at` | `SET status = 'PROCESSING', claimed_by = :workerId, claimed_at = :now, claim_id = :claimId` where `claimId = UUID.randomUUID()` generated before UPDATE. Return `.copy(claimId = generatedId)`. | R1.1 |
| `updateStatusWithHandle` (terminal) | `WHERE id = :id AND status NOT IN ('COMPLETED', 'FAILED')` | `WHERE id = :id AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER') AND (claim_id = :claimId OR :claimId IS NULL)` | R1.1 + R1.2 |
| `updateStatusWithHandle` (non-terminal) | `WHERE id = :id` | `WHERE id = :id AND status = 'PROCESSING'` | R1.5 |
| `resetStaleTasks` | `SET status, claimed_by = NULL, claimed_at = NULL, retry_count` | `SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, claim_id = NULL, retry_count = retry_count + 1 WHERE status = 'PROCESSING' AND claimed_at < :threshold AND retry_count < max_retries` | R1.1 |
| `resetForRetry` | `SET status, claimed_by = NULL, claimed_at = NULL, retry_count` | `SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, claim_id = NULL, retry_count = :newRetryCount WHERE id = :id` | R1.1 |
| `deadLetterExhaustedTasks` | *(new)* | `UPDATE task SET status = 'DEAD_LETTER', completed_at = :now WHERE status = 'PROCESSING' AND claimed_at < :threshold AND retry_count >= max_retries` | R1.2 |
| `insertBatchWithHandle` INSERT | Column list excludes `claim_id` | Add `claim_id` to column list and VALUES; bind `task.claimId` as RAW(16) or null | R1.1 |
| `mapTaskRow` | No `claim_id` mapping | Add `claimId = ci["CLAIM_ID"]?.let { readRawUuid(it) }` | R1.1 |
| `countFailedWithHandle` | `status = 'FAILED'` | `status IN ('FAILED', 'DEAD_LETTER')` | R1.2 |
| `countNonTerminalWithHandle` | `status NOT IN ('COMPLETED', 'FAILED')` | `status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')` | R1.2 |

---

## Caller Impact Table (Comprehensive)

| Caller | File | Change |
|---|---|---|
| `WorkerLoop.processTask` | `WorkerLoop.kt` | Pass `task.claimId!!` to `HandlerInput`; pass `claimId = task.claimId` to `barrierService.onTaskCompleted` |
| `WorkerLoop.reportTaskFailed` | `WorkerLoop.kt` | Pass `claimId = task.claimId` to `barrierService.onTaskCompleted` |
| `BarrierService.onTaskCompleted` | `BarrierService.kt` | Accept `claimId: UUID? = null`; pass to `taskRepo.updateStatusWithHandle` |
| `BarrierService.recoverStuckWorkflow` | `BarrierService.kt` | Passes `claimId = null` to `updateStatusWithHandle` (fence bypass by design for Sweeper recovery) |
| `Sweeper.failExpiredTasks` | `Sweeper.kt` | Passes `claimId = null` to `barrierService.onTaskCompleted` (Sweeper path, fence bypass by design) |
| `Sweeper.reclaimStaleTasks` | `Sweeper.kt` | Replace `findStale` + per-task barrier loop with `taskRepo.deadLetterExhaustedTasks(threshold)` |
| `createTaskForActivity` | `WorkflowModels.kt` | No change needed — `claimId` defaults to `null`, tasks are born PENDING without a claim |
| Parallel task creation in `BarrierService.insertTasksForSequence` | `BarrierService.kt` | No change needed — `Task(...)` constructor uses default `claimId = null` |

---

## Removed/Replaced Code Paths

| Removed | Replaced By | Reason |
|---|---|---|
| `Sweeper.reclaimStaleTasks`: `findStale(threshold)` query + per-task `barrierService.onTaskCompleted(status=FAILED)` loop | Bulk `taskRepo.deadLetterExhaustedTasks(threshold)` returning count | R1.2: eliminates per-task overhead; marks exhausted tasks as `DEAD_LETTER` instead of routing through barrier as `FAILED`. The barrier still fires for these tasks when the Sweeper's `recoverStuckWorkflows` pass detects that all tasks are terminal. |
| `TaskRepository.findStale` | Retained but no longer called from `Sweeper.reclaimStaleTasks` | May be kept for diagnostics/metrics or removed if unused elsewhere. |

---

## Known Pre-existing Issue (Out of Scope)

**`resetForRetry` race:** A narrow window exists where `WorkerLoop.handleTaskFailure` calls `resetForRetry` concurrently with a Sweeper `resetStaleTasks` pass, potentially double-incrementing `retry_count`. This is pre-existing and out of scope for Session 2. Tracked for a future session.

---

## Verification

1. `mvn test` passes
2. Oracle container integration tests verify migration `V2` applies cleanly
3. New tests for claim_id fencing, DEAD_LETTER lifecycle, and status guard
