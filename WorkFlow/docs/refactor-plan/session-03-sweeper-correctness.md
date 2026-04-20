# Session 3 — Sweeper Correctness

**Tier:** 1 (correctness — silent data issues)
**Prerequisites:** Session 2 (DEAD_LETTER status must exist)
**Estimated scope:** 3 fixes in TaskRepository/Sweeper + tests

---

## Items

### R1.3 — Fix sweeper WHERE guard allowing PENDING tasks to be marked FAILED

**Problem:** `updateStatusWithHandle` terminal branch guards with `WHERE status NOT IN ('COMPLETED', 'FAILED')`. But after `resetStaleTasks` resets a task to PENDING, `findStale` (separate transaction) can still find it. The subsequent `onTaskCompleted(..., FAILED)` call hits the terminal branch — `PENDING NOT IN ('COMPLETED', 'FAILED')` evaluates to true, so a PENDING task gets incorrectly marked FAILED.

**Files to modify:**
- `src/main/kotlin/engine/TaskRepository.kt` — terminal branch of `updateStatusWithHandle`

**Fix:**
```sql
-- Change:
WHERE id = :id AND status NOT IN ('COMPLETED', 'FAILED')

-- To:
WHERE id = :id AND status = 'PROCESSING'
```

This is stricter and correct: only PROCESSING tasks can transition to terminal states. A task already reset to PENDING will not match.

Note: With Session 2's `DEAD_LETTER` status, also exclude it. The `status = 'PROCESSING'` guard handles this implicitly.

**Test:**
1. Create a task with `status = PENDING`
2. Call `updateStatusWithHandle(taskId, FAILED, ...)`
3. Assert 0 rows affected — task remains PENDING

---

### R1.4 — Make `reclaimStaleTasks` and `findStale` atomic

**Problem:** `Sweeper.reclaimStaleTasks()` calls `resetStaleTasks()` (UPDATE) then `findStale()` (SELECT) in two separate transactions. Between them, a worker can claim a reset task and flip it back to PROCESSING. `findStale` then finds it and `onTaskCompleted(FAILED)` races with the worker.

**Files to modify:**
- `src/main/kotlin/engine/Sweeper.kt` — `reclaimStaleTasks()` method
- `src/main/kotlin/engine/TaskRepository.kt` — refactor to return exhausted tasks from the same operation

**Option A (preferred): Return exhausted tasks from `resetStaleTasks`**

Replace the two-step approach with a single method that:
1. Resets retryable tasks to PENDING (existing `resetStaleTasks` logic)
2. Returns the list of exhausted tasks (`retry_count >= max_retries`) from the same snapshot

```kotlin
// New method in TaskRepository
suspend fun reclaimAndFindExhausted(threshold: Instant): ReclaimResult

data class ReclaimResult(
    val reclaimed: Int,           // count of tasks reset to PENDING
    val exhausted: List<Task>,    // tasks at max retries, still PROCESSING
)
```

Implementation: use a single `Jdbi.inTransactionSuspend` that:
1. `UPDATE ... SET status = 'PENDING' WHERE ... AND retry_count < max_retries` (returns count)
2. `SELECT ... WHERE status = 'PROCESSING' AND claimed_at < :threshold AND retry_count >= max_retries` (returns list)

Both run in the same transaction, seeing the same snapshot.

**Option B (simpler): Add explicit filter to `findStale`**

Add `AND retry_count >= max_retries` to `findStale` query. This makes `findStale` self-documenting and removes the ordering dependency, though a narrow TOCTOU window still exists between transactions.

**Recommendation:** Option A for correctness, Option B as a quick interim fix.

**Sweeper update:**
```kotlin
private suspend fun reclaimStaleTasks(threshold: Instant) {
    val result = taskRepo.reclaimAndFindExhausted(threshold)
    if (result.reclaimed > 0) log.infof("Reclaimed %d stale tasks", result.reclaimed)

    // With Session 2's DEAD_LETTER, exhausted tasks are already handled by
    // deadLetterExhaustedTasks(). If that method exists, this step is redundant.
    // Otherwise, fail them via barrier:
    for (task in result.exhausted) {
        barrierService.onTaskCompleted(task.id, task.workflowId, task.sequenceNumber, TaskStatus.FAILED, null)
    }
}
```

**Test:**
1. Create two tasks: one with `retry_count < max_retries`, one with `retry_count >= max_retries`
2. Both `status = PROCESSING`, `claimed_at` past threshold
3. Call `reclaimAndFindExhausted(threshold)`
4. Assert first task reset to PENDING, second task returned in `exhausted` list
5. Run concurrently: start a claim on the first task while `reclaimAndFindExhausted` is executing — assert no task is incorrectly marked FAILED

---

### R1.7 — Fix `readLeaseTransitions` fallback

**Problem:** On K8s API failure during first acquisition, `readLeaseTransitions` catches the exception and returns `_epoch.value + 1`. Since `_epoch.value` starts at 0, this always returns 1 — regardless of the real `leaseTransitions` count (which could be 47). A false-low epoch breaks any downstream fencing check.

**Files to modify:**
- `src/main/kotlin/leader/LeaderManager.kt` — `readLeaseTransitions()` method (lines 231-233)

**Fix:**
```kotlin
} catch (e: Exception) {
    log.errorf(e, "Failed to read lease transitions — cannot determine epoch safely")
    // Return a value that will fail-safe: any fencing check comparing against
    // a previous epoch will accept this, and if the real epoch is higher,
    // the next successful read will correct it.
    val fallback = _epoch.value + 1
    if (fallback <= 1L) {
        // First acquisition with no prior state — we have no idea what the real
        // transitions count is. Log critical and use a sentinel.
        log.errorf("First acquisition with no prior epoch — fencing disabled until lease read succeeds")
    }
    fallback
}
```

Better approach: retry the read once before falling back.
```kotlin
} catch (e: Exception) {
    log.warnf(e, "First lease transitions read failed — retrying once")
    try {
        readLeaseTransitionsFromApi()
    } catch (retryEx: Exception) {
        log.errorf(retryEx, "Retry failed — falling back to local increment (epoch=%d)", _epoch.value + 1)
        _epoch.value + 1
    }
}
```

Also add a metric counter for fallback invocations:
```kotlin
meterRegistry.counter("leader_election_epoch_fallback_total").increment()
```

**Test:**
1. Mock K8s client to throw on first `leases().get()` call, succeed on second
2. Assert epoch comes from the retry, not from `_epoch.value + 1`
3. Mock both calls to throw — assert fallback value is logged at ERROR level

---

## Verification

1. `mvn test` passes
2. `SweeperTest` extended with TOCTOU race scenario
3. `LeaderManagerTest` extended with API failure retry scenario
4. No regression in existing barrier/workflow tests
