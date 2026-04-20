# Session 1 — Critical Crash & Corruption Fixes

**Tier:** 0 (must fix before any deployment)
**Prerequisites:** None
**Estimated scope:** 3 small, focused fixes + tests

---

## Items

### R0.1 — Oracle null binding on CLOB column

**Problem:** `TaskRepository.updateStatusWithHandle` binds `.bind("result", resultJson)` where `resultJson: String?`. When null, this becomes `.bind("result", null)` which throws on Oracle JDBC for CLOB columns. This path is hit by every task failure from `Sweeper.failExpiredTasks()` and `Sweeper.reclaimStaleTasks()`.

**Files to modify:**
- `src/main/kotlin/engine/TaskRepository.kt` — lines 187-190 (terminal branch) and 195-198 (non-terminal branch)
- `src/main/kotlin/engine/TaskRepository.kt` — line 288 (`insertBatchWithHandle`)

**Fix:**
```kotlin
// Replace:
.bind("result", resultJson)

// With:
.let { if (resultJson != null) it.bind("result", resultJson) else it.bindNull("result", java.sql.Types.CLOB) }
```

Apply to all three locations. Also audit `insertBatchWithHandle` for `task.resultJson` null binding.

**Test:** Add a test in `RepositoryTest` that calls `updateStatusWithHandle` with `resultJson = null` against the Oracle container — must not throw.

---

### R0.2 — Zero drain window on shutdown

**Problem:** `WorkerLoop.shutdown()` calls `activeJob?.cancelAndJoin()` immediately after signaling stop. This cancels all in-flight handler coroutines at the next suspension point, discarding work. The intended sequence (stop claiming → drain in-flight → force-cancel) is not implemented.

**Files to modify:**
- `src/main/kotlin/worker/WorkerLoop.kt` — `shutdown()` method (lines 125-130)

**Fix:**
```kotlin
override suspend fun shutdown() {
    _accepting.set(false)
    stopChannel.trySend(Unit)
    // Drain: wait for in-flight handlers to finish naturally.
    // takeUntilSignal closes the channelFlow; existing launches run to completion
    // as long as the scope is not cancelled.
    withTimeoutOrNull(shutdownTimeout.toMillis()) {
        activeJob?.join()
    }
    // Force-cancel any stragglers after drain window expires.
    activeJob?.cancelAndJoin()
}
```

`shutdownTimeout` should come from `ShutdownParticipant.shutdownTimeout` (already on the interface).

**Test:** In `WorkerLoopTest`, register a handler that takes 2 seconds, start it, trigger shutdown, and assert:
1. The handler completes (not cancelled) within the drain window.
2. After drain timeout, force-cancel fires.

---

### R0.3 — Handle leak on CancellationException in transactions

**Problem:** `Jdbi.inTransactionSuspend` wraps the blocking call in `withContext(Dispatchers.IO)`. If the outer coroutine is cancelled (e.g., during shutdown), `withContext` transitions to cancelled state. JDBI's internal `inTransaction` catches `Throwable` for rollback, but the `CancellationException` propagation path may leave the Handle open without a clean rollback signal.

**Files to modify:**
- `src/main/kotlin/extension/JdbiExtension.kt` — `inTransactionSuspend` and `useTransactionSuspend`

**Fix:**
```kotlin
suspend fun <R, X : Exception> Jdbi.inTransactionSuspend(callback: HandleCallback<R, X>): R =
    withContext(Dispatchers.IO + NonCancellable) { inTransaction(callback) }

suspend fun <X : Exception> Jdbi.useTransactionSuspend(callback: HandleConsumer<X>) =
    withContext(Dispatchers.IO + NonCancellable) { useTransaction(callback) }
```

`NonCancellable` ensures the transaction commits or rolls back cleanly before cancellation is honoured. The IO thread is held slightly longer during shutdown — acceptable.

**Test:** In a new test, start a transaction coroutine, cancel the parent scope mid-transaction, and verify the Handle is closed and the transaction is rolled back (not leaked).

---

## Verification

After all three fixes:
1. `mvn test` — all existing tests pass
2. New tests for each fix pass against Oracle container
3. Manual smoke test: start `mvn quarkus:dev`, submit a workflow, send SIGTERM mid-execution, verify in-flight task completes and DB state is consistent
