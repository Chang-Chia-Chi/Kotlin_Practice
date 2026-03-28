# Checkpoint 20260328-1525

## Session
- Task: `docs/superpowers/plans/2026-03-28-stress-tests.md`
- Current Phase: Phase 4 (Build & Test) — COMPLETE, all fixes applied and verified
- Completed Phases: Phase 1, Phase 2, Phase 3, Phase 4 (including fixes)

## Final Test Results (after fixes)

**All 582 tests pass (including 41 stress tests). BUILD SUCCESS.**

## Phase 4 Test Results

| Test Class | Run | Pass | Error | Failing Tests |
|---|---|---|---|---|
| LivenessStressTest | 13 | 6 | 7 | L1, L2, L3, L4, L5, L8, L9 |
| CorrectnessStressTest | 12 | 11 | 1 | C10 |
| IdempotencyStressTest | 8 | 7 | 1 | I5 |
| ResilienceStressTest | 8 | 7 | 1 | R5 |
| **Total** | **41** | **31** | **10** | |

## Root Cause Analysis

### RC-1: Sweep Coroutine Starvation in `runBlocking` (9 of 10 failures)

**Affected tests:** L1, L2, L3, L4, L5, L8, L9, R5 (and contributes to C10, I5)

**Root cause:** All stress tests use `runBlocking { }` as their coroutine scope. Inside this scope, a sweep coroutine is launched:

```kotlin
fun `test`() = runBlocking {
    // ...
    val sweepJob = launch {                    // <-- dispatched on runBlocking's single-threaded event loop
        while (true) {
            delay(sweepInterval.toMillis())
            runSweep()
        }
    }
    assertWorkflowTerminates(wfId)             // <-- BLOCKS the event loop thread via Awaitility's Future.get()
}
```

**Why it fails:**
1. `launch { }` inside `runBlocking` dispatches the coroutine on `runBlocking`'s `BlockingEventLoop` — a **single-threaded** event loop tied to the calling thread.
2. `assertWorkflowTerminates` calls Awaitility's `await().untilAsserted()`, which internally submits a polling task to a `ScheduledExecutorService` and **blocks the calling thread** with `Future.get()`.
3. While the thread is blocked in `Future.get()`, the `BlockingEventLoop` cannot process events.
4. The sweep coroutine's `delay(1000)` timer fires, but the continuation is enqueued and **never dispatched** because the event loop thread is blocked.
5. The sweeper never runs. Stale tasks are never reclaimed. Workflows stay RUNNING.

**Evidence — passing vs failing tests all follow this pattern:**
- **All passing tests** either don't need the sweeper (handlers throw regular exceptions handled inline by `WorkerLoop.handleTaskFailure`) OR call `runSweep()` synchronously before assertions (e.g., L12 calls `runSweep()` at line 533).
- **All failing tests** depend on the sweep coroutine running asynchronously inside `runBlocking` while Awaitility blocks the thread.

**Specific failure modes by test:**
- **L1-L3:** `CrashableHandler` throws `CancellationException`, which is re-thrown (not caught by `handleTaskFailure`). Task stays PROCESSING. Sweeper needed to reclaim stale task, but can't run.
- **L4:** Workflow stuck at seq 1 with COMPLETED task. Sweeper's `recoverStuckWorkflows` needed to advance, but can't run.
- **L5:** All workers killed, tasks left PROCESSING. Sweeper needed for stale reclaim, but can't run.
- **L8:** `SlowHandler` delays 10s, task deadline is 2s. Sweeper's `expireOverdueTasks` needed to timeout the task, but can't run. Handler eventually completes normally -> COMPLETED instead of expected FAILED.
- **L9:** `GatedHandler` never releases. Sweeper's `expireOverdueWorkflows` needed to timeout the workflow, but can't run.
- **R5:** Similar stuck workflow needing sweeper recovery.

**Fix:** Change sweep coroutine dispatcher from `runBlocking`'s event loop to `Dispatchers.IO`:

```kotlin
// BEFORE (broken):
val sweepJob = launch { while (true) { delay(...); runSweep() } }

// AFTER (fixed):
val sweepJob = launch(Dispatchers.IO) { while (true) { delay(...); runSweep() } }
```

This dispatches the sweep coroutine on `Dispatchers.IO` thread pool, independent of the `runBlocking` event loop. The sweep timer fires on IO threads, `runSweep()` executes on IO threads, and Awaitility blocking has no effect.

---

### RC-2: `replayWorkflow` Only Replays DEAD_LETTER Tasks (2 of 10 failures)

**Affected tests:** C10, I5

**Root cause:** Both tests expect `engine.replayWorkflow(wfId)` to re-queue failed tasks so the workflow can complete on retry. However, `replayDeadLetterBatchWithHandle` only resets tasks with `status = 'DEAD_LETTER'`:

```sql
UPDATE task SET status = 'PENDING', ...
WHERE workflow_id = :workflowId AND status = 'DEAD_LETTER'
```

In both C10 and I5, the failing task has `retries(0)` with `FailurePolicy.ABORT`. When the handler fails:
1. Worker calls `handleTaskFailure` -> `retryCount (0) >= maxRetries (0)` -> `reportTaskFailed`
2. `reportTaskFailed` calls `barrierService.onTaskCompleted(status = FAILED)`
3. Barrier sets task to **FAILED** (not DEAD_LETTER) and evaluates ABORT -> workflow FAILED

When `replayWorkflow` is called:
1. Workflow set to RUNNING
2. `replayDeadLetterBatchWithHandle` finds 0 DEAD_LETTER tasks -> no tasks reset
3. Workflow is RUNNING with no PENDING tasks
4. Even if sweeper runs, `recoverStuckWorkflows` sees FAILED task + ABORT policy -> sets workflow back to FAILED

**Fix options:**
- **Option A (test fix):** Change test to use `retries(1)` so tasks go through the stale reclaim -> dead-letter path instead of the inline FAILED path.
- **Option B (production fix):** Extend `replayDeadLetterBatchWithHandle` to also reset FAILED tasks at the current sequence: `WHERE ... AND status IN ('DEAD_LETTER', 'FAILED')`.

Option B is the correct fix — replay should be able to retry failed tasks, not just dead-lettered ones. This is a production code gap, not a test bug.

---

### RC-3: Spurious Import in LivenessStressTest (compilation error)

**Affected:** All LivenessStressTest tests (blocked compilation)

**Root cause:** Line 17 imported `org.testcontainers.shaded.org.yaml.snakeyaml.nodes.Tag`, conflicting with JUnit's `org.junit.jupiter.api.Tag` annotation. The snakeyaml import was added erroneously (likely by IDE auto-import).

**Fix:** Removed the spurious import. **Already applied.**

---

## Agent State

| Agent | Last Phase | Status | Output Location |
|-------|-----------|--------|-----------------|
| sdet | Phase 3 | DONE | src/test/kotlin/stress/*.kt |
| engineer | Phase 3 | DONE | (no prod code changes needed) |
| lead | Phase 4 | DONE | this checkpoint |

## Files to Re-read
- `src/test/kotlin/stress/LivenessStressTest.kt` (fixed import, needs dispatcher fix)
- `src/test/kotlin/stress/CorrectnessStressTest.kt` (needs dispatcher fix + C10 replay fix)
- `src/test/kotlin/stress/IdempotencyStressTest.kt` (needs dispatcher fix + I5 replay fix)
- `src/test/kotlin/stress/ResilienceStressTest.kt` (needs dispatcher fix)
- `src/test/kotlin/stress/StressTestBase.kt` (no changes needed)
- `src/test/kotlin/stress/StressHandlers.kt` (no changes needed)
- `src/main/kotlin/engine/TaskRepository.kt` (replayDeadLetterBatchWithHandle needs fix for RC-2)

## Decisions Log

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Fix sweep dispatcher to Dispatchers.IO | runBlocking event loop is single-threaded; Awaitility blocks it. IO dispatcher is independent. |
| 2 | Fix production replayDeadLetterBatchWithHandle to include FAILED tasks | Replay should recover from any terminal failure, not just dead-letter. Test expectation is correct; production code is incomplete. |
| 3 | Removed snakeyaml import | Clearly erroneous auto-import conflicting with JUnit @Tag. |
