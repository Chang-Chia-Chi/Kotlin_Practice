# Phase 4: Watchdog Batching

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce per-item transaction overhead in the watchdog's `expireOverdueWorkflows` by batching into a single transaction with bulk SQL.

**Architecture:** Replace the fetch-then-loop-with-individual-transactions pattern in `expireOverdueWorkflows` with two bulk SQL statements in one transaction. `expireOverdueTasks` cannot be batched (each task needs its own barrier evaluation) so it gets concurrent processing instead.

**Tech Stack:** Kotlin, JDBI, Oracle SQL, Kotlin Coroutines, JUnit 5

**Depends on:** Phase 1 (no direct dependency, but should be applied first for clean diffs)

---

## Task 1: Batch expireOverdueWorkflows into single transaction

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt`
- Test: `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt` (existing, must still pass)

- [ ] **Step 1: Replace expireOverdueWorkflows with bulk SQL**

In `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt`, replace the `expireOverdueWorkflows` method:

```kotlin
    private suspend fun expireOverdueWorkflows() {
        val (timedOutCount, cancelledCount) = jdbi.inTransactionSuspend<Pair<Int, Int>, Exception> { handle ->
            val now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)

            val cancelled = handle.createUpdate(
                """
                UPDATE task SET status = 'CANCELLED', completed_at = :now
                WHERE status IN ('PENDING', 'WAITING_FOR_SIGNAL')
                  AND workflow_id IN (
                    SELECT id FROM workflow WHERE status = 'RUNNING' AND deadline_at < :now
                  )
                """,
            ).bind("now", now).execute()

            val timedOut = handle.createUpdate(
                """
                UPDATE workflow SET status = 'TIMED_OUT', updated_at = :now
                WHERE status = 'RUNNING' AND deadline_at < :now
                """,
            ).bind("now", now).execute()

            timedOut to cancelled
        }

        if (timedOutCount > 0) {
            log.warn("Timed out {} workflow(s), cancelled {} pending task(s)", timedOutCount, cancelledCount)
        }
    }
```

Add imports at the top of the file:

```kotlin
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
```

Changes:
- Replaced N separate transactions (one per workflow) with 2 bulk UPDATE statements in one transaction
- Tasks are cancelled first (while the workflow is still RUNNING, matching the original WHERE condition)
- Then workflows are marked TIMED_OUT
- Per-workflow logging replaced with aggregate log (acceptable for batch operation)
- Removed `workflowRepo.findTimedOut()` call — the bulk UPDATE handles finding + updating in one step

- [ ] **Step 2: Remove unused findTimedOut import if it was the only call site**

Check if `findTimedOut()` is called anywhere else. If this is the only call site, it can stay on the port interface (other callers may use it in the future). No change needed.

- [ ] **Step 3: Run watchdog tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="WorkflowWatchdogTest"`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt
git commit -m "perf(workflow): batch expireOverdueWorkflows into single transaction with bulk SQL"
```

---

## Task 2: Run expireOverdueTasks concurrently

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt`
- Test: `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt` (existing, must still pass)

- [ ] **Step 1: Replace serial loop with concurrent processing**

In `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt`, replace the `expireOverdueTasks` method:

```kotlin
    private suspend fun expireOverdueTasks() {
        val expired = taskRepo.findExpired(Instant.now())
        if (expired.isEmpty()) return

        supervisorScope {
            for (task in expired) {
                launch {
                    try {
                        log.warn("Expiring overdue task {} (deadline={})", task.id, task.deadlineAt)
                        phaseGate.onTaskCompleted(
                            taskId = task.id,
                            workflowId = task.workflowId,
                            sequenceNumber = task.sequenceNumber,
                            status = TaskStatus.TIMED_OUT,
                            resultJson = null,
                        )
                    } catch (e: Exception) {
                        log.error("Failed to expire task {}", task.id, e)
                    }
                }
            }
        }
    }
```

Add import:

```kotlin
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
```

Changes:
- `for (task in expired) { try { ... } }` (serial) → `supervisorScope { for (task in expired) { launch { try { ... } } } }` (concurrent)
- Each task's barrier call runs in its own coroutine, allowing concurrent processing
- `supervisorScope` ensures one failure doesn't cancel the others
- Each `launch` block has its own try/catch so failures are logged and don't propagate

- [ ] **Step 2: Run watchdog tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="WorkflowWatchdogTest"`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt
git commit -m "perf(workflow): run expireOverdueTasks concurrently via supervisorScope"
```
