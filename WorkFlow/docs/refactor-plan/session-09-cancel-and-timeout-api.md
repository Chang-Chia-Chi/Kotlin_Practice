# Session 9 — Workflow Cancel & DAG-Level Timeout

**Tier:** 4 (architecture improvements)
**Prerequisites:** Session 2 (DEAD_LETTER status), Session 5 (enqueued_at column)
**Estimated scope:** New status + cancel API + timeout column + sweeper extension + tests

---

## Items

### R4.1 — Add `CANCELLED` status and bulk cancel on workflow ABORT

**Problem:** When a workflow transitions to FAILED (via ABORT failure policy), in-flight PENDING tasks for the current sequence are not cancelled. Workers continue claiming and executing them. Their results are silently discarded when the CAS fails (workflow already advanced). This wastes compute and creates confusing logs.

**Schema change (new migration `V5__cancelled_and_timeout.sql`):**
```sql
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status
  CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'DEAD_LETTER', 'CANCELLED'));
```

**Files to modify:**

1. `src/main/kotlin/engine/WorkflowModels.kt`:
   ```kotlin
   enum class TaskStatus {
       PENDING, PROCESSING, COMPLETED, FAILED, DEAD_LETTER, CANCELLED;
       val isTerminal: Boolean get() = this in setOf(COMPLETED, FAILED, DEAD_LETTER, CANCELLED)
   }
   ```

2. `src/main/kotlin/engine/TaskRepository.kt` — add `cancelPendingTasks`:
   ```kotlin
   suspend fun cancelPendingTasks(workflowId: UUID): Int
   ```
   ```sql
   UPDATE task SET status = 'CANCELLED', completed_at = SYSTIMESTAMP
   WHERE workflow_id = :workflowId AND status = 'PENDING'
   ```

3. `src/main/kotlin/engine/BarrierService.kt` — in `advanceWorkflow`, when outcome is ABORT/FAILED:
   ```kotlin
   // After marking workflow FAILED:
   workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.FAILED)
   taskRepo.cancelPendingTasks(workflowId)  // bulk cancel orphans
   ```

4. `src/main/kotlin/engine/TaskRepository.kt` — `claimNext` WHERE clause already filters `status = 'PENDING'`, so CANCELLED tasks are naturally excluded.

**Test:**
1. Create a workflow with a PARALLEL fan-out of 5 tasks
2. Fail one task with `failurePolicy = ABORT`
3. Assert the other 4 PENDING tasks are now CANCELLED
4. Assert no worker claims them after cancellation

---

### R4.2 — Add DAG-level wall-clock timeout

**Problem:** No `timeout` exists on `WorkflowDefinition` or `WorkflowRun`. A workflow with 10 sequential 30-minute-deadline activities can run for 5 hours. A stuck workflow (CAS bug, infinite loop) runs indefinitely — the sweeper's `findStuck` only catches workflows where all current-sequence tasks are terminal.

**Schema change (same migration `V5__cancelled_and_timeout.sql`):**
```sql
ALTER TABLE workflow ADD timeout_at TIMESTAMP;
```

**Files to modify:**

1. `src/main/kotlin/dsl/WorkflowDsl.kt` — add optional `timeout` to `WorkflowDefinition`:
   ```kotlin
   data class WorkflowDefinition(
       val activities: List<ActivityDefinition>,
       val timeout: Duration = Duration.ofHours(1),  // default 1 hour
   )
   ```

2. `src/main/kotlin/dsl/WorkflowDslBuilders.kt` — add `timeout` to `WorkflowBuilder`:
   ```kotlin
   var timeout: Duration = Duration.ofHours(1)
   ```

3. `src/main/kotlin/engine/WorkflowModels.kt` — add `timeoutAt: LocalDateTime?` to `WorkflowRun`

4. `src/main/kotlin/engine/WorkflowEngine.kt` — set `timeout_at` on workflow insert:
   ```kotlin
   val timeoutAt = LocalDateTime.now(ZoneOffset.UTC).plus(definition.timeout)
   ```

5. `src/main/kotlin/engine/WorkflowRepository.kt`:
   - `insertWithHandle`: bind `timeout_at`
   - `findStuck`: add secondary condition:
     ```sql
     OR (w.status = 'RUNNING' AND w.timeout_at < SYSTIMESTAMP)
     ```

6. `src/main/kotlin/engine/Sweeper.kt` — `recoverStuckWorkflows` already calls `barrierService.recoverStuckWorkflow` for stuck workflows. Timed-out workflows found by the extended `findStuck` will be processed the same way. If recovery is not appropriate for timeouts, add a separate path that force-fails:
   ```kotlin
   if (workflow.timeoutAt != null && workflow.timeoutAt < now) {
       workflowRepo.updateStatusWithHandle(handle, workflow.id, WorkflowStatus.FAILED, epoch)
       taskRepo.cancelPendingTasks(workflow.id)
       log.warnf("Workflow %s timed out after %s", workflow.id, definition.timeout)
   }
   ```

**Test:**
1. Create a workflow with `timeout = Duration.ofSeconds(1)`
2. Register a handler that sleeps for 5 seconds
3. Wait for sweeper cycle
4. Assert workflow status is FAILED
5. Assert pending tasks are CANCELLED

---

### R4.3 — Add workflow cancel API

**Problem:** No programmatic way to cancel a running workflow. Operators must use raw SQL.

**Files to modify:**
- `src/main/kotlin/engine/WorkflowEngine.kt` — add `cancelWorkflow`:

```kotlin
suspend fun cancelWorkflow(workflowId: UUID): Boolean {
    return jdbi.inTransactionSuspend { handle ->
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: return@inTransactionSuspend false
        if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend false

        workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.FAILED)
        taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
        true
    }
}
```

Note: This does not cancel PROCESSING tasks (they are in-flight on workers). Those tasks will complete, attempt the barrier CAS, fail (workflow already FAILED), and be silently dropped. The reaper will eventually reclaim them if the handler hangs. This is acceptable for at-least-once semantics.

**Test:**
1. Start a multi-step workflow
2. Call `cancelWorkflow(id)` mid-execution
3. Assert workflow status is FAILED
4. Assert remaining PENDING tasks are CANCELLED
5. Assert in-flight PROCESSING tasks complete without error (CAS silently rejected)

---

## Verification

1. `mvn test` passes
2. Migration V5 applies cleanly
3. New tests for CANCELLED status, timeout, and cancel API
4. Existing workflow integration tests still pass
