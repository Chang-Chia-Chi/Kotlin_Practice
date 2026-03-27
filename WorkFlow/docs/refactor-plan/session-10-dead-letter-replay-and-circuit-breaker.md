# Session 10 — Dead-Letter Replay & Circuit Breaker

**Tier:** 4 (architecture improvements)
**Prerequisites:** Session 2 (DEAD_LETTER status), Session 5 (enqueued_at column)
**Estimated scope:** Replay API + per-handler circuit breaker + tests

---

## Items

### R4.4 — Add dead-letter replay API

**Problem:** Exhausted tasks end up in DEAD_LETTER (after Session 2) with no way to retry them except raw SQL. Operators need a programmatic way to replay failed tasks for debugging, bug fixes, or transient infrastructure issues.

**Files to modify:**

1. `src/main/kotlin/engine/TaskRepository.kt` — add `replayDeadLetterTask`:
   ```kotlin
   suspend fun replayDeadLetterTask(taskId: UUID): Boolean
   ```
   ```sql
   UPDATE task
   SET status = 'PENDING',
       retry_count = 0,
       claimed_by = NULL,
       claimed_at = NULL,
       claim_id = NULL,
       completed_at = NULL,
       result = NULL,
       enqueued_at = SYSTIMESTAMP
   WHERE id = :taskId AND status = 'DEAD_LETTER'
   ```
   Returns true if 1 row updated, false if task not found or not in DEAD_LETTER.

   Setting `enqueued_at = SYSTIMESTAMP` puts the replayed task at the back of the FIFO queue. Resetting `retry_count = 0` gives it a fresh set of retries.

2. `src/main/kotlin/engine/TaskRepository.kt` — add `replayDeadLetterBatch`:
   ```kotlin
   suspend fun replayDeadLetterBatch(workflowId: UUID): Int
   ```
   ```sql
   UPDATE task
   SET status = 'PENDING', retry_count = 0, claimed_by = NULL,
       claimed_at = NULL, claim_id = NULL, completed_at = NULL,
       result = NULL, enqueued_at = SYSTIMESTAMP
   WHERE workflow_id = :workflowId AND status = 'DEAD_LETTER'
   ```
   Returns count of replayed tasks.

3. `src/main/kotlin/engine/WorkflowEngine.kt` — add replay methods:
   ```kotlin
   suspend fun replayTask(taskId: UUID): Boolean =
       taskRepo.replayDeadLetterTask(taskId)

   suspend fun replayWorkflowDeadLetters(workflowId: UUID): Int =
       taskRepo.replayDeadLetterBatch(workflowId)
   ```

**Important consideration:** Replaying a dead-lettered task does not automatically re-advance the workflow. The task re-enters the queue, gets claimed, executes, and calls `barrierService.onTaskCompleted`. If the workflow has already advanced past this sequence (or is FAILED), the CAS will silently reject. The operator must also reset the workflow status if needed:
```kotlin
suspend fun replayWorkflow(workflowId: UUID): Boolean {
    return jdbi.inTransactionSuspend { handle ->
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: return@inTransactionSuspend false
        if (workflow.status != WorkflowStatus.FAILED) return@inTransactionSuspend false

        workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.RUNNING, 0)
        taskRepo.replayDeadLetterBatchWithHandle(handle, workflowId)
        true
    }
}
```

**Test:**
1. Create a task, exhaust retries, verify status is DEAD_LETTER
2. Call `replayDeadLetterTask(taskId)`
3. Assert status is PENDING, retry_count is 0, claim fields are null
4. Assert task is claimable by workers
5. Test idempotency: replay an already-replayed task (now PENDING) — assert returns false

---

### R4.7 — Per-handler-key circuit breaker

**Problem:** A poison-pill task type (handler consistently throws) saturates the shared concurrency pool. With `concurrency = 4`, 4 poison-pill tasks occupy all slots, starving healthy task types. There is no per-handler isolation or backoff.

**Design:** Per-handler-key failure tracking with exponential backoff. When a handler key exceeds a failure threshold, new claims for that key are delayed (not rejected — the tasks stay in PENDING).

**Files to modify:**

1. New file: `src/main/kotlin/worker/HandlerCircuitBreaker.kt`:
   ```kotlin
   @Singleton
   class HandlerCircuitBreaker {
       private data class HandlerState(
           val consecutiveFailures: AtomicInteger = AtomicInteger(0),
           val backoffUntil: AtomicReference<Instant> = AtomicReference(Instant.EPOCH),
       )

       private val states = ConcurrentHashMap<String, HandlerState>()

       fun recordSuccess(handlerKey: String) {
           states[handlerKey]?.consecutiveFailures?.set(0)
           states[handlerKey]?.backoffUntil?.set(Instant.EPOCH)
       }

       fun recordFailure(handlerKey: String) {
           val state = states.computeIfAbsent(handlerKey) { HandlerState() }
           val failures = state.consecutiveFailures.incrementAndGet()
           if (failures >= FAILURE_THRESHOLD) {
               val backoff = Duration.ofSeconds(
                   minOf(2.0.pow(failures - FAILURE_THRESHOLD).toLong(), MAX_BACKOFF_SECONDS)
               )
               state.backoffUntil.set(Instant.now().plus(backoff))
           }
       }

       fun isOpen(handlerKey: String): Boolean {
           val state = states[handlerKey] ?: return false
           return Instant.now().isBefore(state.backoffUntil.get())
       }

       companion object {
           const val FAILURE_THRESHOLD = 3
           const val MAX_BACKOFF_SECONDS = 300L  // 5 minutes max
       }
   }
   ```

2. `src/main/kotlin/worker/WorkerLoop.kt` — inject `HandlerCircuitBreaker`:
   ```kotlin
   // In processTask, before executing:
   if (circuitBreaker.isOpen(task.handlerKey)) {
       log.debugf("Circuit breaker open for handler %s — skipping task %s", task.handlerKey, task.id)
       // Release the task back to the queue for later
       taskRepo.resetForRetry(task.id)
       return
   }

   // After successful execution:
   circuitBreaker.recordSuccess(task.handlerKey)

   // In handleTaskFailure:
   circuitBreaker.recordFailure(task.handlerKey)
   ```

3. `src/main/kotlin/worker/WorkerLoop.kt` — add circuit breaker metrics:
   ```kotlin
   meterRegistry.gauge("taskqueue_circuit_breaker_open", Tags.of("handler", handlerKey), ...) { ... }
   ```

**Behavior:**
- After 3 consecutive failures for a handler key: exponential backoff starts (1s, 2s, 4s, 8s, ..., max 5m)
- During backoff: claimed tasks for that key are immediately released back to PENDING (no execution)
- First success resets the counter and backoff
- Per-pod state — each pod tracks its own circuit breaker independently

**Alternative (simpler): `not_before` column**

Instead of application-level circuit breaker, add a `not_before TIMESTAMP` column to the task table. On retry, set `not_before = SYSTIMESTAMP + backoff_duration`. The claim query adds `AND (not_before IS NULL OR not_before < SYSTIMESTAMP)`. This pushes the backoff into the DB and applies across all pods.

```sql
ALTER TABLE task ADD not_before TIMESTAMP;
-- In claimNext inner query:
WHERE status = 'PENDING' AND (not_before IS NULL OR not_before < SYSTIMESTAMP) AND ...
```

This approach is simpler and applies globally. Recommended for the first iteration.

**Test:**
1. Register a handler that always throws
2. Execute 5 tasks for that handler
3. Assert that after 3 failures, subsequent claims are either skipped (app-level) or delayed (DB-level)
4. Register a handler that succeeds, execute one task — assert circuit resets

---

## Verification

1. `mvn test` passes
2. New tests for dead-letter replay (single + batch + idempotency)
3. New tests for circuit breaker (threshold, backoff, reset)
4. Integration test: poison-pill handler does not starve healthy handlers
