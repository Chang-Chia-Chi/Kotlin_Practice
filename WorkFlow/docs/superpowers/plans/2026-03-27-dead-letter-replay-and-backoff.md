# Dead-Letter Replay & Exponential Backoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dead-letter replay API and DB-level exponential backoff via `not_before` column to prevent poison-pill handler saturation.

**Architecture:** New `not_before TIMESTAMP` column on the `task` table filters backed-off tasks out of the claim query. `resetForRetry` computes exponential backoff (`2^retryCount`, capped at 300s). Three replay methods (single, batch, workflow-level) reset dead-lettered tasks to PENDING. A status guard in `BarrierService.onTaskCompleted` prevents FAILED workflows from auto-advancing.

**Tech Stack:** Kotlin, JDBI 3 (raw SQL), Oracle DB, JUnit 5 + kotlinx-coroutines-test

**Spec:** `docs/superpowers/specs/2026-03-27-dead-letter-replay-and-backoff-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/main/resources/db/migration/V3__add_not_before.sql` | Create | Add `not_before` column + index |
| `src/main/kotlin/engine/WorkflowModels.kt` | Modify | Add `notBefore: Instant?` to `Task` data class |
| `src/main/kotlin/engine/TaskRepository.kt` | Modify | `not_before` in claim/retry/insert/map + replay methods |
| `src/main/kotlin/engine/BarrierService.kt` | Modify | Workflow status guard in `onTaskCompleted` |
| `src/main/kotlin/engine/WorkflowEngine.kt` | Modify | Add `replayWorkflow` |
| `src/test/kotlin/engine/RepositoryTest.kt` | Modify | Tests for replay + `not_before` filtering |
| `src/test/kotlin/engine/SweeperTest.kt` | Modify | Tests for sweeper backoff |

---

### Task 1: Schema migration + Task model + row mapping

Add the `not_before` column to the DB and wire it through the data model and row mapper.

**Files:**
- Create: `src/main/resources/db/migration/V3__add_not_before.sql`
- Modify: `src/main/kotlin/engine/WorkflowModels.kt:25-39`
- Modify: `src/main/kotlin/engine/TaskRepository.kt:283-319` (insertBatchWithHandle)
- Modify: `src/main/kotlin/engine/TaskRepository.kt:347-364` (mapTaskRow)
- Test: `src/test/kotlin/engine/RepositoryTest.kt`

- [ ] **Step 1: Create migration file**

Create `src/main/resources/db/migration/V3__add_not_before.sql`:

```sql
-- R4.7: exponential backoff via not_before column
ALTER TABLE task ADD not_before TIMESTAMP;
CREATE INDEX idx_task_not_before ON task (status, not_before);
```

- [ ] **Step 2: Add `notBefore` field to Task data class**

In `src/main/kotlin/engine/WorkflowModels.kt`, add `notBefore` as the last field of the `Task` data class:

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
    val notBefore: Instant? = null,
)
```

- [ ] **Step 3: Update `mapTaskRow` to read `not_before`**

In `src/main/kotlin/engine/TaskRepository.kt`, update `mapTaskRow` (line ~347) to include:

```kotlin
private fun mapTaskRow(row: Map<String, Any?>): Task {
    val ci = caseInsensitive(row)
    return Task(
        id = ci["ID"] as String,
        workflowId = ci["WORKFLOW_ID"] as String,
        sequenceNumber = (ci["SEQUENCE_NUMBER"] as Number).toInt(),
        status = TaskStatus.valueOf(ci["STATUS"] as String),
        handlerKey = ci["HANDLER_KEY"] as String,
        payloadJson = ci["PAYLOAD"]?.let { readClob(it) },
        resultJson = ci["RESULT"]?.let { readClob(it) },
        claimedBy = ci["CLAIMED_BY"] as String?,
        claimedAt = readNullableTimestamp(ci["CLAIMED_AT"]),
        completedAt = readNullableTimestamp(ci["COMPLETED_AT"]),
        retryCount = (ci["RETRY_COUNT"] as Number).toInt(),
        maxRetries = (ci["MAX_RETRIES"] as Number).toInt(),
        deadlineAt = readNullableTimestamp(ci["DEADLINE_AT"]),
        notBefore = readNullableTimestamp(ci["NOT_BEFORE"]),
    )
}
```

- [ ] **Step 4: Update `insertBatchWithHandle` to include `not_before`**

In `src/main/kotlin/engine/TaskRepository.kt`, update `insertBatchWithHandle` (line ~283):

Change the INSERT SQL to include `not_before` in both the column list and VALUES:

```kotlin
fun insertBatchWithHandle(
    handle: Handle,
    tasks: List<Task>,
) {
    if (tasks.isEmpty()) return
    val batch =
        handle.prepareBatch(
            """
        INSERT INTO task (id, workflow_id, sequence_number, status, handler_key,
                          payload, result, claimed_by, claimed_at, completed_at,
                          retry_count, max_retries, deadline_at, not_before)
        VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey,
                :payload, :result, :claimedBy, :claimedAt, :completedAt,
                :retryCount, :maxRetries, :deadlineAt, :notBefore)
        """,
        )
    for (task in tasks) {
        batch
            .bind("id", task.id)
            .bind("workflowId", task.workflowId)
            .bind("sequenceNumber", task.sequenceNumber)
            .bind("status", task.status.name)
            .bind("handlerKey", task.handlerKey)
        bindNullableClob(batch, "payload", task.payloadJson)
        bindNullableClob(batch, "result", task.resultJson)
        batch
            .bind("claimedBy", task.claimedBy)
        bindNullableTimestamp(batch, "claimedAt", task.claimedAt)
        bindNullableTimestamp(batch, "completedAt", task.completedAt)
        batch
            .bind("retryCount", task.retryCount)
            .bind("maxRetries", task.maxRetries)
        bindNullableTimestamp(batch, "deadlineAt", task.deadlineAt)
        bindNullableTimestamp(batch, "notBefore", task.notBefore)
        batch.add()
    }
    batch.execute()
}
```

- [ ] **Step 5: Update test helper `insertTaskDirect` to include `not_before`**

In `src/test/kotlin/engine/RepositoryTest.kt`, update the `makeTask` helper (line ~69) to accept `notBefore`:

```kotlin
private fun makeTask(
    id: String = randomId(),
    workflowId: String,
    sequenceNumber: Int = 1,
    status: TaskStatus = TaskStatus.PENDING,
    handlerKey: String = "test.handler",
    payloadJson: String? = null,
    resultJson: String? = null,
    claimedBy: String? = null,
    claimedAt: Instant? = null,
    completedAt: Instant? = null,
    retryCount: Int = 0,
    maxRetries: Int = 0,
    deadlineAt: Instant? = null,
    notBefore: Instant? = null,
) = Task(
    id = id,
    workflowId = workflowId,
    sequenceNumber = sequenceNumber,
    status = status,
    handlerKey = handlerKey,
    payloadJson = payloadJson,
    resultJson = resultJson,
    claimedBy = claimedBy,
    claimedAt = claimedAt,
    completedAt = completedAt,
    retryCount = retryCount,
    maxRetries = maxRetries,
    deadlineAt = deadlineAt,
    notBefore = notBefore,
)
```

Update `insertTaskDirect` (line ~118) to bind `not_before`:

```kotlin
private fun insertTaskDirect(task: Task) {
    jdbi.useHandle<Exception> { handle ->
        val stmt = handle.createUpdate(
            """INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, payload, result,
               claimed_by, claimed_at, completed_at, retry_count, max_retries, deadline_at, not_before)
               VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey, :payload, :result,
               :claimedBy, :claimedAt, :completedAt, :retryCount, :maxRetries, :deadlineAt, :notBefore)"""
        )
            .bind("id", task.id)
            .bind("workflowId", task.workflowId)
            .bind("sequenceNumber", task.sequenceNumber)
            .bind("status", task.status.name)
            .bind("handlerKey", task.handlerKey)
            .bind("retryCount", task.retryCount)
            .bind("maxRetries", task.maxRetries)

        fun bindStringOrNull(name: String, value: String?) =
            if (value != null) stmt.bind(name, value) else stmt.bindNull(name, java.sql.Types.VARCHAR)
        fun bindTimestampOrNull(name: String, value: Instant?) =
            if (value != null) stmt.bind(name, LocalDateTime.ofInstant(value, ZoneOffset.UTC))
            else stmt.bindNull(name, java.sql.Types.TIMESTAMP)

        bindStringOrNull("payload", task.payloadJson)
        bindStringOrNull("result", task.resultJson)
        bindStringOrNull("claimedBy", task.claimedBy)
        bindTimestampOrNull("claimedAt", task.claimedAt)
        bindTimestampOrNull("completedAt", task.completedAt)
        bindTimestampOrNull("deadlineAt", task.deadlineAt)
        bindTimestampOrNull("notBefore", task.notBefore)

        stmt.execute()
    }
}
```

- [ ] **Step 6: Run existing tests to verify migration + model changes are compatible**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl .`

Expected: All existing tests PASS (no regressions from adding the nullable column).

- [ ] **Step 7: Commit**

```bash
git add src/main/resources/db/migration/V3__add_not_before.sql \
        src/main/kotlin/engine/WorkflowModels.kt \
        src/main/kotlin/engine/TaskRepository.kt \
        src/test/kotlin/engine/RepositoryTest.kt
git commit -m "feat: add not_before column for exponential backoff (schema + model)"
```

---

### Task 2: `not_before` filter in claim query

Make `claimNext` skip tasks that are still in backoff.

**Files:**
- Modify: `src/main/kotlin/engine/TaskRepository.kt:23-70` (claimNext)
- Test: `src/test/kotlin/engine/RepositoryTest.kt`

- [ ] **Step 1: Write the failing test — claim skips tasks with future `not_before`**

Add to the `TaskRepositoryTests` nested class in `RepositoryTest.kt`:

```kotlin
// ── not_before backoff ────────────────────────────────────────────

@Test
fun `claimNext skips tasks with future not_before`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    val futureNotBefore = now().plus(Duration.ofMinutes(5))
    insertTaskDirect(makeTask(
        workflowId = wf.id,
        status = TaskStatus.PENDING,
        handlerKey = "backed-off",
        notBefore = futureNotBefore,
    ))
    insertTaskDirect(makeTask(
        workflowId = wf.id,
        status = TaskStatus.PENDING,
        handlerKey = "ready",
    ))

    val claimed = taskRepo.claimNext("worker-1", 10)

    assertEquals(1, claimed.size)
    assertEquals("ready", claimed[0].handlerKey)
}

@Test
fun `claimNext claims task after not_before has passed`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    val pastNotBefore = now().minus(Duration.ofSeconds(1))
    insertTaskDirect(makeTask(
        workflowId = wf.id,
        status = TaskStatus.PENDING,
        handlerKey = "backoff-expired",
        notBefore = pastNotBefore,
    ))

    val claimed = taskRepo.claimNext("worker-1", 10)

    assertEquals(1, claimed.size)
    assertEquals("backoff-expired", claimed[0].handlerKey)
}

@Test
fun `claimNext claims task with null not_before`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    insertTaskDirect(makeTask(
        workflowId = wf.id,
        status = TaskStatus.PENDING,
        handlerKey = "no-backoff",
        notBefore = null,
    ))

    val claimed = taskRepo.claimNext("worker-1", 10)

    assertEquals(1, claimed.size)
    assertEquals("no-backoff", claimed[0].handlerKey)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest#TaskRepositoryTests#claimNext skips tasks with future not_before" -pl .`

Expected: FAIL — the backed-off task is still claimed because `claimNext` doesn't filter `not_before` yet.

- [ ] **Step 3: Add `not_before` filter to `claimNext`**

In `src/main/kotlin/engine/TaskRepository.kt`, update the inner SELECT in `claimNext` (line ~32):

```kotlin
h.createQuery(
    """
    SELECT * FROM task
    WHERE id IN (
        SELECT id FROM task
        WHERE status = 'PENDING'
          AND (deadline_at IS NULL OR deadline_at > :now)
          AND (not_before IS NULL OR not_before < :now)
        ORDER BY claimed_at NULLS FIRST, id
        FETCH FIRST :limit ROWS ONLY
    )
    FOR UPDATE SKIP LOCKED
    """,
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl .`

Expected: All tests PASS including the three new `not_before` tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/TaskRepository.kt \
        src/test/kotlin/engine/RepositoryTest.kt
git commit -m "feat: filter not_before in claim query for exponential backoff"
```

---

### Task 3: Exponential backoff in `resetForRetry`

Set `not_before` with exponential backoff when a task is reset for retry.

**Files:**
- Modify: `src/main/kotlin/engine/TaskRepository.kt:120-136` (resetForRetry)
- Test: `src/test/kotlin/engine/RepositoryTest.kt`

- [ ] **Step 1: Write the failing test — `resetForRetry` sets `not_before`**

Add to `TaskRepositoryTests` in `RepositoryTest.kt`:

```kotlin
// ── resetForRetry with backoff ───────────────────────────────────

@Test
fun `resetForRetry sets not_before with exponential backoff`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)
    val task = makeTask(
        workflowId = wf.id,
        status = TaskStatus.PROCESSING,
        claimedBy = "worker-1",
        claimedAt = now(),
        maxRetries = 5,
        retryCount = 0,
    )
    insertTaskDirect(task)

    val beforeReset = Instant.now()
    taskRepo.resetForRetry(task.id, 2) // retryCount=2 → backoff = 2^2 = 4s

    val row = readTaskDirect(task.id)!!
    assertEquals("PENDING", row["STATUS"])
    assertNull(row["CLAIMED_BY"])
    assertNull(row["CLAIMED_AT"])
    assertEquals(2, (row["RETRY_COUNT"] as Number).toInt())

    val notBefore = readNullableTimestampDirect(row["NOT_BEFORE"])
    assertNotNull(notBefore, "not_before should be set after resetForRetry")
    // 2^2 = 4 seconds backoff, allow 2s tolerance for execution time
    val expectedMin = beforeReset.plusSeconds(2)
    val expectedMax = beforeReset.plusSeconds(6)
    assertTrue(
        notBefore.isAfter(expectedMin) && notBefore.isBefore(expectedMax),
        "not_before ($notBefore) should be ~4s after reset, " +
            "expected between $expectedMin and $expectedMax",
    )
}

@Test
fun `resetForRetry backoff caps at 300 seconds`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)
    val task = makeTask(
        workflowId = wf.id,
        status = TaskStatus.PROCESSING,
        claimedBy = "worker-1",
        claimedAt = now(),
        maxRetries = 15,
        retryCount = 0,
    )
    insertTaskDirect(task)

    val beforeReset = Instant.now()
    taskRepo.resetForRetry(task.id, 10) // 2^10 = 1024, capped to 300

    val row = readTaskDirect(task.id)!!
    val notBefore = readNullableTimestampDirect(row["NOT_BEFORE"])
    assertNotNull(notBefore)
    // Should be ~300s, not 1024s
    val maxExpected = beforeReset.plusSeconds(310)
    assertTrue(
        notBefore.isBefore(maxExpected),
        "not_before ($notBefore) should be capped at ~300s, not exceed $maxExpected",
    )
    val minExpected = beforeReset.plusSeconds(290)
    assertTrue(
        notBefore.isAfter(minExpected),
        "not_before ($notBefore) should be at least ~300s ($minExpected)",
    )
}
```

Also add this utility method to the test class (alongside `readTaskDirect`):

```kotlin
/** Read a nullable Oracle timestamp from raw row data for assertions. */
private fun readNullableTimestampDirect(value: Any?): Instant? = when (value) {
    null -> null
    is java.sql.Timestamp -> value.toInstant()
    else -> {
        // Oracle JDBC returns oracle.sql.TIMESTAMP — use reflection
        val method = value::class.java.getMethod("timestampValue")
        (method.invoke(value) as java.sql.Timestamp).toInstant()
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest#TaskRepositoryTests#resetForRetry sets not_before with exponential backoff" -pl .`

Expected: FAIL — `not_before` is NULL because `resetForRetry` doesn't set it yet.

- [ ] **Step 3: Update `resetForRetry` to set `not_before` with backoff**

In `src/main/kotlin/engine/TaskRepository.kt`, replace `resetForRetry` (lines 120-136):

```kotlin
suspend fun resetForRetry(
    id: String,
    newRetryCount: Int,
) {
    jdbi.inTransactionSuspend<Unit, Exception> { h: Handle ->
        h
            .createUpdate(
                """
            UPDATE task
            SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
                retry_count = :newRetryCount,
                not_before = SYSTIMESTAMP + NUMTODSINTERVAL(LEAST(POWER(2, :newRetryCount), 300), 'SECOND')
            WHERE id = :id
            """,
            ).bind("id", id)
            .bind("newRetryCount", newRetryCount)
            .execute()
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl .`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/TaskRepository.kt \
        src/test/kotlin/engine/RepositoryTest.kt
git commit -m "feat: exponential backoff in resetForRetry via not_before"
```

---

### Task 4: Sweeper backoff in `resetStaleTasks`

Stale tasks reclaimed by the sweeper should also get `not_before` backoff.

**Files:**
- Modify: `src/main/kotlin/engine/TaskRepository.kt:149-160` (resetStaleTasks)
- Test: `src/test/kotlin/engine/SweeperTest.kt`

- [ ] **Step 1: Write the failing test — stale reclaim sets `not_before`**

Add to `SweeperTest.kt`. First, add a `readNullableTimestampDirect` helper (same as RepositoryTest):

```kotlin
private fun readNullableTimestampDirect(value: Any?): Instant? = when (value) {
    null -> null
    is java.sql.Timestamp -> value.toInstant()
    else -> {
        val method = value::class.java.getMethod("timestampValue")
        (method.invoke(value) as java.sql.Timestamp).toInstant()
    }
}
```

Then add the test (in the appropriate nested class or at top level):

```kotlin
@Test
fun `reclaimStaleTasks sets not_before with backoff`() = runTest {
    val wf = makeWorkflow()
    insertWorkflowDirect(wf)

    val staleTime = Instant.now().minus(Duration.ofMinutes(15))
    val task = makeTask(
        workflowId = wf.id,
        status = TaskStatus.PROCESSING,
        claimedBy = "dead-worker",
        claimedAt = staleTime,
        retryCount = 2,
        maxRetries = 5,
    )
    insertTaskDirect(task)

    val threshold = Instant.now().minus(Duration.ofMinutes(10))
    val beforeReclaim = Instant.now()
    val reclaimed = taskRepo.resetStaleTasks(threshold)

    assertEquals(1, reclaimed)
    val row = readTaskDirect(task.id)!!
    assertEquals("PENDING", row["STATUS"])
    // retry_count incremented from 2 to 3, so backoff = 2^3 = 8s
    val notBefore = readNullableTimestampDirect(row["NOT_BEFORE"])
    assertNotNull(notBefore, "not_before should be set after stale reclaim")
    assertTrue(notBefore.isAfter(beforeReclaim.plusSeconds(4)))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest#reclaimStaleTasks sets not_before with backoff" -pl .`

Expected: FAIL — `not_before` is NULL.

- [ ] **Step 3: Update `resetStaleTasks` to set `not_before`**

In `src/main/kotlin/engine/TaskRepository.kt`, replace `resetStaleTasks` (lines 149-160):

```kotlin
suspend fun resetStaleTasks(staleThreshold: Instant): Int =
    jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
        h
            .createUpdate(
                """
            UPDATE task
            SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL,
                retry_count = retry_count + 1,
                not_before = SYSTIMESTAMP + NUMTODSINTERVAL(LEAST(POWER(2, retry_count + 1), 300), 'SECOND')
            WHERE status = 'PROCESSING' AND claimed_at < :threshold AND retry_count < max_retries
            """,
            ).bind("threshold", LocalDateTime.ofInstant(staleThreshold, ZoneOffset.UTC))
            .execute()
    }
```

Note: `retry_count + 1` in the `POWER` function because the increment happens in the same UPDATE.

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest" -pl .`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/TaskRepository.kt \
        src/test/kotlin/engine/SweeperTest.kt
git commit -m "feat: sweeper stale reclaim sets not_before backoff"
```

---

### Task 5: Barrier workflow status guard

Prevent `onTaskCompleted` from advancing FAILED workflows.

**Files:**
- Modify: `src/main/kotlin/engine/BarrierService.kt:28-72` (onTaskCompleted)
- Test: `src/test/kotlin/engine/SweeperTest.kt`

- [ ] **Step 1: Write the failing test — barrier skips advance for FAILED workflow**

Add to `SweeperTest.kt` (which already has BarrierService wired up as a real instance):

```kotlin
@Test
fun `onTaskCompleted does not advance FAILED workflow`() = runTest {
    // Setup: single-step workflow definition
    val definition = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(
                transition = "test.handler",
                retries = 0,
                deadline = Duration.ofHours(1),
                failurePolicy = FailurePolicy.ABORT,
            ),
        ),
    )
    val wf = makeWorkflow(
        definitionJson = objectMapper.writeValueAsString(definition),
        currentSequence = 1,
        version = 0,
        status = WorkflowStatus.FAILED,
    )
    insertWorkflowDirect(wf)
    val task = makeTask(
        workflowId = wf.id,
        sequenceNumber = 1,
        status = TaskStatus.PROCESSING,
        claimedBy = "worker-1",
        claimedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    )
    insertTaskDirect(task)

    barrier.onTaskCompleted(
        taskId = task.id,
        workflowId = wf.id,
        sequenceNumber = 1,
        status = TaskStatus.COMPLETED,
        resultJson = """{"ok":true}""",
        claimedBy = task.claimedBy,
        claimedAt = task.claimedAt,
    )

    // Task should be updated to COMPLETED
    val taskRow = readTaskDirect(task.id)!!
    assertEquals("COMPLETED", taskRow["STATUS"])

    // BUT workflow should still be FAILED — not advanced
    val wfRow = readWorkflowDirect(wf.id)!!
    assertEquals("FAILED", wfRow["STATUS"])
    assertEquals(1, (wfRow["CURRENT_SEQUENCE"] as Number).toInt())
    assertEquals(0, (wfRow["VERSION"] as Number).toInt())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest#onTaskCompleted does not advance FAILED workflow" -pl .`

Expected: FAIL — barrier currently advances the FAILED workflow (CAS succeeds because it only checks `current_sequence` and `version`).

- [ ] **Step 3: Add status guard to `onTaskCompleted`**

In `src/main/kotlin/engine/BarrierService.kt`, add the guard after loading the workflow (after line 48):

```kotlin
// 3. Load workflow and compute sequence metadata
val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
    ?: throw IllegalStateException("Workflow not found: $workflowId")
if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend
```

The full block at lines 46-48 becomes:

```kotlin
// 3. Load workflow and compute sequence metadata
val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
    ?: throw IllegalStateException("Workflow not found: $workflowId")
if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend
val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest" -pl .`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/BarrierService.kt \
        src/test/kotlin/engine/SweeperTest.kt
git commit -m "fix: barrier skips advance for non-RUNNING workflows"
```

---

### Task 6: Dead-letter replay — single task

Add `replayDeadLetterTask` to `TaskRepository`.

**Files:**
- Modify: `src/main/kotlin/engine/TaskRepository.kt`
- Test: `src/test/kotlin/engine/RepositoryTest.kt`

- [ ] **Step 1: Write failing tests for single replay**

Add to `TaskRepositoryTests` in `RepositoryTest.kt`:

```kotlin
// ── Dead-letter replay ────────────────────────────────────────────

@Test
fun `replayDeadLetterTask resets DEAD_LETTER to PENDING`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    val task = makeTask(
        workflowId = wf.id,
        status = TaskStatus.DEAD_LETTER,
        claimedBy = "old-worker",
        claimedAt = now().minus(Duration.ofHours(1)),
        completedAt = now().minus(Duration.ofMinutes(30)),
        resultJson = """{"error":"timeout"}""",
        retryCount = 3,
        maxRetries = 3,
        notBefore = now().plus(Duration.ofMinutes(5)),
    )
    insertTaskDirect(task)

    val result = taskRepo.replayDeadLetterTask(task.id)

    assertTrue(result)
    val row = readTaskDirect(task.id)!!
    assertEquals("PENDING", row["STATUS"])
    assertEquals(0, (row["RETRY_COUNT"] as Number).toInt())
    assertNull(row["CLAIMED_BY"])
    assertNull(row["CLAIMED_AT"])
    assertNull(row["COMPLETED_AT"])
    assertNull(row["RESULT"])
    assertNull(row["NOT_BEFORE"])
}

@Test
fun `replayDeadLetterTask returns false for non-DEAD_LETTER task`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    val task = makeTask(workflowId = wf.id, status = TaskStatus.PENDING)
    insertTaskDirect(task)

    val result = taskRepo.replayDeadLetterTask(task.id)

    assertFalse(result)
}

@Test
fun `replayDeadLetterTask returns false for non-existent task`() = runTest {
    val result = taskRepo.replayDeadLetterTask(randomId())
    assertFalse(result)
}

@Test
fun `replayed task is claimable by workers`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    val task = makeTask(
        workflowId = wf.id,
        status = TaskStatus.DEAD_LETTER,
        retryCount = 3,
        maxRetries = 3,
    )
    insertTaskDirect(task)

    taskRepo.replayDeadLetterTask(task.id)
    val claimed = taskRepo.claimNext("worker-1", 10)

    assertEquals(1, claimed.size)
    assertEquals(task.id, claimed[0].id)
    assertEquals(TaskStatus.PROCESSING, claimed[0].status)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest#TaskRepositoryTests#replayDeadLetterTask resets DEAD_LETTER to PENDING" -pl .`

Expected: FAIL — `replayDeadLetterTask` method does not exist yet.

- [ ] **Step 3: Implement `replayDeadLetterTask`**

Add to `src/main/kotlin/engine/TaskRepository.kt` in the "Suspend methods" section (after `resetForRetry`, around line 136):

```kotlin
suspend fun replayDeadLetterTask(taskId: String): Boolean =
    jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
        val count = h
            .createUpdate(
                """
            UPDATE task
            SET status = 'PENDING', retry_count = 0,
                claimed_by = NULL, claimed_at = NULL,
                completed_at = NULL, result = NULL, not_before = NULL
            WHERE id = :taskId AND status = 'DEAD_LETTER'
            """,
            ).bind("taskId", taskId)
            .execute()
        count > 0
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl .`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/TaskRepository.kt \
        src/test/kotlin/engine/RepositoryTest.kt
git commit -m "feat: add replayDeadLetterTask for single dead-letter replay"
```

---

### Task 7: Dead-letter replay — batch + WithHandle

Add `replayDeadLetterBatch` and `replayDeadLetterBatchWithHandle` to `TaskRepository`.

**Files:**
- Modify: `src/main/kotlin/engine/TaskRepository.kt`
- Test: `src/test/kotlin/engine/RepositoryTest.kt`

- [ ] **Step 1: Write failing tests for batch replay**

Add to `TaskRepositoryTests` in `RepositoryTest.kt`:

```kotlin
@Test
fun `replayDeadLetterBatch replays all DEAD_LETTER tasks for workflow`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    val dl1 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
    val dl2 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
    val completed = makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED)
    insertTaskDirect(dl1)
    insertTaskDirect(dl2)
    insertTaskDirect(completed)

    val count = taskRepo.replayDeadLetterBatch(wf.id)

    assertEquals(2, count)
    assertEquals("PENDING", readTaskDirect(dl1.id)!!["STATUS"])
    assertEquals("PENDING", readTaskDirect(dl2.id)!!["STATUS"])
    assertEquals("COMPLETED", readTaskDirect(completed.id)!!["STATUS"])
}

@Test
fun `replayDeadLetterBatch returns 0 when no DEAD_LETTER tasks`() = runTest {
    val wf = makeWorkflow()
    workflowRepo.insert(wf)

    insertTaskDirect(makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED))

    val count = taskRepo.replayDeadLetterBatch(wf.id)
    assertEquals(0, count)
}

@Test
fun `replayDeadLetterBatch scoped to workflow`() = runTest {
    val wf1 = makeWorkflow()
    val wf2 = makeWorkflow()
    workflowRepo.insert(wf1)
    workflowRepo.insert(wf2)

    val task1 = makeTask(workflowId = wf1.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
    val task2 = makeTask(workflowId = wf2.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
    insertTaskDirect(task1)
    insertTaskDirect(task2)

    val count = taskRepo.replayDeadLetterBatch(wf1.id)

    assertEquals(1, count)
    assertEquals("PENDING", readTaskDirect(task1.id)!!["STATUS"])
    assertEquals("DEAD_LETTER", readTaskDirect(task2.id)!!["STATUS"])
}

@Test
fun `replayDeadLetterBatchWithHandle works within transaction`() {
    val wf = makeWorkflow()
    insertWorkflowDirect(wf)
    val task = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
    insertTaskDirect(task)

    val count = jdbi.inTransaction<Int, Exception> { handle ->
        taskRepo.replayDeadLetterBatchWithHandle(handle, wf.id)
    }

    assertEquals(1, count)
    assertEquals("PENDING", readTaskDirect(task.id)!!["STATUS"])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest#TaskRepositoryTests#replayDeadLetterBatch replays all DEAD_LETTER tasks for workflow" -pl .`

Expected: FAIL — methods do not exist yet.

- [ ] **Step 3: Implement batch replay methods**

Add to `src/main/kotlin/engine/TaskRepository.kt`:

In the "Suspend methods" section (after `replayDeadLetterTask`):

```kotlin
suspend fun replayDeadLetterBatch(workflowId: String): Int =
    jdbi.inTransactionSuspend<Int, Exception> { h: Handle ->
        replayDeadLetterBatchWithHandle(h, workflowId)
    }
```

In the "Handle methods" section (after `insertBatchWithHandle`):

```kotlin
fun replayDeadLetterBatchWithHandle(handle: Handle, workflowId: String): Int =
    handle
        .createUpdate(
            """
        UPDATE task
        SET status = 'PENDING', retry_count = 0,
            claimed_by = NULL, claimed_at = NULL,
            completed_at = NULL, result = NULL, not_before = NULL
        WHERE workflow_id = :workflowId AND status = 'DEAD_LETTER'
        """,
        ).bind("workflowId", workflowId)
        .execute()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl .`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/TaskRepository.kt \
        src/test/kotlin/engine/RepositoryTest.kt
git commit -m "feat: add batch dead-letter replay (replayDeadLetterBatch + WithHandle)"
```

---

### Task 8: `replayWorkflow` in WorkflowEngine

Transactional workflow-level replay: reset FAILED workflow to RUNNING and replay all dead letters atomically.

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowEngine.kt`
- Test: `src/test/kotlin/engine/SweeperTest.kt` (has barrier + repos wired up)

- [ ] **Step 1: Write failing tests for `replayWorkflow`**

Add to `SweeperTest.kt` (which has `jdbi`, `workflowRepo`, `taskRepo` wired up):

```kotlin
// ── Workflow replay ───────────────────────────────────────────────

@Test
fun `replayWorkflow resets FAILED workflow and replays dead-lettered tasks`() = runTest {
    val wf = makeWorkflow(status = WorkflowStatus.FAILED)
    insertWorkflowDirect(wf)

    val dl1 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
    val dl2 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
    val completed = makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED)
    insertTaskDirect(dl1)
    insertTaskDirect(dl2)
    insertTaskDirect(completed)

    val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
    val result = engine.replayWorkflow(wf.id)

    assertTrue(result)
    val wfRow = readWorkflowDirect(wf.id)!!
    assertEquals("RUNNING", wfRow["STATUS"])
    assertEquals("PENDING", readTaskDirect(dl1.id)!!["STATUS"])
    assertEquals("PENDING", readTaskDirect(dl2.id)!!["STATUS"])
    assertEquals("COMPLETED", readTaskDirect(completed.id)!!["STATUS"])
}

@Test
fun `replayWorkflow returns false for RUNNING workflow`() = runTest {
    val wf = makeWorkflow(status = WorkflowStatus.RUNNING)
    insertWorkflowDirect(wf)

    val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
    val result = engine.replayWorkflow(wf.id)

    assertFalse(result)
}

@Test
fun `replayWorkflow returns false for COMPLETED workflow`() = runTest {
    val wf = makeWorkflow(status = WorkflowStatus.COMPLETED)
    insertWorkflowDirect(wf)

    val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
    val result = engine.replayWorkflow(wf.id)

    assertFalse(result)
}

@Test
fun `replayWorkflow returns false for non-existent workflow`() = runTest {
    val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
    val result = engine.replayWorkflow(randomId())

    assertFalse(result)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest#replayWorkflow resets FAILED workflow and replays dead-lettered tasks" -pl .`

Expected: FAIL — `replayWorkflow` method does not exist.

- [ ] **Step 3: Implement `replayWorkflow`**

In `src/main/kotlin/engine/WorkflowEngine.kt`, add after `startWorkflow`:

```kotlin
suspend fun replayWorkflow(workflowId: String): Boolean =
    jdbi.inTransactionSuspend<Boolean, Exception> { handle ->
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: return@inTransactionSuspend false
        if (workflow.status != WorkflowStatus.FAILED) return@inTransactionSuspend false

        workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.RUNNING)
        taskRepo.replayDeadLetterBatchWithHandle(handle, workflowId)
        true
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest" -pl .`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/WorkflowEngine.kt \
        src/test/kotlin/engine/SweeperTest.kt
git commit -m "feat: add replayWorkflow for transactional workflow-level replay"
```

---

### Task 9: Full test suite verification

Run all tests and verify coverage.

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`

Expected: All tests PASS.

- [ ] **Step 2: Run coverage check**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`

Expected: Coverage thresholds met.

- [ ] **Step 3: Final commit (if any fixups needed)**

Only if previous steps required adjustments.
