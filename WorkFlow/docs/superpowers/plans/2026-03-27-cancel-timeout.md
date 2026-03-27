# Session 9: Cancel & Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add state machine enforcement, CANCELLED/TIMED_OUT statuses, workflow cancel API, DAG-level deadline, and sweeper timeout enforcement.

**Architecture:** Transition tables in enum companion objects validate all status changes. `updateStatusWithHandle` gets mandatory `expectedStatus` guard for race safety. Sweeper gains `expireOverdueWorkflows()`. Cancel API in `WorkflowEngine`.

**Tech Stack:** Kotlin, JDBI 3, Oracle, Quarkus, JUnit 5, kotlinx-coroutines-test

**Spec:** `docs/superpowers/specs/2026-03-27-cancel-timeout-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/main/kotlin/engine/WorkflowModels.kt` | Modify | Add TIMED_OUT/CANCELLED to enums, transition tables, `deadlineAt` to WorkflowRun |
| `src/main/kotlin/dsl/WorkflowDsl.kt` | Modify | Add `deadline: Duration` to WorkflowDefinition |
| `src/main/kotlin/dsl/WorkflowDslBuilders.kt` | Modify | Add `deadline()` to WorkflowBuilder |
| `src/main/kotlin/engine/WorkflowRepository.kt` | Modify | Generalize `updateStatusWithHandle`, add `findTimedOut`, update `insertWithHandle`/mapper for `deadline_at` |
| `src/main/kotlin/engine/TaskRepository.kt` | Modify | Add `cancelPendingTasksWithHandle`, update hardcoded status lists in SQL |
| `src/main/kotlin/engine/WorkflowEngine.kt` | Modify | Add `cancelWorkflow`, compute `deadlineAt` in `startWorkflow` |
| `src/main/kotlin/engine/BarrierService.kt` | Modify | ABORT path: use `expectedStatus` guard + cancel pending tasks |
| `src/main/kotlin/engine/Sweeper.kt` | Modify | Rename `failExpiredTasks`→`expireOverdueTasks`, add `expireOverdueWorkflows` |
| `src/main/resources/db/migration/V4__cancelled_and_timeout.sql` | Create | Schema migration for new statuses + `deadline_at` |
| `src/test/resources/db/migration/V4__cancelled_and_timeout.sql` | Create | Same migration for test schema |
| `src/test/kotlin/engine/WorkflowModelsTest.kt` | Modify | State machine transition tests |
| `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt` | Modify | Deadline DSL tests |
| `src/test/kotlin/engine/WorkflowEngineTest.kt` | Modify | Cancel API tests, deadline persistence tests |
| `src/test/kotlin/engine/SweeperTest.kt` | Modify | Timeout enforcement tests, expireOverdueTasks status change |
| `src/test/kotlin/engine/BarrierServiceTest.kt` | Modify | ABORT cancels siblings test |

---

## Task 1: Schema Migration

**Files:**
- Create: `src/main/resources/db/migration/V4__cancelled_and_timeout.sql`
- Create: `src/test/resources/db/migration/V4__cancelled_and_timeout.sql`

- [ ] **Step 1: Create migration file**

Create `src/main/resources/db/migration/V4__cancelled_and_timeout.sql`:

```sql
-- Session 9: cancel/timeout statuses + workflow deadline

-- Task: add TIMED_OUT, CANCELLED statuses
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status
    CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED'));

-- Workflow: add TIMED_OUT, CANCELLED statuses
ALTER TABLE workflow DROP CONSTRAINT chk_workflow_status;
ALTER TABLE workflow ADD CONSTRAINT chk_workflow_status
    CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'CANCELLED'));

-- Workflow: add deadline_at for DAG-level timeout (default 1 hour)
ALTER TABLE workflow ADD deadline_at TIMESTAMP;
UPDATE workflow SET deadline_at = created_at + INTERVAL '1' HOUR WHERE deadline_at IS NULL;
ALTER TABLE workflow MODIFY deadline_at NOT NULL;

-- Index for sweeper to find timed-out workflows
CREATE INDEX idx_workflow_deadline ON workflow (status, deadline_at);
```

- [ ] **Step 2: Copy migration to test resources**

Copy the same file to `src/test/resources/db/migration/V4__cancelled_and_timeout.sql`.

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/db/migration/V4__cancelled_and_timeout.sql src/test/resources/db/migration/V4__cancelled_and_timeout.sql
git commit -m "schema: V4 migration for cancel/timeout statuses and workflow deadline_at"
```

---

## Task 2: State Machine — Enum Transition Tables

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowModels.kt`
- Modify: `src/test/kotlin/engine/WorkflowModelsTest.kt`

- [ ] **Step 1: Write failing tests for state machine transitions**

In `src/test/kotlin/engine/WorkflowModelsTest.kt`, replace the existing enum count/valueOf tests and add transition tests. The existing tests that assert "exactly three values" and "exactly five values" must be updated to match the new enum entries.

```kotlin
// ── WorkflowStatus enum ─────────────────────────────────────────────

@Test
fun `WorkflowStatus contains exactly five values`() {
    assertEquals(
        setOf("RUNNING", "COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"),
        WorkflowStatus.entries.map { it.name }.toSet(),
    )
}

@Test
fun `WorkflowStatus isTerminal returns true for all except RUNNING`() {
    assertEquals(false, WorkflowStatus.RUNNING.isTerminal)
    WorkflowStatus.entries.filter { it != WorkflowStatus.RUNNING }.forEach {
        assertEquals(true, it.isTerminal, "Expected isTerminal=true for $it")
    }
}

@Test
fun `WorkflowStatus allows all legal transitions from RUNNING`() {
    listOf(
        WorkflowStatus.COMPLETED,
        WorkflowStatus.FAILED,
        WorkflowStatus.TIMED_OUT,
        WorkflowStatus.CANCELLED,
    ).forEach { target ->
        WorkflowStatus.requireTransition(WorkflowStatus.RUNNING, target) // should not throw
    }
}

@Test
fun `WorkflowStatus allows future reclaim transitions`() {
    listOf(
        WorkflowStatus.FAILED,
        WorkflowStatus.TIMED_OUT,
        WorkflowStatus.CANCELLED,
    ).forEach { source ->
        WorkflowStatus.requireTransition(source, WorkflowStatus.RUNNING) // should not throw
    }
}

@Test
fun `WorkflowStatus rejects illegal transitions`() {
    val illegal = listOf(
        WorkflowStatus.COMPLETED to WorkflowStatus.RUNNING,
        WorkflowStatus.COMPLETED to WorkflowStatus.FAILED,
        WorkflowStatus.FAILED to WorkflowStatus.COMPLETED,
        WorkflowStatus.RUNNING to WorkflowStatus.RUNNING,
    )
    illegal.forEach { (from, to) ->
        val ex = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
            WorkflowStatus.requireTransition(from, to)
        }
        assertTrue(ex.message!!.contains("Illegal workflow transition"))
    }
}

// ── TaskStatus enum ─────────────────────────────────────────────────

@Test
fun `TaskStatus contains exactly seven values`() {
    assertEquals(
        setOf("PENDING", "PROCESSING", "COMPLETED", "FAILED", "TIMED_OUT", "DEAD_LETTER", "CANCELLED"),
        TaskStatus.entries.map { it.name }.toSet(),
    )
}

@Test
fun `isTerminal returns true only for terminal statuses`() {
    val expectedTerminal = setOf(
        TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMED_OUT,
        TaskStatus.DEAD_LETTER, TaskStatus.CANCELLED,
    )
    TaskStatus.entries.forEach { status ->
        assertEquals(
            status in expectedTerminal,
            status.isTerminal,
            "Expected isTerminal=${status in expectedTerminal} for $status",
        )
    }
}

@Test
fun `TaskStatus allows all legal transitions`() {
    val legal = listOf(
        TaskStatus.PENDING to TaskStatus.PROCESSING,
        TaskStatus.PENDING to TaskStatus.CANCELLED,
        TaskStatus.PROCESSING to TaskStatus.COMPLETED,
        TaskStatus.PROCESSING to TaskStatus.FAILED,
        TaskStatus.PROCESSING to TaskStatus.TIMED_OUT,
        TaskStatus.PROCESSING to TaskStatus.PENDING,
        TaskStatus.PROCESSING to TaskStatus.DEAD_LETTER,
        TaskStatus.FAILED to TaskStatus.PENDING,
        TaskStatus.FAILED to TaskStatus.DEAD_LETTER,
    )
    legal.forEach { (from, to) ->
        TaskStatus.requireTransition(from, to) // should not throw
    }
}

@Test
fun `TaskStatus rejects illegal transitions`() {
    val illegal = listOf(
        TaskStatus.PENDING to TaskStatus.COMPLETED,
        TaskStatus.PENDING to TaskStatus.FAILED,
        TaskStatus.COMPLETED to TaskStatus.PENDING,
        TaskStatus.CANCELLED to TaskStatus.PENDING,
        TaskStatus.DEAD_LETTER to TaskStatus.PROCESSING,
    )
    illegal.forEach { (from, to) ->
        val ex = org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
            TaskStatus.requireTransition(from, to)
        }
        assertTrue(ex.message!!.contains("Illegal task transition"))
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`
Expected: Compilation errors (TIMED_OUT, CANCELLED, requireTransition don't exist yet)

- [ ] **Step 3: Implement enum changes in WorkflowModels.kt**

Replace the full content of `src/main/kotlin/engine/WorkflowModels.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import java.time.Instant
import java.util.UUID

enum class WorkflowStatus {
    RUNNING, COMPLETED, FAILED, TIMED_OUT, CANCELLED;

    val isTerminal: Boolean get() = this != RUNNING

    companion object {
        private val allowed = setOf(
            RUNNING to COMPLETED,
            RUNNING to FAILED,
            RUNNING to TIMED_OUT,
            RUNNING to CANCELLED,
            FAILED to RUNNING,       // future: workflow reclaim
            TIMED_OUT to RUNNING,    // future: workflow reclaim
            CANCELLED to RUNNING,    // future: workflow reclaim
        )

        fun requireTransition(from: WorkflowStatus, to: WorkflowStatus) {
            require((from to to) in allowed) {
                "Illegal workflow transition: $from → $to"
            }
        }
    }
}

enum class TaskStatus {
    PENDING, PROCESSING, COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED;

    val isTerminal: Boolean get() = this in terminalStatuses

    companion object {
        private val terminalStatuses = setOf(COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED)
        private val allowed = setOf(
            PENDING to PROCESSING,
            PENDING to CANCELLED,
            PROCESSING to COMPLETED,
            PROCESSING to FAILED,
            PROCESSING to TIMED_OUT,
            PROCESSING to PENDING,       // stale reclaim
            PROCESSING to DEAD_LETTER,
            FAILED to PENDING,           // future: retry-on-failure
            FAILED to DEAD_LETTER,       // future: retry-on-failure exhausted
        )

        fun requireTransition(from: TaskStatus, to: TaskStatus) {
            require((from to to) in allowed) {
                "Illegal task transition: $from → $to"
            }
        }
    }
}

data class WorkflowRun(
    val id: String,
    val definitionJson: String,
    val currentSequence: Int,
    val version: Int,
    val status: WorkflowStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val deadlineAt: Instant,
)

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

internal fun createTaskForActivity(
    workflowId: String,
    sequenceNumber: Int,
    activity: ActivityDefinition,
    payload: String?,
    now: Instant,
): Task {
    return Task(
        id = UUID.randomUUID().toString(),
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = TaskStatus.PENDING,
        handlerKey = activity.transition,
        payloadJson = payload,
        resultJson = null,
        claimedBy = null,
        claimedAt = null,
        completedAt = null,
        retryCount = 0,
        maxRetries = activity.retries,
        deadlineAt = now.plus(activity.deadline),
    )
}
```

**Key changes from current:**
- `WorkflowStatus`: added `TIMED_OUT`, `CANCELLED`, `isTerminal` property, `companion object` with transition table and `requireTransition`
- `TaskStatus`: added `TIMED_OUT`, `CANCELLED`, `companion object` with transition table and `requireTransition`
- `WorkflowRun`: added `deadlineAt: Instant` (non-nullable)

- [ ] **Step 4: Update WorkflowRun helper in WorkflowModelsTest.kt**

The `workflowRun()` helper needs a `deadlineAt` parameter:

```kotlin
private fun workflowRun(
    id: String = "wf-1",
    definitionJson: String = """{"activities":[]}""",
    currentSequence: Int = 1,
    version: Int = 0,
    status: WorkflowStatus = WorkflowStatus.RUNNING,
    createdAt: Instant = now,
    updatedAt: Instant = now,
    deadlineAt: Instant = later,
) = WorkflowRun(id, definitionJson, currentSequence, version, status, createdAt, updatedAt, deadlineAt)
```

Also update `WorkflowRun construction preserves all fields` to assert `deadlineAt`:

```kotlin
@Test
fun `WorkflowRun construction preserves all fields`() {
    val run = workflowRun()
    assertEquals("wf-1", run.id)
    assertEquals("""{"activities":[]}""", run.definitionJson)
    assertEquals(1, run.currentSequence)
    assertEquals(0, run.version)
    assertEquals(WorkflowStatus.RUNNING, run.status)
    assertEquals(now, run.createdAt)
    assertEquals(now, run.updatedAt)
    assertEquals(later, run.deadlineAt)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/engine/WorkflowModels.kt src/test/kotlin/engine/WorkflowModelsTest.kt
git commit -m "feat: state machine transition tables for WorkflowStatus and TaskStatus"
```

---

## Task 3: DSL — Workflow Deadline

**Files:**
- Modify: `src/main/kotlin/dsl/WorkflowDsl.kt`
- Modify: `src/main/kotlin/dsl/WorkflowDslBuilders.kt`
- Modify: `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt`

- [ ] **Step 1: Write failing tests for deadline DSL**

Add to `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt`:

```kotlin
@Test
fun `workflow deadline defaults to 1 hour`() {
    val def = workflow {
        activity("step1") { transition("handler1") }
    }
    assertEquals(Duration.ofHours(1), def.deadline)
}

@Test
fun `workflow deadline can be customized`() {
    val def = workflow {
        deadline(Duration.ofMinutes(30))
        activity("step1") { transition("handler1") }
    }
    assertEquals(Duration.ofMinutes(30), def.deadline)
}

@Test
fun `workflow deadline must be positive`() {
    assertThrows<IllegalArgumentException> {
        workflow {
            deadline(Duration.ZERO)
            activity("step1") { transition("handler1") }
        }
    }
}

@Test
fun `workflow deadline negative throws`() {
    assertThrows<IllegalArgumentException> {
        workflow {
            deadline(Duration.ofMinutes(-1))
            activity("step1") { transition("handler1") }
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowDslBuildersTest" -pl WorkFlow`
Expected: FAIL — `deadline` property doesn't exist

- [ ] **Step 3: Add deadline to WorkflowDefinition**

In `src/main/kotlin/dsl/WorkflowDsl.kt`, change:

```kotlin
data class WorkflowDefinition(
    val activities: List<ActivityDefinition>,
) {
    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
    }
}
```

to:

```kotlin
data class WorkflowDefinition(
    val activities: List<ActivityDefinition>,
    val deadline: Duration = Duration.ofHours(1),
) {
    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
    }
}
```

- [ ] **Step 4: Add deadline to WorkflowBuilder**

In `src/main/kotlin/dsl/WorkflowDslBuilders.kt`, change the `WorkflowBuilder` class:

```kotlin
@WorkflowDsl
class WorkflowBuilder {
    private val activities = mutableListOf<ActivityDefinition>()
    private var deadline: Duration = Duration.ofHours(1)

    fun activity(name: String, block: ActivityBuilder.() -> Unit) {
        activities += ActivityBuilder().apply(block).build(name)
    }

    fun deadline(d: Duration) { deadline = d }

    fun build(): WorkflowDefinition {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        return WorkflowDefinition(activities = activities.toList(), deadline = deadline)
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowDslBuildersTest" -pl WorkFlow`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/dsl/WorkflowDsl.kt src/main/kotlin/dsl/WorkflowDslBuilders.kt src/test/kotlin/dsl/WorkflowDslBuildersTest.kt
git commit -m "feat: add deadline field to WorkflowDefinition and DSL builder"
```

---

## Task 4: Repository — Generalize updateStatusWithHandle + deadline_at Support

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowRepository.kt`
- Modify: `src/main/kotlin/engine/TaskRepository.kt`

- [ ] **Step 1: Update WorkflowRepository.updateStatusWithHandle**

In `src/main/kotlin/engine/WorkflowRepository.kt`, change `updateStatusWithHandle` (line 104-113):

```kotlin
fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus): Boolean {
```

to:

```kotlin
fun updateStatusWithHandle(
    handle: Handle,
    id: String,
    newStatus: WorkflowStatus,
    expectedStatus: WorkflowStatus,
): Boolean {
    WorkflowStatus.requireTransition(expectedStatus, newStatus)
    val count = handle.createUpdate(
        "UPDATE workflow SET status = :status, updated_at = :now WHERE id = :id AND status = :expectedStatus",
    )
        .bind("id", id)
        .bind("status", newStatus.name)
        .bind("expectedStatus", expectedStatus.name)
        .bind("now", LocalDateTime.now(ZoneOffset.UTC))
        .execute()
    return count == 1
}
```

Also update the suspend wrapper `updateStatus` (line 31-34):

```kotlin
suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean =
    jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
        updateStatusWithHandle(h, id, newStatus, expectedStatus)
    }
```

- [ ] **Step 2: Update WorkflowRepository.insertWithHandle for deadline_at**

Change `insertWithHandle` (line 60-75) to include `deadline_at`:

```kotlin
fun insertWithHandle(handle: Handle, run: WorkflowRun) {
    handle.createUpdate(
        """
        INSERT INTO workflow (id, definition, current_sequence, version, status, created_at, updated_at, deadline_at)
        VALUES (:id, :definition, :currentSequence, :version, :status, :createdAt, :updatedAt, :deadlineAt)
        """,
    )
        .bind("id", run.id)
        .bind("definition", run.definitionJson)
        .bind("currentSequence", run.currentSequence)
        .bind("version", run.version)
        .bind("status", run.status.name)
        .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
        .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
        .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, ZoneOffset.UTC))
        .execute()
}
```

- [ ] **Step 3: Update WorkflowRepository.mapWorkflowRow for deadline_at**

Change `mapWorkflowRow` (line 115-126) to include `deadlineAt`:

```kotlin
private fun mapWorkflowRow(row: Map<String, Any?>): WorkflowRun {
    val ci = caseInsensitive(row)
    return WorkflowRun(
        id = ci["ID"] as String,
        definitionJson = readClob(ci["DEFINITION"]),
        currentSequence = (ci["CURRENT_SEQUENCE"] as Number).toInt(),
        version = (ci["VERSION"] as Number).toInt(),
        status = WorkflowStatus.valueOf(ci["STATUS"] as String),
        createdAt = readTimestamp(ci["CREATED_AT"]),
        updatedAt = readTimestamp(ci["UPDATED_AT"]),
        deadlineAt = readTimestamp(ci["DEADLINE_AT"]),
    )
}
```

- [ ] **Step 4: Update WorkflowRepository.findStuck — include TIMED_OUT/CANCELLED in terminal status list**

Change the `findStuck` query's NOT IN clause (line 48):

```sql
AND t.status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED')
```

- [ ] **Step 5: Add WorkflowRepository.findTimedOut**

Add new method after `findStuck`:

```kotlin
suspend fun findTimedOut(): List<WorkflowRun> =
    jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
        h.createQuery(
            """
            SELECT * FROM workflow
            WHERE status = 'RUNNING' AND deadline_at < :now
            """,
        )
            .bind("now", LocalDateTime.now(ZoneOffset.UTC))
            .mapToMap()
            .list()
            .map(::mapWorkflowRow)
    }
```

- [ ] **Step 6: Add TaskRepository.cancelPendingTasksWithHandle**

Add to `src/main/kotlin/engine/TaskRepository.kt` in the Handle methods section (after line 281):

```kotlin
fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int {
    return handle.createUpdate(
        """
        UPDATE task SET status = 'CANCELLED', completed_at = :now
        WHERE workflow_id = :workflowId AND status = 'PENDING'
        """,
    )
        .bind("workflowId", workflowId)
        .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MICROS))
        .execute()
}
```

- [ ] **Step 7: Update hardcoded terminal status lists in TaskRepository SQL**

In `TaskRepository.kt`, update these SQL NOT IN clauses:

Line 192 — `updateStatusWithHandle` terminal branch:
```sql
AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED')
```

Line 232 — `countNonTerminalWithHandle`:
```sql
AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED')
```

Line 249 — `countFailedWithHandle` — add TIMED_OUT since timed-out tasks count as failures:
```sql
AND status IN ('FAILED', 'TIMED_OUT', 'DEAD_LETTER')
```

- [ ] **Step 8: Compile check**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: BUILD SUCCESS (may have downstream compilation errors in callers — those are fixed in Tasks 5-7)

- [ ] **Step 9: Commit**

```bash
git add src/main/kotlin/engine/WorkflowRepository.kt src/main/kotlin/engine/TaskRepository.kt
git commit -m "feat: generalize updateStatusWithHandle with expectedStatus guard, add deadline_at support"
```

---

## Task 5: WorkflowEngine — Cancel API + Deadline Computation

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowEngine.kt`
- Modify: `src/test/kotlin/engine/WorkflowEngineTest.kt`

- [ ] **Step 1: Write failing tests**

Add to `src/test/kotlin/engine/WorkflowEngineTest.kt`:

```kotlin
@Test
fun `startWorkflow sets deadline_at from definition deadline`() = runTest {
    val definition = workflow {
        deadline(Duration.ofMinutes(45))
        activity("step1") {
            transition("order.validate")
        }
    }
    val before = Instant.now()
    val runId = engine.startWorkflow(definition)
    val after = Instant.now()

    val run = workflowRepo.findById(runId)
    assertNotNull(run)
    // deadline should be ~45 minutes from now
    assertTrue(run.deadlineAt.isAfter(before.plus(Duration.ofMinutes(44))))
    assertTrue(run.deadlineAt.isBefore(after.plus(Duration.ofMinutes(46))))
}

@Test
fun `cancelWorkflow transitions RUNNING to CANCELLED and cancels pending tasks`() = runTest {
    val definition = workflow {
        activity("step1") { transition("handler1") }
        activity("step2") { transition("handler2") }
    }
    val runId = engine.startWorkflow(definition, """{"data":"test"}""")

    val result = engine.cancelWorkflow(runId)
    assertTrue(result)

    val run = workflowRepo.findById(runId)
    assertNotNull(run)
    assertEquals(WorkflowStatus.CANCELLED, run.status)

    val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
    assertTrue(tasks.all { it.status == TaskStatus.CANCELLED })
}

@Test
fun `cancelWorkflow returns false for non-RUNNING workflow`() = runTest {
    val definition = workflow {
        activity("step1") { transition("handler1") }
    }
    val runId = engine.startWorkflow(definition)

    // Complete the workflow first
    val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
    barrierService.onTaskCompleted(
        taskId = tasks[0].id,
        workflowId = runId,
        sequenceNumber = 1,
        status = TaskStatus.COMPLETED,
        resultJson = null,
    )

    val result = engine.cancelWorkflow(runId)
    assertEquals(false, result)
}

@Test
fun `cancelWorkflow returns false for nonexistent workflow`() = runTest {
    val result = engine.cancelWorkflow("nonexistent-id")
    assertEquals(false, result)
}
```

Note: The test class needs `barrierService` added as a field. Add to setup:

```kotlin
private lateinit var barrierService: BarrierService

@BeforeAll
fun setup() {
    jdbi = OracleTestContainer.jdbi
    workflowRepo = WorkflowRepository(jdbi)
    taskRepo = TaskRepository(jdbi)
    barrierService = BarrierService(jdbi, workflowRepo, taskRepo, objectMapper)
    engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowEngineTest" -pl WorkFlow`
Expected: Compilation error — `cancelWorkflow` doesn't exist, `deadlineAt` not in WorkflowRun constructor call

- [ ] **Step 3: Update startWorkflow to compute deadlineAt**

In `src/main/kotlin/engine/WorkflowEngine.kt`, update `startWorkflow`:

```kotlin
suspend fun startWorkflow(definition: WorkflowDefinition, initialPayload: String? = null): String {
    require(definition.activities.isNotEmpty()) { "WorkflowDefinition must have at least one activity" }

    val workflowId = UUID.randomUUID().toString()
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    val definitionJson = objectMapper.writeValueAsString(definition)
    val deadlineAt = now.plus(definition.deadline).truncatedTo(ChronoUnit.MICROS)

    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        val run = WorkflowRun(
            id = workflowId,
            definitionJson = definitionJson,
            currentSequence = 1,
            version = 0,
            status = WorkflowStatus.RUNNING,
            createdAt = now,
            updatedAt = now,
            deadlineAt = deadlineAt,
        )
        workflowRepo.insertWithHandle(handle, run)

        val firstActivity = definition.activities.first()
        val task = createTaskForActivity(
            workflowId = workflowId,
            sequenceNumber = 1,
            activity = firstActivity,
            payload = initialPayload,
            now = now,
        )
        taskRepo.insertBatchWithHandle(handle, listOf(task))
    }

    log.info("Started workflow {} with {} activities", workflowId, definition.activities.size)
    return workflowId
}
```

- [ ] **Step 4: Add cancelWorkflow method**

Add to `WorkflowEngine.kt`:

```kotlin
suspend fun cancelWorkflow(workflowId: String): Boolean {
    return jdbi.inTransactionSuspend<Boolean, Exception> { handle ->
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: return@inTransactionSuspend false
        if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend false

        val updated = workflowRepo.updateStatusWithHandle(
            handle, workflowId, WorkflowStatus.CANCELLED, expectedStatus = WorkflowStatus.RUNNING,
        )
        if (updated) {
            taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
            log.info("Cancelled workflow {}", workflowId)
        }
        updated
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowEngineTest" -pl WorkFlow`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/engine/WorkflowEngine.kt src/test/kotlin/engine/WorkflowEngineTest.kt
git commit -m "feat: cancel API and deadline_at computation in WorkflowEngine"
```

---

## Task 6: BarrierService — ABORT Cancels Siblings

**Files:**
- Modify: `src/main/kotlin/engine/BarrierService.kt`
- Modify: `src/test/kotlin/engine/BarrierServiceTest.kt`

- [ ] **Step 1: Write failing test for ABORT + cancel**

Add to `src/test/kotlin/engine/BarrierServiceTest.kt`:

```kotlin
@Test
fun `ABORT failure policy cancels sibling PENDING tasks`() = runTest {
    // Create a workflow where sequence 2 is a PARALLEL fan-out with ABORT policy
    val definition = workflow {
        activity("scatter") {
            transition("scatter.handler")
            fanOut {
                transition("parallel.handler")
                retries(0)
                failurePolicy(FailurePolicy.ABORT)
                joinPolicy(JoinPolicy.All)
            }
        }
    }
    val workflowId = engine.startWorkflow(definition)

    // Complete scatter task with 3 items
    val scatterTasks = taskRepo.findByWorkflowAndSequence(workflowId, 1)
    assertEquals(1, scatterTasks.size)
    barrierService.onTaskCompleted(
        taskId = scatterTasks[0].id,
        workflowId = workflowId,
        sequenceNumber = 1,
        status = TaskStatus.COMPLETED,
        resultJson = """["a","b","c"]""",
    )

    // 3 parallel tasks at sequence 2
    val parallelTasks = taskRepo.findByWorkflowAndSequence(workflowId, 2)
    assertEquals(3, parallelTasks.size)

    // Fail the first one — ABORT policy should cancel the other 2
    barrierService.onTaskCompleted(
        taskId = parallelTasks[0].id,
        workflowId = workflowId,
        sequenceNumber = 2,
        status = TaskStatus.FAILED,
        resultJson = null,
    )

    // Workflow should be FAILED
    val workflow = workflowRepo.findById(workflowId)
    assertNotNull(workflow)
    assertEquals(WorkflowStatus.FAILED, workflow.status)

    // Remaining tasks should be CANCELLED
    val updatedTasks = taskRepo.findByWorkflowAndSequence(workflowId, 2)
    val cancelled = updatedTasks.filter { it.status == TaskStatus.CANCELLED }
    assertEquals(2, cancelled.size)
}
```

Note: This test needs `engine` as a field. Add to the test class's setup if not present:
```kotlin
private lateinit var engine: WorkflowEngine
// In setup():
engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest#ABORT failure policy cancels sibling PENDING tasks" -pl WorkFlow`
Expected: FAIL — remaining tasks are still PENDING, not CANCELLED

- [ ] **Step 3: Update BarrierService.advanceWorkflow ABORT path**

In `src/main/kotlin/engine/BarrierService.kt`, change lines 139-145:

```kotlin
FailurePolicy.ABORT -> {
    workflowRepo.updateStatusWithHandle(handle, workflow.id, WorkflowStatus.FAILED)
    return
}
```

to:

```kotlin
FailurePolicy.ABORT -> {
    val updated = workflowRepo.updateStatusWithHandle(
        handle, workflow.id, WorkflowStatus.FAILED, expectedStatus = WorkflowStatus.RUNNING,
    )
    if (updated) {
        taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
    }
    return
}
```

- [ ] **Step 4: Update BarrierService.advanceWorkflow COMPLETED path**

Also update the COMPLETED transition (line 150-151):

```kotlin
workflowRepo.updateStatusWithHandle(handle, workflow.id, WorkflowStatus.COMPLETED)
```

to:

```kotlin
workflowRepo.updateStatusWithHandle(
    handle, workflow.id, WorkflowStatus.COMPLETED, expectedStatus = WorkflowStatus.RUNNING,
)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest" -pl WorkFlow`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/engine/BarrierService.kt src/test/kotlin/engine/BarrierServiceTest.kt
git commit -m "feat: ABORT failure policy cancels sibling PENDING tasks via expectedStatus guard"
```

---

## Task 7: Sweeper — Timeout Enforcement

**Files:**
- Modify: `src/main/kotlin/engine/Sweeper.kt`
- Modify: `src/test/kotlin/engine/SweeperTest.kt`

- [ ] **Step 1: Write failing tests**

Add to `src/test/kotlin/engine/SweeperTest.kt`:

```kotlin
@Nested
inner class ExpireOverdueWorkflows {

    @Test
    fun `timed-out workflow transitions to TIMED_OUT and cancels pending tasks`() = runTest {
        val definition = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "step1", transition = "handler1"),
                ActivityDefinition(name = "step2", transition = "handler2"),
            ),
        )
        val wfId = randomId()
        val pastDeadline = now().minus(Duration.ofMinutes(5))
        val wf = makeWorkflow(
            id = wfId,
            definition = definition,
            updatedAt = now().minus(Duration.ofHours(1)),
            deadlineAt = pastDeadline,
        )
        workflowRepo.insert(wf)

        // Insert a PENDING task
        val task = Task(
            id = randomId(), workflowId = wfId, sequenceNumber = 1,
            status = TaskStatus.PENDING, handlerKey = "handler1",
            payloadJson = null, resultJson = null,
            claimedBy = null, claimedAt = null, completedAt = null,
            retryCount = 0, maxRetries = 3, deadlineAt = null,
        )
        taskRepo.insertBatch(listOf(task))

        sweeper.patrol()

        val updatedWf = workflowRepo.findById(wfId)
        assertNotNull(updatedWf)
        assertEquals(WorkflowStatus.TIMED_OUT, updatedWf.status)

        val updatedTasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
        assertTrue(updatedTasks.all { it.status == TaskStatus.CANCELLED })
    }

    @Test
    fun `workflow within deadline is not expired`() = runTest {
        val definition = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "step1", transition = "handler1"),
            ),
        )
        val wfId = randomId()
        val futureDeadline = now().plus(Duration.ofHours(1))
        val wf = makeWorkflow(
            id = wfId,
            definition = definition,
            deadlineAt = futureDeadline,
        )
        workflowRepo.insert(wf)

        sweeper.patrol()

        val updatedWf = workflowRepo.findById(wfId)
        assertNotNull(updatedWf)
        assertEquals(WorkflowStatus.RUNNING, updatedWf.status)
    }
}
```

Note: The `makeWorkflow` helper in SweeperTest needs a `deadlineAt` parameter. Update it:

```kotlin
private fun makeWorkflow(
    id: String = randomId(),
    definition: WorkflowDefinition,
    currentSequence: Int = 1,
    version: Int = 0,
    status: WorkflowStatus = WorkflowStatus.RUNNING,
    createdAt: Instant = now(),
    updatedAt: Instant = now(),
    deadlineAt: Instant = now().plus(Duration.ofHours(1)),
): WorkflowRun = WorkflowRun(
    id = id,
    definitionJson = objectMapper.writeValueAsString(definition),
    currentSequence = currentSequence,
    version = version,
    status = status,
    createdAt = createdAt,
    updatedAt = updatedAt,
    deadlineAt = deadlineAt,
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest" -pl WorkFlow`
Expected: FAIL — `expireOverdueWorkflows` doesn't exist, `makeWorkflow` missing `deadlineAt`

- [ ] **Step 3: Rename failExpiredTasks → expireOverdueTasks**

In `src/main/kotlin/engine/Sweeper.kt`, rename the method and change the status from `TaskStatus.FAILED` to `TaskStatus.TIMED_OUT`:

```kotlin
private suspend fun expireOverdueTasks() {
    val expired = taskRepo.findExpired(Instant.now())
    for (task in expired) {
        try {
            log.warn("Expiring overdue task {} (deadline={})", task.id, task.deadlineAt)
            barrierService.onTaskCompleted(
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
```

- [ ] **Step 4: Add expireOverdueWorkflows method**

Add to `Sweeper.kt`:

```kotlin
private suspend fun expireOverdueWorkflows() {
    val timedOut = workflowRepo.findTimedOut()
    for (workflow in timedOut) {
        try {
            jdbi.inTransactionSuspend<Unit, Exception> { handle ->
                val updated = workflowRepo.updateStatusWithHandle(
                    handle, workflow.id, WorkflowStatus.TIMED_OUT, expectedStatus = WorkflowStatus.RUNNING,
                )
                if (updated) {
                    taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                    log.warn("Workflow {} timed out (deadline was {})", workflow.id, workflow.deadlineAt)
                }
            }
        } catch (e: Exception) {
            log.error("Failed to time out workflow {}", workflow.id, e)
        }
    }
}
```

This requires `jdbi` to be injected into Sweeper. Update the constructor:

```kotlin
@ApplicationScoped
class Sweeper(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val barrierService: BarrierService,
    private val config: FrameworkConfig,
)
```

- [ ] **Step 5: Update patrol() to call new methods**

Change `patrol()`:

```kotlin
suspend fun patrol() {
    expireOverdueTasks()
    reclaimStaleTasks()
    recoverStuckWorkflows()
    expireOverdueWorkflows()
}
```

- [ ] **Step 6: Update SweeperTest setup to pass jdbi**

In `SweeperTest.kt`, update the Sweeper construction in `@BeforeAll`:

```kotlin
sweeper = Sweeper(jdbi, workflowRepo, taskRepo, barrier, testConfig)
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SweeperTest" -pl WorkFlow`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add src/main/kotlin/engine/Sweeper.kt src/test/kotlin/engine/SweeperTest.kt
git commit -m "feat: sweeper expireOverdueWorkflows and rename failExpiredTasks to expireOverdueTasks"
```

---

## Task 8: Fix Compilation Across Existing Tests

**Files:**
- Modify: Any test file that constructs `WorkflowRun` without `deadlineAt`

Since `WorkflowRun` now requires `deadlineAt: Instant`, all existing test helpers that construct it will break. This task fixes all of them.

- [ ] **Step 1: Find all WorkflowRun constructions in tests**

Search for `WorkflowRun(` in test files. Key files likely affected:
- `src/test/kotlin/engine/BarrierServiceTest.kt` — `makeWorkflow` helper
- `src/test/kotlin/engine/RepositoryTest.kt` — any direct construction
- `src/test/kotlin/engine/WorkflowIntegrationTest.kt` — workflow helpers

Each `makeWorkflow` or `WorkflowRun(...)` call needs `deadlineAt` added with a sensible default like `now().plus(Duration.ofHours(1))`.

- [ ] **Step 2: Fix each file**

Add `deadlineAt = Instant.now().plus(Duration.ofHours(1))` (or equivalent using the test's `now()` helper) to every `WorkflowRun(...)` construction that's missing it.

- [ ] **Step 3: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: BUILD SUCCESS — all tests pass

- [ ] **Step 4: Commit**

```bash
git add -u
git commit -m "fix: add deadlineAt to all WorkflowRun constructions in tests"
```

---

## Task 9: Full Verification

- [ ] **Step 1: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: BUILD SUCCESS

- [ ] **Step 2: Run coverage check**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`
Expected: Coverage thresholds met

- [ ] **Step 3: Verify migration applies cleanly**

The Oracle Testcontainer tests already run Flyway migrations. If tests pass, V4 applied cleanly.
