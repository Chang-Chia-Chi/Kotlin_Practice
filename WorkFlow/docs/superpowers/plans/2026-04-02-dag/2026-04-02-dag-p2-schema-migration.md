# DAG Refactor — P2: Schema Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add V2 Flyway migration: drop `current_sequence` from `workflow`, add `activity_name` to `task`, add `SKIPPED` to the task status CHECK constraint. Update `SchemaTest` to match.

**Architecture:** Pure SQL + Oracle DDL changes. No Kotlin model changes in this plan. After this plan, the DB schema matches what the refactored code will expect. `SchemaTest` is an Oracle integration test — Docker Desktop must be running.

**Tech Stack:** Flyway, Oracle Free (Testcontainers), JUnit 5

---

### Task 1: Write failing schema tests for new V2 structure

**Files:**
- Modify: `src/test/kotlin/workflow/adapter/persistent/SchemaTest.kt`

- [ ] **Step 1: Add tests for V2 schema changes**

Add these tests to `SchemaTest.kt` (after the existing tests):

```kotlin
    // ── Test 16: activity_name column exists on task ────────────────────

    @Test
    fun activityNameColumnExistsOnTask() {
        jdbi.useHandle<Exception> { handle ->
            val cols = handle.createQuery(
                "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = 'TASK' AND COLUMN_NAME = 'ACTIVITY_NAME'"
            ).mapTo(String::class.java).list()
            assertEquals(1, cols.size, "Expected ACTIVITY_NAME column on TASK table")
        }
    }

    // ── Test 17: current_sequence column removed from workflow ──────────

    @Test
    fun currentSequenceColumnAbsentFromWorkflow() {
        jdbi.useHandle<Exception> { handle ->
            val cols = handle.createQuery(
                "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = 'WORKFLOW' AND COLUMN_NAME = 'CURRENT_SEQUENCE'"
            ).mapTo(String::class.java).list()
            assertEquals(0, cols.size, "Expected CURRENT_SEQUENCE column to be absent from WORKFLOW table")
        }
    }

    // ── Test 18: SKIPPED is a valid task status ─────────────────────────

    @Test
    fun skippedStatusAccepted() {
        val wfId = insertWorkflow()
        val taskId = insertTask(workflowId = wfId, status = "SKIPPED")
        jdbi.useHandle<Exception> { handle ->
            val status = handle.createQuery("SELECT status FROM task WHERE id = :id")
                .bind("id", taskId)
                .mapTo(String::class.java)
                .one()
            assertEquals("SKIPPED", status)
        }
    }
```

- [ ] **Step 2: Remove `current_sequence` from `insertWorkflow()` helper**

The `insertWorkflow()` helper in `SchemaTest.kt` currently inserts `current_sequence`. After V2 migration drops the column, those inserts will fail.

Replace the entire `insertWorkflow()` helper (lines 47–90 of SchemaTest.kt) with:

```kotlin
    private fun insertWorkflow(
        id: String = randomId(),
        definition: String = """{"activities":{}}""",
        version: Int? = null,
        status: String = "RUNNING",
        createdAt: Instant = now(),
        updatedAt: Instant = now(),
    ): String {
        val createdAtLdt = LocalDateTime.ofInstant(createdAt, ZoneOffset.UTC)
        val updatedAtLdt = LocalDateTime.ofInstant(updatedAt, ZoneOffset.UTC)
        val deadlineAtLdt = createdAtLdt.plusHours(1)
        jdbi.useHandle<Exception> { handle ->
            if (version != null) {
                handle.createUpdate(
                    """INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at)
                       VALUES (:id, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)"""
                )
                    .bind("id", id)
                    .bind("definition", definition)
                    .bind("version", version)
                    .bind("status", status)
                    .bind("createdAt", createdAtLdt)
                    .bind("updatedAt", updatedAtLdt)
                    .bind("deadlineAt", deadlineAtLdt)
                    .execute()
            } else {
                handle.createUpdate(
                    """INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at)
                       VALUES (:id, :definition, :status, :createdAt, :updatedAt, :deadlineAt)"""
                )
                    .bind("id", id)
                    .bind("definition", definition)
                    .bind("status", status)
                    .bind("createdAt", createdAtLdt)
                    .bind("updatedAt", updatedAtLdt)
                    .bind("deadlineAt", deadlineAtLdt)
                    .execute()
            }
        }
        return id
    }
```

- [ ] **Step 3: Update the existing NOT NULL constraint test for workflow**

In `workflowNotNullConstraints()`, remove the `current_sequence` null check block (it no longer applies since the column is dropped). Also update all raw INSERT strings in that test to remove `current_sequence`:

Find all occurrences of:
```sql
INSERT INTO workflow (id, definition, current_sequence, status, created_at, updated_at, deadline_at) VALUES (...)
```

And replace with (removing `current_sequence` and its `:currentSequence` placeholder):
```sql
INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at) VALUES (...)
```

Also remove the null `current_sequence` assertion block:
```kotlin
// Remove this block entirely:
// null current_sequence
assertThrows<UnableToExecuteStatementException> {
    jdbi.useHandle<Exception> { handle ->
        handle.createUpdate(
            "INSERT INTO workflow (id, definition, current_sequence, status, created_at, updated_at, deadline_at) VALUES (:id, 'def', NULL, 'RUNNING', :ts, :ts, :dl)"
        ).bind("id", randomId()).bind("ts", ts).bind("dl", dl).execute()
    }
}
```

- [ ] **Step 4: Update `workflowInsertAndReadRoundTrip` test**

Remove the `currentSequence` check from `workflowInsertAndReadRoundTrip()`:

Remove:
```kotlin
assertEquals(3, (row["CURRENT_SEQUENCE"] as Number).toInt())
```

Update the `insertWorkflow` call to not pass `currentSequence`:
```kotlin
insertWorkflow(
    id = id,
    definition = definition,
    version = 5,
    status = "COMPLETED",
    createdAt = ts,
    updatedAt = ts,
)
```

- [ ] **Step 5: Update `workflowVersionDefaultsToZero` to not use currentSequence**

The `insertWorkflow()` no longer has `currentSequence` parameter — existing callers that don't pass it are fine.

- [ ] **Step 6: Run existing schema tests to confirm they fail as expected**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SchemaTest" -pl WorkFlow`

Expected: Tests `activityNameColumnExistsOnTask`, `currentSequenceColumnAbsentFromWorkflow` FAIL because the column changes don't exist yet. `skippedStatusAccepted` FAIL because SKIPPED is not in the CHECK constraint yet.

- [ ] **Step 7: Commit test changes**

```bash
git add src/test/kotlin/workflow/adapter/persistent/SchemaTest.kt
git commit -m "test: update SchemaTest for V2 schema (no current_sequence, activity_name, SKIPPED)"
```

---

### Task 2: Create V2 Flyway migration

**Files:**
- Create: `src/main/resources/db/migration/V2__dag_schema.sql`

- [ ] **Step 1: Create the migration file**

Create `src/main/resources/db/migration/V2__dag_schema.sql`:

```sql
-- V2: DAG workflow engine schema changes
-- 1. Drop current_sequence from workflow (replaced by per-activity sequence tracking)
-- 2. Add activity_name to task (links task to its DAG node)
-- 3. Add SKIPPED to task status constraint (terminal status for skipped branches)

ALTER TABLE workflow DROP COLUMN current_sequence;

ALTER TABLE task ADD (activity_name VARCHAR2(255));

ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status CHECK (status IN (
    'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED',
    'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'WAITING_FOR_SIGNAL', 'SKIPPED'
));
```

- [ ] **Step 2: Run schema tests to confirm they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SchemaTest" -pl WorkFlow`

Expected: `BUILD SUCCESS` — all 18 schema tests pass

Note: Other integration tests (`DefaultPhaseGateTest`, `WorkflowEngineTest`, etc.) will fail at this point because `JdbiWorkflowRepository` still tries to INSERT into the dropped `current_sequence` column. These are fixed in P3.

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/db/migration/V2__dag_schema.sql
git commit -m "feat: V2 schema — drop current_sequence, add activity_name, SKIPPED status"
```
