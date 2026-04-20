# Trigger P1: Foundation Types Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce `HandlerResult` sealed interface, add `DEFERRED` to `TaskStatus`, add trigger columns to `Task` model + DB schema, and add `defer()`/`findDeferred()` to `TaskRepository`.

**Architecture:** Replace `HandlerOutput` with a sealed `HandlerResult` that supports both `Completed` and `Defer` variants. Extend `Task` and the DB schema with nullable trigger metadata columns. All changes are additive — existing behavior is preserved until the WorkerLoop wiring (P2).

**Tech Stack:** Kotlin, JDBI, Oracle, Flyway migration

---

### Task 1: Add `DEFERRED` to `TaskStatus`

**Files:**
- Modify: `src/main/kotlin/workflow/model/TaskStatus.kt`
- Modify: `src/test/kotlin/workflow/model/WorkflowModelsTest.kt`

- [ ] **Step 1: Write the failing test**

In `WorkflowModelsTest.kt`, add a test that verifies `DEFERRED` exists and is non-terminal:

```kotlin
@Test
fun `DEFERRED status is non-terminal`() {
    val deferred = TaskStatus.valueOf("DEFERRED")
    assertFalse(deferred.isTerminal)
}

@Test
fun `DEFERRED allows transitions from PROCESSING and to terminal states`() {
    assertDoesNotThrow { TaskStatus.requireTransition(TaskStatus.PROCESSING, TaskStatus.DEFERRED) }
    assertDoesNotThrow { TaskStatus.requireTransition(TaskStatus.DEFERRED, TaskStatus.COMPLETED) }
    assertDoesNotThrow { TaskStatus.requireTransition(TaskStatus.DEFERRED, TaskStatus.FAILED) }
    assertDoesNotThrow { TaskStatus.requireTransition(TaskStatus.DEFERRED, TaskStatus.TIMED_OUT) }
    assertDoesNotThrow { TaskStatus.requireTransition(TaskStatus.DEFERRED, TaskStatus.CANCELLED) }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`
Expected: FAIL — `DEFERRED` does not exist yet.

- [ ] **Step 3: Add DEFERRED to TaskStatus**

In `src/main/kotlin/workflow/model/TaskStatus.kt`:

Add `DEFERRED` to the enum (after `CANCELLED`, before `SKIPPED`):

```kotlin
PENDING, PROCESSING, WAITING_FOR_SIGNAL, COMPLETED, FAILED,
TIMED_OUT, DEAD_LETTER, CANCELLED,
DEFERRED,  // waiting for external trigger (not terminal)
SKIPPED;
```

Add to the `allowed` transitions set:

```kotlin
PROCESSING to DEFERRED,
DEFERRED to COMPLETED,
DEFERRED to FAILED,
DEFERRED to TIMED_OUT,
DEFERRED to CANCELLED,
```

`DEFERRED` must NOT be in `terminalStatuses`.

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: add DEFERRED status to TaskStatus enum
```

---

### Task 2: Create `HandlerResult` sealed interface

**Files:**
- Create: `src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt`
- Modify: `src/test/kotlin/worker/usecase/service/execution/HandlerRegistryTest.kt`

- [ ] **Step 1: Write the failing test**

In `HandlerRegistryTest.kt` (or a new `HandlerResultTest.kt` if the file is unrelated), add:

```kotlin
@Test
fun `HandlerResult Completed wraps result`() {
    val result: HandlerResult = HandlerResult.Completed("output")
    assertTrue(result is HandlerResult.Completed)
    assertEquals("output", (result as HandlerResult.Completed).result)
}

@Test
fun `HandlerResult Completed with null result`() {
    val result: HandlerResult = HandlerResult.Completed(null)
    assertNull((result as HandlerResult.Completed).result)
}

@Test
fun `HandlerResult Defer carries trigger metadata`() {
    val result: HandlerResult = HandlerResult.Defer(
        triggerType = "k8s-job",
        triggerMeta = """{"jobName":"test","namespace":"default"}""",
    )
    assertTrue(result is HandlerResult.Defer)
    val defer = result as HandlerResult.Defer
    assertEquals("k8s-job", defer.triggerType)
    assertEquals("""{"jobName":"test","namespace":"default"}""", defer.triggerMeta)
}

@Test
fun `exhaustive when on HandlerResult`() {
    val results = listOf(
        HandlerResult.Completed("ok"),
        HandlerResult.Defer("sql-exec", "{}"),
    )
    for (r in results) {
        val label = when (r) {
            is HandlerResult.Completed -> "completed"
            is HandlerResult.Defer -> "deferred"
        }
        assertTrue(label.isNotEmpty())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="HandlerRegistryTest" -pl WorkFlow`
Expected: FAIL — `HandlerResult` does not exist.

- [ ] **Step 3: Create HandlerResult.kt**

Create `src/main/kotlin/worker/usecase/port/inbound/execution/HandlerResult.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.execution

sealed interface HandlerResult {
    data class Completed(val result: String?) : HandlerResult
    data class Defer(
        val triggerType: String,
        val triggerMeta: String,
    ) : HandlerResult
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="HandlerRegistryTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: add HandlerResult sealed interface with Completed and Defer variants
```

---

### Task 3: Add trigger columns to `Task` model

**Files:**
- Modify: `src/main/kotlin/workflow/model/Task.kt`

- [ ] **Step 1: Add triggerType and triggerMeta to Task data class**

In `src/main/kotlin/workflow/model/Task.kt`, add two nullable fields to the `Task` data class after `queueName`:

```kotlin
val triggerType: String? = null,
val triggerMeta: String? = null,
```

- [ ] **Step 2: Run existing tests to verify nothing breaks**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: PASS — new fields have defaults, so all existing call sites still compile.

- [ ] **Step 3: Commit**

```
feat: add triggerType and triggerMeta fields to Task model
```

---

### Task 4: Schema migration — add trigger columns to task table

**Files:**
- Create: `src/main/resources/db/migration/V2__add_trigger_columns.sql`

- [ ] **Step 1: Create migration file**

Create `src/main/resources/db/migration/V2__add_trigger_columns.sql`:

```sql
-- Add trigger columns for deferrable task support
ALTER TABLE task ADD (
    trigger_type  VARCHAR2(50),
    trigger_meta  CLOB
);

-- Update CHECK constraint to include DEFERRED status
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status CHECK (status IN (
    'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED',
    'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'WAITING_FOR_SIGNAL', 'SKIPPED',
    'DEFERRED'
));

-- Index for trigger loop sweep query
CREATE INDEX idx_task_deferred ON task (status) WHERE status = 'DEFERRED';
```

Note: Oracle does not support partial indexes (`WHERE` clause). Replace the last line with:

```sql
-- Index for trigger loop sweep query (Oracle-compatible)
CREATE INDEX idx_task_deferred ON task (status, trigger_type);
```

- [ ] **Step 2: Run SchemaTest to verify migration applies**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SchemaTest" -pl WorkFlow`
Expected: PASS — Flyway applies V2 migration.

- [ ] **Step 3: Commit**

```
feat: add V2 migration for trigger columns and DEFERRED status
```

---

### Task 5: Update `JdbiTaskRepository` — mapTaskRow, insertBatch, defer, findDeferred

**Files:**
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`
- Modify: `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt`
- Create: `src/main/kotlin/worker/usecase/port/inbound/trigger/DeferredTaskRef.kt`

- [ ] **Step 1: Create DeferredTaskRef data class**

Create `src/main/kotlin/worker/usecase/port/inbound/trigger/DeferredTaskRef.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

import java.time.Instant

data class DeferredTaskRef(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val triggerType: String,
    val triggerMeta: String,
    val deadlineAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
)
```

- [ ] **Step 2: Add defer() and findDeferred() to TaskRepository interface**

In `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt`, add:

```kotlin
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef

// ... existing methods ...

suspend fun defer(taskId: String, triggerType: String, triggerMeta: String): Boolean
suspend fun findDeferred(): List<DeferredTaskRef>
```

- [ ] **Step 3: Update mapTaskRow to read trigger columns**

In `JdbiTaskRepository.mapTaskRow()`, add after the `queueName` line:

```kotlin
triggerType = ci["TRIGGER_TYPE"] as? String,
triggerMeta = ci["TRIGGER_META"]?.let { readClob(it) },
```

- [ ] **Step 4: Update insertBatchWithHandle to write trigger columns**

In `JdbiTaskRepository.insertBatchWithHandle()`, add `trigger_type` and `trigger_meta` to the INSERT SQL and bind them:

SQL columns: add `trigger_type, trigger_meta` after `queue_name`.
SQL values: add `:triggerType, :triggerMeta` after `:queueName`.
Bind:
```kotlin
bindNullableClob(batch, "triggerType", task.triggerType?.let { it } )
bindNullableClob(batch, "triggerMeta", task.triggerMeta)
```

Wait — `triggerType` is VARCHAR2(50), not CLOB. Use:
```kotlin
if (task.triggerType != null) batch.bind("triggerType", task.triggerType) else batch.bindNull("triggerType", Types.VARCHAR)
bindNullableClob(batch, "triggerMeta", task.triggerMeta)
```

- [ ] **Step 5: Implement defer() in JdbiTaskRepository**

```kotlin
override suspend fun defer(taskId: String, triggerType: String, triggerMeta: String): Boolean =
    jdbi.inTransactionSuspend<Boolean, Exception> { h: Handle ->
        val count = h.createUpdate(
            """
            UPDATE task
            SET status = 'DEFERRED', trigger_type = :triggerType, trigger_meta = :triggerMeta
            WHERE id = :taskId AND status = 'PROCESSING'
            """,
        )
            .bind("taskId", taskId)
            .bind("triggerType", triggerType)
            .bind("triggerMeta", triggerMeta)
            .execute()
        count > 0
    }
```

- [ ] **Step 6: Implement findDeferred() in JdbiTaskRepository**

```kotlin
override suspend fun findDeferred(): List<DeferredTaskRef> =
    jdbi.withHandleSuspend<List<DeferredTaskRef>, Exception> { h: Handle ->
        h.createQuery(
            """
            SELECT id, workflow_id, sequence_number, trigger_type, trigger_meta, deadline_at, retry_count, max_retries
            FROM task
            WHERE status = 'DEFERRED'
            """,
        )
            .mapToMap()
            .list()
            .map { row ->
                val ci = caseInsensitive(row)
                DeferredTaskRef(
                    taskId = ci["ID"] as String,
                    workflowId = ci["WORKFLOW_ID"] as String,
                    sequenceNumber = (ci["SEQUENCE_NUMBER"] as Number).toInt(),
                    triggerType = ci["TRIGGER_TYPE"] as String,
                    triggerMeta = readClob(ci["TRIGGER_META"]!!),
                    deadlineAt = readNullableTimestamp(ci["DEADLINE_AT"]),
                    retryCount = (ci["RETRY_COUNT"] as Number).toInt(),
                    maxRetries = (ci["MAX_RETRIES"] as Number).toInt(),
                )
            }
    }
```

- [ ] **Step 7: Run all tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: PASS

- [ ] **Step 8: Commit**

```
feat: add defer() and findDeferred() to TaskRepository with trigger column support
```

---

### Task 6: Create TriggerTypes constants and helper functions

**Files:**
- Create: `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt`
- Create: `src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerTypesTest.kt`

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/worker/usecase/port/inbound/trigger/TriggerTypesTest.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class TriggerTypesTest {

    private val objectMapper: ObjectMapper = jacksonObjectMapper()

    @Test
    fun `deferK8sJob creates Defer with correct type and meta`() {
        val result = deferK8sJob(objectMapper, "my-job", "ml-namespace")
        assertEquals(TriggerTypes.K8S_JOB, result.triggerType)
        val meta = objectMapper.readValue<Map<String, String>>(result.triggerMeta)
        assertEquals("my-job", meta["jobName"])
        assertEquals("ml-namespace", meta["namespace"])
    }

    @Test
    fun `deferSqlExec creates Defer with correct type and meta`() {
        val result = deferSqlExec(
            objectMapper,
            datasource = "warehouse",
            sql = "CALL run_etl(:taskId)",
            params = mapOf("taskId" to "t-123"),
        )
        assertEquals(TriggerTypes.SQL_EXEC, result.triggerType)
        val meta = objectMapper.readValue<Map<String, Any>>(result.triggerMeta)
        assertEquals("warehouse", meta["datasource"])
        assertEquals("CALL run_etl(:taskId)", meta["sql"])
        @Suppress("UNCHECKED_CAST")
        assertEquals("t-123", (meta["params"] as Map<String, Any>)["taskId"])
    }

    @Test
    fun `deferSqlExec with empty params`() {
        val result = deferSqlExec(objectMapper, "default", "SELECT 1 FROM DUAL")
        val meta = objectMapper.readValue<Map<String, Any>>(result.triggerMeta)
        @Suppress("UNCHECKED_CAST")
        assertTrue((meta["params"] as Map<String, Any>).isEmpty())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerTypesTest" -pl WorkFlow`
Expected: FAIL — `TriggerTypes` does not exist.

- [ ] **Step 3: Create TriggerTypes.kt**

Create `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt`:

```kotlin
package com.workflow.worker.usecase.port.inbound.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult

object TriggerTypes {
    const val K8S_JOB = "k8s-job"
    const val SQL_EXEC = "sql-exec"
}

fun deferK8sJob(
    objectMapper: ObjectMapper,
    jobName: String,
    namespace: String,
): HandlerResult.Defer = HandlerResult.Defer(
    triggerType = TriggerTypes.K8S_JOB,
    triggerMeta = objectMapper.writeValueAsString(
        mapOf("jobName" to jobName, "namespace" to namespace),
    ),
)

fun deferSqlExec(
    objectMapper: ObjectMapper,
    datasource: String,
    sql: String,
    params: Map<String, Any?> = emptyMap(),
): HandlerResult.Defer = HandlerResult.Defer(
    triggerType = TriggerTypes.SQL_EXEC,
    triggerMeta = objectMapper.writeValueAsString(
        mapOf("datasource" to datasource, "sql" to sql, "params" to params),
    ),
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="TriggerTypesTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: add TriggerTypes constants and deferK8sJob/deferSqlExec helpers
```
