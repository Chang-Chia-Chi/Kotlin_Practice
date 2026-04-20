# Phase 3: TaskRepository Interface Cleanup

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove 8 dead methods from `TaskRepository` interface and their implementations/tests. Reduce the interface from 28 methods to 20.

**Architecture:** Pure deletion. No behavioral changes. No new code.

**Tech Stack:** Kotlin, JDBI

**Spec:** `docs/superpowers/specs/2026-04-05-phase-gate-refactoring-design.md` — Phase 3

**Depends on:** Phase 2 (CAS methods already removed from WorkflowRepository)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt` | Modify | Remove 8 dead methods from interface |
| `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt` | Modify | Remove implementations of 8 dead methods |
| `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt` | Modify | Remove tests for deleted methods |

---

### Task 1: Remove dead methods from TaskRepository interface

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt`

- [ ] **Step 1: Remove 8 methods from the interface**

Remove these lines from `TaskRepository.kt`:

```kotlin
    // REMOVE these suspend methods:
    suspend fun updateStatus(id: String, newStatus: TaskStatus, resultJson: String? = null): Boolean
    suspend fun countNonTerminal(workflowId: String, sequenceNumber: Int): Int
    suspend fun countFailed(workflowId: String, sequenceNumber: Int): Int
    suspend fun countTotal(workflowId: String, sequenceNumber: Int): Int

    // REMOVE these Handle methods:
    fun countFailedWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun countTotalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun findByWorkflowAndSequenceWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): List<Task>
    fun countCompletedWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
```

The resulting interface should have exactly 20 methods:

```kotlin
package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import org.jdbi.v3.core.Handle
import java.time.Instant

interface TaskRepository {
    suspend fun insertBatch(tasks: List<Task>)
    suspend fun claimNext(workerId: String, limit: Int, queueName: String = "default"): List<Task>
    suspend fun findByWorkflowAndSequence(workflowId: String, sequenceNumber: Int): List<Task>
    suspend fun resetForRetry(id: String, newRetryCount: Int)
    suspend fun replayDeadLetterTask(taskId: String): Boolean
    suspend fun replayDeadLetterBatch(workflowId: String): Int
    suspend fun findExpired(now: Instant): List<Task>
    suspend fun resetStaleTasks(staleThreshold: Instant): Int
    suspend fun deadLetterExhaustedTasks(staleThreshold: Instant): Int

    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: TaskStatus, resultJson: String? = null, claimedBy: String? = null, claimedAt: Instant? = null): Boolean
    fun countNonTerminalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int
    fun insertBatchWithHandle(handle: Handle, tasks: List<Task>)
    fun replayDeadLetterBatchWithHandle(handle: Handle, workflowId: String): Int
    fun countAllNonTerminalWithHandle(handle: Handle, workflowId: String): Int
    fun findDistinctQueuesByWorkflowId(handle: Handle, workflowId: String, statuses: List<String>): List<String>
    fun countStatusSummariesByWorkflowWithHandle(handle: Handle, workflowId: String): Map<Int, TaskStatusCounts>
    fun findByWorkflowIdWithHandle(handle: Handle, workflowId: String): List<Task>
    fun cancelTasksForOverdueWorkflowsWithHandle(handle: Handle, now: java.time.LocalDateTime): Int
}
```

- [ ] **Step 2: Commit interface change**

```bash
git add src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt
git commit -m "refactor(workflow): remove 8 dead methods from TaskRepository interface"
```

---

### Task 2: Remove dead implementations from JdbiTaskRepository

**Files:**
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`

- [ ] **Step 1: Remove implementations**

Remove these method implementations from `JdbiTaskRepository.kt`:

1. `override suspend fun updateStatus(...)` (L82-89)
2. `override suspend fun countNonTerminal(...)` (L91-97)
3. `override suspend fun countFailed(...)` (L99-105)
4. `override suspend fun countTotal(...)` (L107-113)
5. `override fun countFailedWithHandle(...)` (L276-290)
6. `override fun countTotalWithHandle(...)` (L293-304)
7. `override fun findByWorkflowAndSequenceWithHandle(...)` (L306-318)
8. `override fun countCompletedWithHandle(...)` (L396-407)

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl .`
Expected: SUCCESS (no production code references the removed methods).

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt
git commit -m "refactor(workflow): remove dead method implementations from JdbiTaskRepository"
```

---

### Task 3: Remove dead tests from RepositoryTest

**Files:**
- Modify: `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`

- [ ] **Step 1: Remove test blocks for deleted methods**

Remove the following test sections from `RepositoryTest.kt`:

1. `countNonTerminal` tests (around L687-731) — the `countNonTerminal counts PENDING and PROCESSING tasks`, `returns zero when all tasks terminal`, `returns zero when no tasks exist`, `scoped to sequence number` tests
2. `countNonTerminal excludes DEAD_LETTER` test (around L1285-1293)
3. `countFailed` tests (around L750-785) — `counts FAILED tasks`, `returns zero when no failures`, `scoped to sequence number` tests
4. `countFailedWithHandle` test (around L788-803)
5. `countFailed includes DEAD_LETTER` test (around L1297-1306)
6. `countTotal` tests (around L804-836) — `counts all tasks at sequence`, `returns zero when no tasks`, `scoped to sequence number` tests
7. `countTotalWithHandle` test (around L839-854)

Note: The exact line numbers may have shifted due to Phase 2 changes. Search for the test method names to find the current locations.

- [ ] **Step 2: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All remaining tests PASS. Test count will decrease by ~15-20 (the removed tests).

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt
git commit -m "test(workflow): remove tests for deleted TaskRepository methods"
```
