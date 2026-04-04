# Phase 3: Bulk Queries and N+1 Elimination

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate N+1 query patterns in DefaultPhaseGate by introducing bulk query methods and replacing per-sequence/per-predecessor individual queries with pre-fetched in-memory maps.

**Architecture:** Add `TaskStatusCounts` value class and two new bulk methods to `TaskRepository` port + adapter. Then refactor `DefaultPhaseGate.onTaskCompleted()` and `recoverStuckWorkflow()` to pre-fetch all task data upfront and evaluate in-memory. The existing individual count/find methods remain on the port for use by other callers.

**Tech Stack:** Kotlin, JDBI, Oracle SQL, JUnit 5

---

## Task 1: Add TaskStatusCounts and bulk count method to TaskRepository

**Files:**
- Create: `src/main/kotlin/workflow/model/TaskStatusCounts.kt`
- Modify: `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt`
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`
- Test: `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`

- [ ] **Step 1: Write the test for bulk status counts**

In `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`, add this test (inside the existing class, adapting to the test style used there — uses `OracleTestContainer.jdbi`):

```kotlin
    @Test
    fun `countStatusSummariesByWorkflow returns counts grouped by sequence`() = runTest {
        val wfId = randomId()
        workflowRepo.insert(makeWorkflow(id = wfId))

        val tasks = listOf(
            makeTask(workflowId = wfId, sequenceNumber = 1, status = TaskStatus.COMPLETED),
            makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.PENDING),
            makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED),
            makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.FAILED),
        )
        taskRepo.insertBatch(tasks)

        val result = jdbi.withHandle<Map<Int, TaskStatusCounts>, Exception> { h ->
            taskRepo.countStatusSummariesByWorkflowWithHandle(h, wfId)
        }

        val seq1 = result[1]!!
        assertEquals(1, seq1.total)
        assertEquals(1, seq1.completed)
        assertEquals(0, seq1.nonTerminal)
        assertEquals(0, seq1.failed)

        val seq2 = result[2]!!
        assertEquals(3, seq2.total)
        assertEquals(1, seq2.completed)
        assertEquals(1, seq2.nonTerminal)
        assertEquals(1, seq2.failed)
    }
```

Note: This test uses the existing `makeTask` and `makeWorkflow` helpers and `runTest` pattern from the test file. Add import for `TaskStatusCounts` from `com.workflow.workflow.model.TaskStatusCounts`.

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="RepositoryTest#countStatusSummariesByWorkflow*"`
Expected: FAIL — `TaskStatusCounts` class and `countStatusSummariesByWorkflowWithHandle` method don't exist yet.

- [ ] **Step 3: Create TaskStatusCounts data class**

Create `src/main/kotlin/workflow/model/TaskStatusCounts.kt`:

```kotlin
package com.workflow.workflow.model

data class TaskStatusCounts(
    val total: Int,
    val completed: Int,
    val nonTerminal: Int,
    val failed: Int,
)
```

- [ ] **Step 4: Add bulk count method to TaskRepository port**

In `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt`, add this method to the interface:

```kotlin
    fun countStatusSummariesByWorkflowWithHandle(handle: Handle, workflowId: String): Map<Int, TaskStatusCounts>
```

Add import: `import com.workflow.workflow.model.TaskStatusCounts`

- [ ] **Step 5: Implement in JdbiTaskRepository**

In `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`, add this method:

```kotlin
    override fun countStatusSummariesByWorkflowWithHandle(handle: Handle, workflowId: String): Map<Int, TaskStatusCounts> =
        handle
            .createQuery(
                """
            SELECT sequence_number,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
                   SUM(CASE WHEN status NOT IN ('COMPLETED','FAILED','TIMED_OUT','DEAD_LETTER','CANCELLED','SKIPPED') THEN 1 ELSE 0 END) AS non_terminal,
                   SUM(CASE WHEN status IN ('FAILED','TIMED_OUT','DEAD_LETTER') THEN 1 ELSE 0 END) AS failed
            FROM task
            WHERE workflow_id = :workflowId
            GROUP BY sequence_number
            """,
            ).bind("workflowId", workflowId)
            .mapToMap()
            .list()
            .associate { rawRow ->
                val row = caseInsensitive(rawRow)
                val seq = (row["SEQUENCE_NUMBER"] as Number).toInt()
                seq to TaskStatusCounts(
                    total = (row["TOTAL"] as Number).toInt(),
                    completed = (row["COMPLETED"] as Number).toInt(),
                    nonTerminal = (row["NON_TERMINAL"] as Number).toInt(),
                    failed = (row["FAILED"] as Number).toInt(),
                )
            }
```

Add import: `import com.workflow.workflow.model.TaskStatusCounts`

- [ ] **Step 6: Run the test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="RepositoryTest#countStatusSummariesByWorkflow*"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/main/kotlin/workflow/model/TaskStatusCounts.kt src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt
git commit -m "feat(workflow): add bulk countStatusSummariesByWorkflow to TaskRepository"
```

---

## Task 2: Add bulk findByWorkflowId method to TaskRepository

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt`
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`
- Test: `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`

- [ ] **Step 1: Write the test**

In `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`, add:

```kotlin
    @Test
    fun `findByWorkflowIdWithHandle returns all tasks for a workflow`() = runTest {
        val wfId = randomId()
        val otherWfId = randomId()
        workflowRepo.insert(makeWorkflow(id = wfId))
        workflowRepo.insert(makeWorkflow(id = otherWfId))

        val tasks = listOf(
            makeTask(workflowId = wfId, sequenceNumber = 1, status = TaskStatus.COMPLETED),
            makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.PENDING),
            makeTask(workflowId = otherWfId, sequenceNumber = 1, status = TaskStatus.PENDING),
        )
        taskRepo.insertBatch(tasks)

        val result = jdbi.withHandle<List<Task>, Exception> { h ->
            taskRepo.findByWorkflowIdWithHandle(h, wfId)
        }

        assertEquals(2, result.size)
        assertTrue(result.all { it.workflowId == wfId })
        assertEquals(setOf(1, 2), result.map { it.sequenceNumber }.toSet())
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="RepositoryTest#findByWorkflowIdWithHandle*"`
Expected: FAIL — method doesn't exist yet.

- [ ] **Step 3: Add method to TaskRepository port**

In `src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt`, add:

```kotlin
    fun findByWorkflowIdWithHandle(handle: Handle, workflowId: String): List<Task>
```

- [ ] **Step 4: Implement in JdbiTaskRepository**

In `src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt`, add:

```kotlin
    override fun findByWorkflowIdWithHandle(handle: Handle, workflowId: String): List<Task> =
        handle
            .createQuery("SELECT * FROM task WHERE workflow_id = :workflowId")
            .bind("workflowId", workflowId)
            .mapToMap()
            .list()
            .map(::mapTaskRow)
```

- [ ] **Step 5: Run the test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="RepositoryTest#findByWorkflowIdWithHandle*"`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt
git commit -m "feat(workflow): add bulk findByWorkflowIdWithHandle to TaskRepository"
```

---

## Task 3: Refactor DefaultPhaseGate.onTaskCompleted to use bulk queries

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`
- Test: `src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt` (existing, must still pass)

This is the core N+1 elimination. We pre-fetch ALL task counts and tasks for the workflow before entering the successor evaluation loop, then use in-memory lookups instead of per-sequence SQL.

- [ ] **Step 1: Refactor onTaskCompleted to pre-fetch task data**

In `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`:

**a)** Add import at the top:

```kotlin
import com.workflow.workflow.model.TaskStatusCounts
```

**b)** After loading the definition and sequenceMap (after `val seqByName = ...` from Phase 1 Task 5), add bulk pre-fetch:

```kotlin
            // Pre-fetch all task counts and tasks for this workflow (eliminates N+1)
            val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
            val allTasks = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
            val tasksBySeq = allTasks.groupBy { it.sequenceNumber }
```

**c)** In step 3c (PARALLEL phase check, around line 126-127), replace:

```kotlin
                val completedCount = taskRepo.countCompletedWithHandle(handle, workflowId, sequenceNumber)
                val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)
```

with:

```kotlin
                val counts = allCounts[sequenceNumber] ?: TaskStatusCounts(0, 0, 0, 0)
                val completedCount = counts.completed
                val totalCount = counts.total
```

**d)** In step 4 (successor evaluation loop), replace the dispatch guard:

```kotlin
                if (taskRepo.countTotalWithHandle(handle, workflowId, sSeq) > 0) continue
```

with:

```kotlin
                if ((allCounts[sSeq]?.total ?: 0) > 0) continue
```

**e)** Replace the predecessor gate:

```kotlin
                val allPredTerminal = successor.predecessorSequences.all { predSeq ->
                    taskRepo.countNonTerminalWithHandle(handle, workflowId, predSeq) == 0
                }
```

with:

```kotlin
                val allPredTerminal = successor.predecessorSequences.all { predSeq ->
                    (allCounts[predSeq]?.nonTerminal ?: 0) == 0
                }
```

**f)** Replace the `isAnyEdgeTaken` call to pass `tasksBySeq`:

First, change the method signature of `isAnyEdgeTaken` to accept `tasksBySeq`:

```kotlin
    private fun isAnyEdgeTaken(
        tasksBySeq: Map<Int, List<Task>>,
        successor: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        definition: WorkflowDefinition,
    ): Boolean {
        val targetActName = successor.activityName
        for ((predActName, predActivity) in definition.activities) {
            val edgesToTarget = predActivity.successors.filter { it.target == targetActName }
            if (edgesToTarget.isEmpty()) continue

            val predOutputSeq = sequenceMap.values
                .firstOrNull { si ->
                    val name = si.activityName.removeSuffix(".__parallel__")
                    name == predActName && (si.phaseType == PhaseType.PARALLEL || si.phaseType == PhaseType.LINEAR)
                }?.sequenceNumber ?: continue

            val predTasks = tasksBySeq[predOutputSeq] ?: continue
            for (predTask in predTasks) {
                for (edge in edgesToTarget) {
                    if (isEdgeTaken(predTask, edge.label, predActivity.failurePolicy)) return true
                }
            }
        }
        return false
    }
```

Changes: removed `handle: Handle` and `workflowId: String` parameters, replaced `taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, predOutputSeq)` with `tasksBySeq[predOutputSeq] ?: continue`.

**g)** Update all call sites of `isAnyEdgeTaken` in `onTaskCompleted`:

From:
```kotlin
                    isAnyEdgeTaken(handle, workflowId, successor, sequenceMap, definition)
```

To:
```kotlin
                    isAnyEdgeTaken(tasksBySeq, successor, sequenceMap, definition)
```

**h)** Step 5 (completion check) — `countAllNonTerminalWithHandle` must remain a real DB query because it needs to see newly inserted tasks from this transaction:

```kotlin
                val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
```

This line stays unchanged.

- [ ] **Step 2: Run DefaultPhaseGate tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="DefaultPhaseGateTest"`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt
git commit -m "perf(workflow): onTaskCompleted uses bulk-fetched counts and tasks, eliminates N+1"
```

---

## Task 4: Refactor DefaultPhaseGate.recoverStuckWorkflow to use bulk queries

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`
- Test: `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt` (existing, must still pass)
- Test: `src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt` (existing, must still pass)

- [ ] **Step 1: Refactor recoverStuckWorkflow to pre-fetch task data**

In `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`, replace the `recoverStuckWorkflow` method body. After loading definition, sequenceMap, seqByName (from Phase 1 Task 5), add:

```kotlin
            // Pre-fetch all task data for this workflow (eliminates N+1)
            val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
            val allTasks = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
            val tasksBySeq = allTasks.groupBy { it.sequenceNumber }
```

Then replace the main loop. The full method after refactoring:

```kotlin
    override suspend fun recoverStuckWorkflow(workflowId: String) {
        val signalQueues = withCasRetry(workflowId) { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: run {
                    log.warn("Workflow not found during recovery: {}", workflowId)
                    return@withCasRetry emptyList()
                }
            if (workflow.status != WorkflowStatus.RUNNING) return@withCasRetry emptyList()

            val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
            val sequenceMap = buildSequenceMap(definition)
            val seqByName: Map<String, SequenceInfo> = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
            val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
            val signalQueueSet = mutableSetOf<String>()

            // Pre-fetch all task counts and tasks (eliminates N+1)
            val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
            val tasksBySeq = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
                .groupBy { it.sequenceNumber }

            for ((seq, seqInfo) in sequenceMap.entries.sortedBy { it.key }) {
                if ((allCounts[seq]?.total ?: 0) > 0) continue

                val allPredTerminal = seqInfo.predecessorSequences.isEmpty() ||
                    seqInfo.predecessorSequences.all { predSeq ->
                        (allCounts[predSeq]?.total ?: 0) > 0 &&
                            (allCounts[predSeq]?.nonTerminal ?: 0) == 0
                    }
                if (!allPredTerminal) continue

                when (seqInfo.phaseType) {
                    PhaseType.SCATTER -> {
                        val task = createTaskForActivity(
                            workflowId, seqInfo.activityName, seq, seqInfo.activity, now,
                        )
                        taskRepo.insertBatchWithHandle(handle, listOf(task))
                        signalQueueSet += seqInfo.activity.queue
                    }
                    PhaseType.PARALLEL -> continue
                    PhaseType.LINEAR -> {
                        val edgeTaken = isAnyEdgeTaken(tasksBySeq, seqInfo, sequenceMap, definition)
                        if (edgeTaken || seqInfo.predecessorSequences.isEmpty()) {
                            val task = createTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(task))
                            signalQueueSet += seqInfo.activity.queue
                        } else {
                            val skipped = createSkippedTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(skipped))
                        }
                    }
                }
            }

            // Completion / failure check — must query DB to see newly inserted tasks
            val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
            if (globalNonTerminal == 0) {
                val abortFailure = sequenceMap.entries.any { (seq, seqInfo) ->
                    seqInfo.phaseType != PhaseType.PARALLEL &&
                        seqInfo.activity.failurePolicy == FailurePolicy.ABORT &&
                        (allCounts[seq]?.failed ?: 0) > 0
                }
                val terminalStatus = if (abortFailure) WorkflowStatus.FAILED else WorkflowStatus.COMPLETED
                workflowRepo.updateStatusWithHandle(
                    handle, workflowId, terminalStatus, WorkflowStatus.RUNNING,
                )
                return@withCasRetry emptyList()
            }

            if (signalQueueSet.isEmpty()) return@withCasRetry emptyList()

            requireCasWin(handle, workflowId, workflow.version)
            signalQueueSet.toList()
        }

        signalQueues.forEach { notifier.signal(it) }
    }
```

Key changes:
- `taskRepo.countTotalWithHandle(handle, workflowId, seq)` → `allCounts[seq]?.total ?: 0`
- `taskRepo.countNonTerminalWithHandle(handle, workflowId, predSeq)` → `allCounts[predSeq]?.nonTerminal ?: 0`
- `taskRepo.countFailedWithHandle(handle, workflowId, seq)` → `allCounts[seq]?.failed ?: 0`
- `isAnyEdgeTaken` now uses `tasksBySeq` instead of per-query lookups

- [ ] **Step 2: Run all affected tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f /c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/WorkFlow/pom.xml test -Dtest="DefaultPhaseGateTest,WorkflowWatchdogTest"`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt
git commit -m "perf(workflow): recoverStuckWorkflow uses bulk-fetched counts and tasks, eliminates N+1"
```
