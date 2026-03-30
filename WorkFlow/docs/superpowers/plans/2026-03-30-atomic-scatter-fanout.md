# Atomic Scatter Fan-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CLOB round-trip in scatter-to-parallel fan-out with a single-transaction atomic path that passes handler results directly from memory.

**Architecture:** Introduce `onScatterTaskCompleted` in BarrierService that merges task completion + barrier evaluation + fan-out insertion into one transaction. WorkerLoop detects scatter tasks via the cached definition's sequence map and routes to the new path. The old `insertFanOutFromScatter` (JSON_TABLE-based) is deleted.

**Tech Stack:** Kotlin, JDBI 3, Oracle, JUnit 5, kotlinx-coroutines-test

---

### File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/main/kotlin/engine/WorkflowModels.kt:95-118` | Add `item` parameter to `createTaskForActivity` |
| Modify | `src/main/kotlin/engine/TaskRepository.kt:206-248` | Delete `insertFanOutFromScatter`, add `insertFanOutTasks` |
| Modify | `src/main/kotlin/engine/BarrierService.kt` | Add `onScatterTaskCompleted`, thread `scatterResult` through `evaluateAndAdvance` → `resolveAndExecute` → `executeDecision` |
| Modify | `src/main/kotlin/worker/WorkerLoop.kt:220-268` | Detect scatter tasks and route to `onScatterTaskCompleted` |
| Modify | `src/test/kotlin/benchmark/InstrumentedComponents.kt:31-36` | Update instrumented override to match new method signature |
| Modify | `src/test/kotlin/engine/BarrierServiceTest.kt:764-849` | Update scatter fan-out tests (Test 11, 12) to use `onScatterTaskCompleted` |
| Modify | `src/test/kotlin/engine/WorkflowIntegrationTest.kt` | Update E2E scatter tests to use `onScatterTaskCompleted` |

---

### Task 1: Add `item` parameter to `createTaskForActivity`

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowModels.kt:95-118`

- [ ] **Step 1: Add the `item` parameter**

In `src/main/kotlin/engine/WorkflowModels.kt`, modify `createTaskForActivity` to accept an optional `item` parameter:

```kotlin
internal fun createTaskForActivity(
    workflowId: String,
    sequenceNumber: Int,
    activity: ActivityDefinition,
    now: Instant,
    item: String? = null,
): Task {
    return Task(
        id = UUID.randomUUID().toString(),
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = TaskStatus.PENDING,
        handlerKey = activity.transition,
        item = item,
        resultJson = null,
        claimedBy = null,
        claimedAt = null,
        completedAt = null,
        retryCount = 0,
        maxRetries = activity.retries,
        deadlineAt = now.plus(activity.deadline),
        backoffBase = activity.backoffBase.seconds.toInt(),
        backoffCap = activity.backoffCap.seconds.toInt(),
        queueName = activity.queue,
    )
}
```

- [ ] **Step 2: Verify existing tests still pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest" -pl WorkFlow`
Expected: All tests PASS (no behavioral change — `item` defaults to `null`).

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/engine/WorkflowModels.kt
git commit -m "refactor: add item parameter to createTaskForActivity"
```

---

### Task 2: Replace `insertFanOutFromScatter` with `insertFanOutTasks` in TaskRepository

**Files:**
- Modify: `src/main/kotlin/engine/TaskRepository.kt:206-248`

- [ ] **Step 1: Write the failing test**

Add a test in `src/test/kotlin/engine/RepositoryTest.kt` inside a new `@Nested inner class InsertFanOutTasks` block. This test creates fan-out tasks from an in-memory items list using the new method:

```kotlin
@Nested
inner class InsertFanOutTasks {

    @Test
    fun `insertFanOutTasks batch-inserts one task per item with correct fields`() {
        val wfId = UUID.randomUUID().toString()
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        insertWorkflowDirect(wfId)

        val activity = ActivityDefinition(
            name = "parallel-activity",
            transition = "parallel.handler",
            retries = 2,
            deadline = Duration.ofMinutes(10),
            queue = "test-queue",
        )
        val targetSeqInfo = SequenceInfo(
            sequenceNumber = 2,
            activityIndex = 1,
            activity = activity,
            phaseType = PhaseType.PARALLEL,
            nextSequence = null,
        )
        val items = listOf("""{"item":"A"}""", """{"item":"B"}""", """{"item":"C"}""")

        jdbi.useHandle<Exception> { handle ->
            taskRepo.insertFanOutTasks(handle, wfId, items, targetSeqInfo, now)
        }

        val tasks = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(3, tasks.size)
        assertTrue(tasks.all { it.handlerKey == "parallel.handler" })
        assertTrue(tasks.all { it.status == TaskStatus.PENDING })
        assertTrue(tasks.all { it.maxRetries == 2 })
        assertTrue(tasks.all { it.queueName == "test-queue" })
        assertEquals(items.toSet(), tasks.map { it.item }.toSet())
    }

    @Test
    fun `insertFanOutTasks with empty list throws IllegalArgumentException`() {
        val wfId = UUID.randomUUID().toString()
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        insertWorkflowDirect(wfId)

        val activity = ActivityDefinition(
            name = "parallel-activity",
            transition = "parallel.handler",
        )
        val targetSeqInfo = SequenceInfo(
            sequenceNumber = 2,
            activityIndex = 1,
            activity = activity,
            phaseType = PhaseType.PARALLEL,
            nextSequence = null,
        )

        jdbi.useHandle<Exception> { handle ->
            assertThrows<IllegalArgumentException> {
                taskRepo.insertFanOutTasks(handle, wfId, emptyList(), targetSeqInfo, now)
            }
        }
    }
}
```

Note: `insertWorkflowDirect` is a test helper that already exists in `RepositoryTest` for inserting a workflow row. If it takes only an `id` parameter, use it; otherwise replicate the pattern used by existing tests in the same class.

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest$InsertFanOutTasks" -pl WorkFlow`
Expected: FAIL — `insertFanOutTasks` does not exist yet.

- [ ] **Step 3: Implement `insertFanOutTasks` and delete `insertFanOutFromScatter`**

In `src/main/kotlin/engine/TaskRepository.kt`, replace the `insertFanOutFromScatter` method (lines 206-248) with:

```kotlin
fun insertFanOutTasks(
    handle: Handle,
    workflowId: String,
    items: List<String>,
    targetSeqInfo: SequenceInfo,
    now: Instant,
) {
    require(items.isNotEmpty()) {
        "Fan-out items must not be empty for workflow $workflowId at sequence ${targetSeqInfo.sequenceNumber}."
    }
    val tasks = items.map { item ->
        createTaskForActivity(workflowId, targetSeqInfo.sequenceNumber, targetSeqInfo.activity, now, item = item)
    }
    insertBatchWithHandle(handle, tasks)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest$InsertFanOutTasks" -pl WorkFlow`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/TaskRepository.kt src/test/kotlin/engine/RepositoryTest.kt
git commit -m "feat: replace insertFanOutFromScatter with in-memory insertFanOutTasks"
```

---

### Task 3: Thread `scatterResult` through BarrierService and add `onScatterTaskCompleted`

**Files:**
- Modify: `src/main/kotlin/engine/BarrierService.kt`

- [ ] **Step 1: Write the failing test**

In `src/test/kotlin/engine/BarrierServiceTest.kt`, add a new `@Nested inner class AtomicScatterFanOut` block:

```kotlin
@Nested
inner class AtomicScatterFanOut {

    @Test
    fun `onScatterTaskCompleted atomically completes task and creates fan-out tasks`() = runTest {
        val def = fanOutDef(joinPolicy = JoinPolicy.All)
        val wfId = randomId()
        val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
        insertWorkflowDirect(wf)

        val scatterTaskId = randomId()
        insertTaskDirect(
            makeTask(
                id = scatterTaskId, workflowId = wfId, sequenceNumber = 1,
                status = TaskStatus.PROCESSING, handlerKey = "scatter.handler",
            ),
        )

        val scatterPayloads = listOf("""{"item":"A"}""", """{"item":"B"}""", """{"item":"C"}""")
        val scatterResultJson = objectMapper.writeValueAsString(scatterPayloads)

        barrier.onScatterTaskCompleted(
            taskId = scatterTaskId,
            workflowId = wfId,
            sequenceNumber = 1,
            resultJson = scatterResultJson,
            claimedBy = null,
            claimedAt = null,
        )

        // Scatter task should be COMPLETED with NULL result (no CLOB stored)
        val scatterTask = readTasksDirect(wfId, 1)
        assertEquals(1, scatterTask.size)
        assertEquals("COMPLETED", scatterTask[0]["STATUS"])
        assertNull(scatterTask[0]["RESULT"], "result CLOB should be null — not stored")

        // Workflow advanced to sequence 2
        val updatedWf = readWorkflowDirect(wfId)
        assertNotNull(updatedWf)
        assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())

        // 3 fan-out tasks created at sequence 2
        val seq2Tasks = readTasksDirect(wfId, 2)
        assertEquals(3, seq2Tasks.size)
        assertTrue(seq2Tasks.all { it["HANDLER_KEY"] == "parallel.handler" })
        assertTrue(seq2Tasks.all { it["STATUS"] == "PENDING" })
        val items = seq2Tasks.map { it["ITEM"] as String }.sorted()
        assertEquals(scatterPayloads.sorted(), items)
    }

    @Test
    fun `onScatterTaskCompleted with empty items throws and does not advance`() = runTest {
        val def = fanOutDef(joinPolicy = JoinPolicy.All)
        val wfId = randomId()
        val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
        insertWorkflowDirect(wf)

        val scatterTaskId = randomId()
        insertTaskDirect(
            makeTask(
                id = scatterTaskId, workflowId = wfId, sequenceNumber = 1,
                status = TaskStatus.PROCESSING, handlerKey = "scatter.handler",
            ),
        )

        assertThrows<IllegalArgumentException> {
            barrier.onScatterTaskCompleted(
                taskId = scatterTaskId,
                workflowId = wfId,
                sequenceNumber = 1,
                resultJson = "[]",
                claimedBy = null,
                claimedAt = null,
            )
        }

        // Workflow should NOT have advanced
        val updatedWf = readWorkflowDirect(wfId)
        assertNotNull(updatedWf)
        assertEquals(1, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
    }

    @Test
    fun `onScatterTaskCompleted signals notifier with correct queue`() = runTest {
        val def = fanOutDef(joinPolicy = JoinPolicy.All)
        val wfId = randomId()
        val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
        insertWorkflowDirect(wf)

        val scatterTaskId = randomId()
        insertTaskDirect(
            makeTask(
                id = scatterTaskId, workflowId = wfId, sequenceNumber = 1,
                status = TaskStatus.PROCESSING, handlerKey = "scatter.handler",
            ),
        )

        val signalCountBefore = notifier.signalCount
        barrier.onScatterTaskCompleted(
            taskId = scatterTaskId,
            workflowId = wfId,
            sequenceNumber = 1,
            resultJson = """["a","b"]""",
            claimedBy = null,
            claimedAt = null,
        )

        assertEquals(signalCountBefore + 1, notifier.signalCount)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest$AtomicScatterFanOut" -pl WorkFlow`
Expected: FAIL — `onScatterTaskCompleted` does not exist yet.

- [ ] **Step 3: Implement the changes in BarrierService**

In `src/main/kotlin/engine/BarrierService.kt`:

**3a.** Add the `onScatterTaskCompleted` method after `onTaskCompleted`:

```kotlin
suspend fun onScatterTaskCompleted(
    taskId: String,
    workflowId: String,
    sequenceNumber: Int,
    resultJson: String,
    claimedBy: String?,
    claimedAt: Instant?,
) {
    val items: List<String> = objectMapper.readValue(resultJson)
    var signalQueue: String? = null

    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        val updated = taskRepo.updateStatusWithHandle(
            handle, taskId, TaskStatus.COMPLETED,
            resultJson = null, claimedBy, claimedAt,
        )
        if (!updated) return@inTransactionSuspend

        val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
        if (nonTerminal > 0) return@inTransactionSuspend

        signalQueue = evaluateAndAdvance(handle, workflowId, sequenceNumber, scatterItems = items)
    }

    if (signalQueue != null) notifier.signal(signalQueue!!)
}
```

**3b.** Add `scatterItems` parameter to `evaluateAndAdvance`:

```kotlin
private fun evaluateAndAdvance(
    handle: Handle,
    workflowId: String,
    sequenceNumber: Int,
    scatterItems: List<String>? = null,
): String? {
    val workflow =
        workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: throw IllegalStateException("Workflow not found: $workflowId")
    if (workflow.status != WorkflowStatus.RUNNING) return null
    if (sequenceNumber != workflow.currentSequence) return null

    val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
    val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)

    return resolveAndExecute(handle, workflow, sequenceNumber, failedCount, totalCount, scatterItems)
}
```

**3c.** Add `scatterItems` parameter to `resolveAndExecute`:

```kotlin
private fun resolveAndExecute(
    handle: Handle,
    workflow: WorkflowRun,
    sequenceNumber: Int,
    failedCount: Int,
    totalCount: Int,
    scatterItems: List<String>? = null,
): String? {
    val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
    val sequenceMap = buildSequenceMap(definition)
    val seqInfo =
        sequenceMap[sequenceNumber]
            ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

    val strategy = strategyRegistry.resolve(seqInfo.phaseType)
    val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, totalCount)
    val decision = strategy.resolve(context)

    return executeDecision(handle, workflow, seqInfo, sequenceMap, decision, scatterItems)
}
```

**3d.** Add `scatterItems` parameter to `executeDecision` and replace `insertFanOutFromScatter` with `insertFanOutTasks`:

```kotlin
private fun executeDecision(
    handle: Handle,
    workflow: WorkflowRun,
    seqInfo: SequenceInfo,
    sequenceMap: Map<Int, SequenceInfo>,
    decision: AdvancementDecision,
    scatterItems: List<String>? = null,
): String? {
    when (decision) {
        is AdvancementDecision.Advance -> {
            val casWon =
                workflowRepo.casAdvanceWithHandle(
                    handle,
                    workflow.id,
                    seqInfo.sequenceNumber,
                    decision.nextSequence,
                    workflow.version,
                )
            if (!casWon) {
                log.debug("CAS lost for workflow {} at sequence {}", workflow.id, seqInfo.sequenceNumber)
                return null
            }
            val nextSeqInfo = sequenceMap[decision.nextSequence]!!
            val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
            when (nextSeqInfo.phaseType) {
                PhaseType.PARALLEL -> {
                    val items = scatterItems
                        ?: throw IllegalStateException(
                            "PARALLEL phase requires scatter items but none provided for workflow ${workflow.id}"
                        )
                    taskRepo.insertFanOutTasks(handle, workflow.id, items, nextSeqInfo, now)
                }

                PhaseType.LINEAR -> {
                    taskRepo.insertBatchWithHandle(
                        handle,
                        listOf(createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now)),
                    )
                }
            }
            return nextSeqInfo.activity.queue
        }

        is AdvancementDecision.Complete -> {
            workflowRepo.updateStatusWithHandle(
                handle,
                workflow.id,
                WorkflowStatus.COMPLETED,
                expectedStatus = WorkflowStatus.RUNNING,
            )
            return null
        }

        is AdvancementDecision.Abort -> {
            log.warn("Workflow {} failed at sequence {}: {}", workflow.id, seqInfo.sequenceNumber, decision.reason)
            val updated =
                workflowRepo.updateStatusWithHandle(
                    handle,
                    workflow.id,
                    WorkflowStatus.FAILED,
                    expectedStatus = WorkflowStatus.RUNNING,
                )
            if (updated) {
                taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
            }
            return null
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest" -pl WorkFlow`
Expected: All tests PASS, including existing scatter tests (which still use `onTaskCompleted` and now hit the `IllegalStateException` guard — see Task 5 for migration).

Note: The existing Test 11 (`ScatterToParallelHandoff`) and Test 12 (`EmptyScatterResult`) call `barrier.onTaskCompleted` which routes through the old `onTaskCompleted` → `evaluateAndAdvance(scatterItems=null)` → `executeDecision(scatterItems=null)`. When it hits the `PARALLEL` branch, `scatterItems` is null and it throws `IllegalStateException`. These tests need to be migrated in Task 5. For now, verify the new `AtomicScatterFanOut` tests pass. Run **only** the new nested class first:

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest$AtomicScatterFanOut" -pl WorkFlow`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/BarrierService.kt src/test/kotlin/engine/BarrierServiceTest.kt
git commit -m "feat: add onScatterTaskCompleted with atomic single-transaction fan-out"
```

---

### Task 4: Route scatter tasks in WorkerLoop to `onScatterTaskCompleted`

**Files:**
- Modify: `src/main/kotlin/worker/WorkerLoop.kt:220-268`

- [ ] **Step 1: Write the failing test**

In `src/test/kotlin/worker/WorkerLoopTest.kt`, add a test that verifies scatter tasks call `onScatterTaskCompleted` instead of `onTaskCompleted`. This test mocks the barrier service and verifies the correct method is called:

```kotlin
@Nested
inner class ScatterTaskRouting {

    @Test
    fun `scatter task routes to onScatterTaskCompleted`() = runTest {
        val scatterResult = """["a","b","c"]"""
        val handler = mock<TransitionHandler>()
        whenever(handler.execute(any())).thenReturn(HandlerOutput(scatterResult))
        whenever(handlerRegistry.resolve("scatter.handler")).thenReturn(handler)

        val definition = workflow {
            activity("scatter") {
                transition("scatter.handler")
                fanOut("parallel")
            }
            activity("parallel") {
                transition("parallel.handler")
            }
        }
        val defJson = objectMapper.writeValueAsString(definition)
        val workflowId = UUID.randomUUID().toString()
        whenever(workflowRepo.findById(workflowId)).thenReturn(
            WorkflowRun(
                id = workflowId, definitionJson = defJson,
                currentSequence = 1, version = 0, status = WorkflowStatus.RUNNING,
                createdAt = Instant.now(), updatedAt = Instant.now(),
                deadlineAt = Instant.now().plus(Duration.ofMinutes(30)),
            ),
        )

        val task = Task(
            id = UUID.randomUUID().toString(), workflowId = workflowId,
            sequenceNumber = 1, status = TaskStatus.PROCESSING,
            handlerKey = "scatter.handler", resultJson = null,
            claimedBy = "worker-1", claimedAt = Instant.now(),
            completedAt = null, retryCount = 0, maxRetries = 3,
            deadlineAt = Instant.now().plus(Duration.ofMinutes(30)),
            queueName = "default",
        )
        whenever(taskRepo.claimNext(any(), any(), any())).thenReturn(listOf(task)).thenReturn(emptyList())

        val scope = CoroutineScope(SupervisorJob() + UnconfinedTestDispatcher(testScheduler))
        val job = workerLoop.start(scope)
        advanceUntilIdle()
        workerLoop.shutdown()
        job.join()

        verify(barrierService).onScatterTaskCompleted(
            taskId = eq(task.id),
            workflowId = eq(workflowId),
            sequenceNumber = eq(1),
            resultJson = eq(scatterResult),
            claimedBy = eq(task.claimedBy),
            claimedAt = eq(task.claimedAt),
        )
        verify(barrierService, never()).onTaskCompleted(
            taskId = any(), workflowId = any(), sequenceNumber = any(),
            status = any(), resultJson = any(), claimedBy = any(), claimedAt = any(),
        )
    }

    @Test
    fun `non-scatter task routes to onTaskCompleted`() = runTest {
        val handler = mock<TransitionHandler>()
        whenever(handler.execute(any())).thenReturn(HandlerOutput("""{"done":true}"""))
        whenever(handlerRegistry.resolve("step.handler")).thenReturn(handler)

        val definition = workflow {
            activity("step1") { transition("step.handler") }
            activity("step2") { transition("step2.handler") }
        }
        val defJson = objectMapper.writeValueAsString(definition)
        val workflowId = UUID.randomUUID().toString()
        whenever(workflowRepo.findById(workflowId)).thenReturn(
            WorkflowRun(
                id = workflowId, definitionJson = defJson,
                currentSequence = 1, version = 0, status = WorkflowStatus.RUNNING,
                createdAt = Instant.now(), updatedAt = Instant.now(),
                deadlineAt = Instant.now().plus(Duration.ofMinutes(30)),
            ),
        )

        val task = Task(
            id = UUID.randomUUID().toString(), workflowId = workflowId,
            sequenceNumber = 1, status = TaskStatus.PROCESSING,
            handlerKey = "step.handler", resultJson = null,
            claimedBy = "worker-1", claimedAt = Instant.now(),
            completedAt = null, retryCount = 0, maxRetries = 3,
            deadlineAt = Instant.now().plus(Duration.ofMinutes(30)),
            queueName = "default",
        )
        whenever(taskRepo.claimNext(any(), any(), any())).thenReturn(listOf(task)).thenReturn(emptyList())

        val scope = CoroutineScope(SupervisorJob() + UnconfinedTestDispatcher(testScheduler))
        val job = workerLoop.start(scope)
        advanceUntilIdle()
        workerLoop.shutdown()
        job.join()

        verify(barrierService).onTaskCompleted(
            taskId = eq(task.id),
            workflowId = eq(workflowId),
            sequenceNumber = eq(1),
            status = eq(TaskStatus.COMPLETED),
            resultJson = eq("""{"done":true}"""),
            claimedBy = eq(task.claimedBy),
            claimedAt = eq(task.claimedAt),
        )
        verify(barrierService, never()).onScatterTaskCompleted(
            taskId = any(), workflowId = any(), sequenceNumber = any(),
            resultJson = any(), claimedBy = any(), claimedAt = any(),
        )
    }
}
```

Note: Adapt mock setup to match existing patterns in `WorkerLoopTest.kt` (e.g., how `barrierService`, `handlerRegistry`, `workflowRepo`, `taskRepo` are mocked). The above uses Mockito — match whatever the test file already uses.

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest$ScatterTaskRouting" -pl WorkFlow`
Expected: FAIL — `onScatterTaskCompleted` is never called because `processTask` doesn't route to it yet.

- [ ] **Step 3: Implement scatter detection and routing in WorkerLoop**

In `src/main/kotlin/worker/WorkerLoop.kt`, modify the `processTask` method. After `val output = handler.execute(input)` (line 242), replace the barrier call with routing logic:

```kotlin
val output = handler.execute(input)

try {
    val cached = definitionCache[task.workflowId]
    val seqInfo = cached?.sequenceMap?.get(task.sequenceNumber)
    val nextSeqInfo = seqInfo?.nextSequence?.let { cached.sequenceMap[it] }
    val isScatter = nextSeqInfo?.phaseType == PhaseType.PARALLEL

    if (isScatter && output.result != null) {
        barrierService.onScatterTaskCompleted(
            taskId = task.id,
            workflowId = task.workflowId,
            sequenceNumber = task.sequenceNumber,
            resultJson = output.result,
            claimedBy = task.claimedBy,
            claimedAt = task.claimedAt,
        )
    } else {
        barrierService.onTaskCompleted(
            taskId = task.id,
            workflowId = task.workflowId,
            sequenceNumber = task.sequenceNumber,
            status = TaskStatus.COMPLETED,
            resultJson = output.result,
            claimedBy = task.claimedBy,
            claimedAt = task.claimedAt,
        )
    }
} catch (e: CancellationException) {
    throw e
} catch (e: Exception) {
    log.error("Barrier failed for COMPLETED task {}, falling through to failure path", task.id, e)
    handleTaskFailure(task, e)
}
```

Add the missing import at the top of the file:

```kotlin
import com.workflow.engine.PhaseType
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest" -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/WorkerLoop.kt src/test/kotlin/worker/WorkerLoopTest.kt
git commit -m "feat: route scatter tasks to onScatterTaskCompleted in WorkerLoop"
```

---

### Task 5: Migrate existing scatter tests to use `onScatterTaskCompleted`

**Files:**
- Modify: `src/test/kotlin/engine/BarrierServiceTest.kt:764-849`
- Modify: `src/test/kotlin/engine/WorkflowIntegrationTest.kt`

- [ ] **Step 1: Update BarrierServiceTest Test 11 (`ScatterToParallelHandoff`)**

The existing test at line 771 calls `barrier.onTaskCompleted(scatterTaskId, wfId, 1, TaskStatus.COMPLETED, scatterResultJson)`. Change it to call `onScatterTaskCompleted`:

```kotlin
@Test
fun `scatter task completes with payloads - CAS winner reads result, inserts N sub-tasks at next sequence`() = runTest {
    val def = fanOutDef(joinPolicy = JoinPolicy.All)
    val wfId = randomId()
    val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
    insertWorkflowDirect(wf)

    val scatterTaskId = randomId()
    val scatterPayloads = listOf(
        """{"item":"A"}""",
        """{"item":"B"}""",
        """{"item":"C"}""",
    )
    val scatterResultJson = objectMapper.writeValueAsString(scatterPayloads)

    insertTaskDirect(
        makeTask(
            id = scatterTaskId, workflowId = wfId, sequenceNumber = 1,
            status = TaskStatus.PROCESSING, handlerKey = "scatter.handler",
        ),
    )

    barrier.onScatterTaskCompleted(
        taskId = scatterTaskId,
        workflowId = wfId,
        sequenceNumber = 1,
        resultJson = scatterResultJson,
        claimedBy = null,
        claimedAt = null,
    )

    val updatedWf = readWorkflowDirect(wfId)
    assertNotNull(updatedWf)
    assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())

    val seq2Tasks = readTasksDirect(wfId, 2)
    assertEquals(3, seq2Tasks.size)
    assertTrue(seq2Tasks.all { it["HANDLER_KEY"] == "parallel.handler" })
    assertTrue(seq2Tasks.all { it["STATUS"] == "PENDING" })
    val items = seq2Tasks.map { it["ITEM"] as String }.sorted()
    assertEquals(scatterPayloads.sorted(), items)
}
```

- [ ] **Step 2: Update BarrierServiceTest Test 12 (`EmptyScatterResult`)**

Change the `onTaskCompleted` call to `onScatterTaskCompleted`:

```kotlin
@Test
fun `scatter task with empty array result fails fast instead of silently skipping parallel phase`() = runTest {
    val def = fanOutDef(joinPolicy = JoinPolicy.All)
    val wfId = randomId()
    val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
    insertWorkflowDirect(wf)

    val scatterTaskId = randomId()

    insertTaskDirect(
        makeTask(
            id = scatterTaskId, workflowId = wfId, sequenceNumber = 1,
            status = TaskStatus.PROCESSING, handlerKey = "scatter.handler",
        ),
    )

    val ex = assertThrows<IllegalArgumentException> {
        barrier.onScatterTaskCompleted(
            taskId = scatterTaskId,
            workflowId = wfId,
            sequenceNumber = 1,
            resultJson = "[]",
            claimedBy = null,
            claimedAt = null,
        )
    }
    assertTrue(ex.message!!.contains("Fan-out items must not be empty"))

    val updatedWf = readWorkflowDirect(wfId)
    assertNotNull(updatedWf)
    assertEquals(1, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
}
```

- [ ] **Step 3: Update BarrierServiceTest Test 13 (`AbortCancelsSiblings`)**

The test at line 878 uses `barrier.onTaskCompleted` to complete the scatter task. Change to `onScatterTaskCompleted`:

```kotlin
barrier.onScatterTaskCompleted(
    taskId = scatterTasks[0].id,
    workflowId = workflowId,
    sequenceNumber = 1,
    resultJson = """["a","b","c"]""",
    claimedBy = null,
    claimedAt = null,
)
```

- [ ] **Step 4: Update WorkflowIntegrationTest `FanOutWorkflowE2E`**

Find the test at `scatter to 50 parallel sub-tasks then linear completes workflow` and change the scatter completion call from `barrier.onTaskCompleted` to `barrier.onScatterTaskCompleted`:

```kotlin
barrier.onScatterTaskCompleted(
    taskId = scatterTasks[0].id,
    workflowId = runId,
    sequenceNumber = 1,
    resultJson = scatterResult,
    claimedBy = null,
    claimedAt = null,
)
```

- [ ] **Step 5: Update WorkflowIntegrationTest concurrent completion test**

Find the second scatter test in `WorkflowIntegrationTest` (around line 391) that calls `barrier.onTaskCompleted` for scatter completion and change it to `barrier.onScatterTaskCompleted`:

```kotlin
barrier.onScatterTaskCompleted(
    taskId = scatterTasks[0].id,
    workflowId = runId,
    sequenceNumber = 1,
    resultJson = objectMapper.writeValueAsString(payloads),
    claimedBy = null,
    claimedAt = null,
)
```

- [ ] **Step 6: Run all barrier and integration tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest,WorkflowIntegrationTest" -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/test/kotlin/engine/BarrierServiceTest.kt src/test/kotlin/engine/WorkflowIntegrationTest.kt
git commit -m "test: migrate scatter tests to onScatterTaskCompleted"
```

---

### Task 6: Update InstrumentedComponents and run full test suite

**Files:**
- Modify: `src/test/kotlin/benchmark/InstrumentedComponents.kt:31-36`

- [ ] **Step 1: Update `InstrumentedTaskRepository` to override `insertFanOutTasks`**

Replace the `insertFanOutFromScatter` override in `src/test/kotlin/benchmark/InstrumentedComponents.kt`:

```kotlin
override fun insertFanOutTasks(
    handle: Handle, workflowId: String, items: List<String>,
    targetSeqInfo: SequenceInfo, now: Instant,
) = timer.time("task.fanout_insert") {
    super.insertFanOutTasks(handle, workflowId, items, targetSeqInfo, now)
}
```

- [ ] **Step 2: Run the full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/benchmark/InstrumentedComponents.kt
git commit -m "chore: update InstrumentedTaskRepository for new insertFanOutTasks signature"
```

---

### Task 7: Run coverage check

- [ ] **Step 1: Generate JaCoCo report and check coverage**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test jacoco:report -pl WorkFlow`
Then: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`

Expected: Coverage meets thresholds. If any package drops below threshold, add targeted tests for uncovered branches.

- [ ] **Step 2: Commit if any coverage fixes were needed**

```bash
git add -A
git commit -m "test: add coverage for atomic scatter fan-out"
```
