# Atomic Scatter Fan-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CLOB round-trip in scatter-to-parallel fan-out with a single-transaction atomic path that passes handler results directly from memory.

**Architecture:** Merge the two transactions in `onTaskCompleted` into one. Thread `resultJson` through to `executeDecision`, which collapses the PARALLEL/LINEAR branch into a single `insertBatchWithHandle` call. Delete `insertFanOutFromScatter` entirely. No new API surface, no WorkerLoop changes.

**Tech Stack:** Kotlin, JDBI 3, Oracle, JUnit 5, kotlinx-coroutines-test

---

### File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/main/kotlin/engine/WorkflowModels.kt:95-118` | Add `item` parameter to `createTaskForActivity` |
| Modify | `src/main/kotlin/engine/TaskRepository.kt:206-248` | Delete `insertFanOutFromScatter` |
| Modify | `src/main/kotlin/engine/BarrierService.kt` | Merge transactions, thread `resultJson`, collapse PARALLEL/LINEAR branch |
| Delete override | `src/test/kotlin/benchmark/InstrumentedComponents.kt:31-36` | Remove `insertFanOutFromScatter` override |
| Modify | `src/test/kotlin/engine/BarrierServiceTest.kt` | Verify no CLOB stored for scatter, add atomicity test |
| No change | `src/main/kotlin/worker/WorkerLoop.kt` | — |
| No change | `src/main/kotlin/engine/Sweeper.kt` | — |

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

### Task 2: Delete `insertFanOutFromScatter` and its instrumented override

**Files:**
- Modify: `src/main/kotlin/engine/TaskRepository.kt:206-248`
- Modify: `src/test/kotlin/benchmark/InstrumentedComponents.kt:31-36`

- [ ] **Step 1: Delete `insertFanOutFromScatter` from TaskRepository**

In `src/main/kotlin/engine/TaskRepository.kt`, delete the entire `insertFanOutFromScatter` method (lines 206-248).

- [ ] **Step 2: Delete the instrumented override**

In `src/test/kotlin/benchmark/InstrumentedComponents.kt`, delete the `insertFanOutFromScatter` override (lines 31-36):

```kotlin
// DELETE this entire block:
override fun insertFanOutFromScatter(
    handle: Handle, workflowId: String, scatterSequence: Int,
    targetSeqInfo: SequenceInfo, now: Instant,
) = timer.time("task.fanout_insert") {
    super.insertFanOutFromScatter(handle, workflowId, scatterSequence, targetSeqInfo, now)
}
```

- [ ] **Step 3: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile test-compile -pl WorkFlow`
Expected: FAIL — `BarrierService.kt` still references `insertFanOutFromScatter`. This is expected; Task 3 fixes it.

- [ ] **Step 4: Commit (WIP — will compile after Task 3)**

```bash
git add src/main/kotlin/engine/TaskRepository.kt src/test/kotlin/benchmark/InstrumentedComponents.kt
git commit -m "refactor: delete insertFanOutFromScatter (replaced in next commit)"
```

---

### Task 3: Merge transactions and collapse PARALLEL/LINEAR branch in BarrierService

**Files:**
- Modify: `src/main/kotlin/engine/BarrierService.kt`

- [ ] **Step 1: Write the failing test**

In `src/test/kotlin/engine/BarrierServiceTest.kt`, add a new `@Nested inner class AtomicScatterFanOut`:

```kotlin
@Nested
inner class AtomicScatterFanOut {

    @Test
    fun `scatter result is not stored as CLOB - task result is null after fan-out`() = runTest {
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

        barrier.onTaskCompleted(scatterTaskId, wfId, 1, TaskStatus.COMPLETED, scatterResultJson)

        // Workflow advanced to sequence 2
        val updatedWf = readWorkflowDirect(wfId)
        assertNotNull(updatedWf)
        assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())

        // 3 fan-out tasks created
        val seq2Tasks = readTasksDirect(wfId, 2)
        assertEquals(3, seq2Tasks.size)
        assertTrue(seq2Tasks.all { it["HANDLER_KEY"] == "parallel.handler" })
        assertTrue(seq2Tasks.all { it["STATUS"] == "PENDING" })
        val items = seq2Tasks.map { it["ITEM"] as String }.sorted()
        assertEquals(scatterPayloads.sorted(), items)
    }

    @Test
    fun `empty scatter result throws and does not advance workflow`() = runTest {
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
            barrier.onTaskCompleted(scatterTaskId, wfId, 1, TaskStatus.COMPLETED, "[]")
        }
        assertTrue(ex.message!!.contains("Fan-out produced 0 items"))

        val updatedWf = readWorkflowDirect(wfId)
        assertNotNull(updatedWf)
        assertEquals(1, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest$AtomicScatterFanOut" -pl WorkFlow`
Expected: FAIL — compilation error since `insertFanOutFromScatter` was deleted in Task 2.

- [ ] **Step 3: Implement the BarrierService changes**

Rewrite `src/main/kotlin/engine/BarrierService.kt` with these changes:

**3a.** Merge the two transactions in `onTaskCompleted` into one:

```kotlin
suspend fun onTaskCompleted(
    taskId: String,
    workflowId: String,
    sequenceNumber: Int,
    status: TaskStatus,
    resultJson: String?,
    claimedBy: String? = null,
    claimedAt: Instant? = null,
) {
    var signalQueue: String? = null

    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
        if (!updated) return@inTransactionSuspend

        val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
        if (nonTerminal > 0) return@inTransactionSuspend

        signalQueue = evaluateAndAdvance(handle, workflowId, sequenceNumber, resultJson)
    }

    if (signalQueue != null) notifier.signal(signalQueue!!)
}
```

**3b.** Add `resultJson` parameter to `evaluateAndAdvance`:

```kotlin
private fun evaluateAndAdvance(
    handle: Handle,
    workflowId: String,
    sequenceNumber: Int,
    resultJson: String? = null,
): String? {
    val workflow =
        workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: throw IllegalStateException("Workflow not found: $workflowId")
    if (workflow.status != WorkflowStatus.RUNNING) return null
    if (sequenceNumber != workflow.currentSequence) return null

    val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
    val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)

    return resolveAndExecute(handle, workflow, sequenceNumber, failedCount, totalCount, resultJson)
}
```

**3c.** Add `resultJson` parameter to `resolveAndExecute`:

```kotlin
private fun resolveAndExecute(
    handle: Handle,
    workflow: WorkflowRun,
    sequenceNumber: Int,
    failedCount: Int,
    totalCount: Int,
    resultJson: String? = null,
): String? {
    val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
    val sequenceMap = buildSequenceMap(definition)
    val seqInfo =
        sequenceMap[sequenceNumber]
            ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

    val strategy = strategyRegistry.resolve(seqInfo.phaseType)
    val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, totalCount)
    val decision = strategy.resolve(context)

    return executeDecision(handle, workflow, seqInfo, sequenceMap, decision, resultJson)
}
```

**3d.** Collapse PARALLEL/LINEAR branch in `executeDecision`:

```kotlin
private fun executeDecision(
    handle: Handle,
    workflow: WorkflowRun,
    seqInfo: SequenceInfo,
    sequenceMap: Map<Int, SequenceInfo>,
    decision: AdvancementDecision,
    resultJson: String? = null,
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
            val tasks = when (nextSeqInfo.phaseType) {
                PhaseType.PARALLEL -> {
                    val items: List<String> = objectMapper.readValue(
                        resultJson ?: throw IllegalStateException(
                            "PARALLEL phase requires scatter result but none provided for workflow ${workflow.id}"
                        )
                    )
                    require(items.isNotEmpty()) {
                        "Fan-out produced 0 items for workflow ${workflow.id}. " +
                            "Scatter handler must return a non-empty JSON array."
                    }
                    items.map {
                        createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now, item = it)
                    }
                }

                PhaseType.LINEAR -> {
                    listOf(createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now))
                }
            }
            taskRepo.insertBatchWithHandle(handle, tasks)
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

- [ ] **Step 4: Run the new tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest$AtomicScatterFanOut" -pl WorkFlow`
Expected: PASS.

- [ ] **Step 5: Run all barrier and integration tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest,WorkflowIntegrationTest,SweeperTest" -pl WorkFlow`
Expected: All tests PASS. The existing scatter tests (Test 11, 12, 13) and E2E tests continue to work because they call the same `onTaskCompleted` — now with a single transaction internally.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/engine/BarrierService.kt src/test/kotlin/engine/BarrierServiceTest.kt
git commit -m "feat: merge barrier transactions, collapse PARALLEL/LINEAR into single insertBatchWithHandle"
```

---

### Task 4: Run full test suite and coverage check

- [ ] **Step 1: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS.

- [ ] **Step 2: Generate JaCoCo report and check coverage**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test jacoco:report -pl WorkFlow`
Then: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`

Expected: Coverage meets thresholds. If any package drops below, add targeted tests.

- [ ] **Step 3: Commit if any coverage fixes were needed**

```bash
git add -A
git commit -m "test: coverage fixes for atomic scatter fan-out"
```
