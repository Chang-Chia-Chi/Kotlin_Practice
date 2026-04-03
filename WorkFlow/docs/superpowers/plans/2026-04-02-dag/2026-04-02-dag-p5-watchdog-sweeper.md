# DAG Refactor — P5: Watchdog & Sweeper Update

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `recoverStuckWorkflow()` in `DefaultPhaseGate` with the DAG-aware stuck detection algorithm. Replace the `findStuck()` placeholder in `JdbiWorkflowRepository` with a query that detects activities whose predecessors are all terminal but have no task row yet. Update `WorkflowWatchdog` and `WorkflowWatchdogTest`.

**Architecture:** Stuck detection shifts from "no tasks at `current_sequence`" to "an activity whose all predecessors are terminal but has no task row at its sequence number." The check is DB-driven for efficiency. `recoverStuckWorkflow()` delegates to the same successor evaluation logic as `onTaskCompleted()`.

**Tech Stack:** Kotlin coroutines, JDBI 3, Oracle Free (Testcontainers), JUnit 5

---

### Task 1: Implement `recoverStuckWorkflow()` in `DefaultPhaseGate`

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`

- [ ] **Step 1: Replace the stub `recoverStuckWorkflow()` with real implementation**

The stuck recovery re-evaluates every activity that has all predecessors terminal but no task row yet. This is identical to the successor evaluation in `onTaskCompleted()` step 4, but driven from the full sequence map rather than a completing task.

Replace the `recoverStuckWorkflow()` stub:

```kotlin
override suspend fun recoverStuckWorkflow(workflowId: String) {
    var signalQueues: List<String> = emptyList()

    var attempts = 0
    while (true) {
        try {
            jdbi.inTransactionSuspend<Unit, Exception> { handle ->
                val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                    ?: run {
                        log.warn("Workflow not found during recovery: {}", workflowId)
                        return@inTransactionSuspend
                    }
                if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

                val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
                val sequenceMap = buildSequenceMap(definition)
                val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
                val signalQueueSet = mutableSetOf<String>()

                for ((seq, seqInfo) in sequenceMap) {
                    // Skip if a task already exists for this sequence (guard: already dispatched or skipped)
                    if (taskRepo.countTotalWithHandle(handle, workflowId, seq) > 0) continue

                    // Check if all predecessors are terminal
                    val allPredTerminal = seqInfo.predecessorSequences.isEmpty() ||
                        seqInfo.predecessorSequences.all { predSeq ->
                            taskRepo.countNonTerminalWithHandle(handle, workflowId, predSeq) == 0 &&
                            taskRepo.countTotalWithHandle(handle, workflowId, predSeq) > 0
                        }
                    if (!allPredTerminal) continue

                    // SCATTER: dispatch if no task exists yet for this scatter seq
                    // (We can't re-create parallel tasks without the scatter result, so skip SCATTER recovery here —
                    //  the main onTaskCompleted SCATTER path handles this once the SCATTER task result is available)
                    if (seqInfo.phaseType == PhaseType.SCATTER) {
                        // A stuck SCATTER means the scatter task itself was lost — re-dispatch it
                        val task = createTaskForActivity(workflowId, seqInfo.activityName, seq, seqInfo.activity, now)
                        taskRepo.insertBatchWithHandle(handle, listOf(task))
                        signalQueueSet += seqInfo.activity.queue
                        continue
                    }

                    // PARALLEL: if scatter seq is terminal but parallel seq has no tasks → re-dispatch scatter
                    // (can't recover without scatter result; skip)
                    if (seqInfo.phaseType == PhaseType.PARALLEL) continue

                    // LINEAR: evaluate fate using same logic as onTaskCompleted
                    val edgeTaken = isAnyEdgeTaken(handle, workflowId, seqInfo, sequenceMap, definition)
                    if (edgeTaken || seqInfo.predecessorSequences.isEmpty()) {
                        val task = createTaskForActivity(workflowId, seqInfo.activityName, seq, seqInfo.activity, now)
                        taskRepo.insertBatchWithHandle(handle, listOf(task))
                        signalQueueSet += seqInfo.activity.queue
                    }
                }

                if (signalQueueSet.isEmpty()) return@inTransactionSuspend

                val casWon = workflowRepo.casVersionWithHandle(handle, workflowId, workflow.version)
                if (!casWon) throw RetryableException("CAS loss in recovery")

                signalQueues = signalQueueSet.toList()
            }
            break
        } catch (e: RetryableException) {
            if (++attempts >= 10) throw IllegalStateException("CAS retry exhausted in recovery for $workflowId", e)
            log.debug("CAS retry {} in recoverStuckWorkflow for {}", attempts, workflowId)
        }
    }

    signalQueues.forEach { notifier.signal(it) }
}
```

---

### Task 2: Implement `findStuck()` in `JdbiWorkflowRepository`

The new stuck detection: a workflow is "stuck" if it's RUNNING, past the grace period since last update, and has at least one activity where all predecessors are terminal but no task row exists for that sequence.

**Files:**
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt`

- [ ] **Step 1: Replace the `findStuck()` placeholder**

```kotlin
override suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun> =
    jdbi.withHandleSuspend<List<WorkflowRun>, Exception> { h: Handle ->
        // A workflow is stuck if it is RUNNING, past grace period, and has zero non-terminal tasks
        // (i.e., all tasks that exist are terminal, meaning the engine crashed before dispatching next activity)
        val cutoff = LocalDateTime.ofInstant(Instant.now().minus(gracePeriod), ZoneOffset.UTC)
        h.createQuery(
            """
            SELECT w.* FROM workflow w
            WHERE w.status = 'RUNNING'
              AND w.updated_at < :cutoff
              AND NOT EXISTS (
                SELECT 1 FROM task t
                WHERE t.workflow_id = w.id
                  AND t.status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
              )
            """,
        )
            .bind("cutoff", cutoff)
            .mapToMap()
            .list()
            .map(::mapWorkflowRow)
    }
```

Note: This is a conservative query — it returns workflows with no active tasks. The full per-activity check happens in `recoverStuckWorkflow()` itself (which uses `buildSequenceMap`). This two-phase approach avoids the complexity of joining across the definition JSON in SQL.

---

### Task 3: Update `WorkflowWatchdog` logging

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt`

- [ ] **Step 1: Remove `currentSequence` from watchdog log statement**

In `recoverStuckWorkflows()`, the log currently references `workflow.currentSequence`. Replace with just the workflow ID:

```kotlin
private suspend fun recoverStuckWorkflows() {
    val gracePeriod = watchdogConfig.gracePeriod()
    val stuck = workflowRepo.findStuck(gracePeriod)
    for (workflow in stuck) {
        try {
            log.warn(
                "Recovering stuck workflow {} (last updated {})",
                workflow.id, workflow.updatedAt,
            )
            phaseGate.recoverStuckWorkflow(workflow.id)
        } catch (e: Exception) {
            log.error("Failed to recover stuck workflow {}", workflow.id, e)
        }
    }
}
```

---

### Task 4: Update `WorkflowWatchdogTest`

**Files:**
- Modify: `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt`

- [ ] **Step 1: Read the existing test to understand what changes**

Read `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt` to identify any `currentSequence` or old workflow DSL usage.

- [ ] **Step 2: Update workflow definitions to new DSL**

Any `WorkflowDefinition(activities = listOf(...))` calls must be replaced with the new `workflow { ... }` DSL. Any `WorkflowRun(..., currentSequence = N, ...)` must be updated to remove `currentSequence`.

- [ ] **Step 3: Update stuck workflow test**

The stuck detection test likely creates a workflow in a state where no active tasks exist and `updated_at` is in the past. Verify the test still works with the new `findStuck()` query. The key assertion is that `recoverStuckWorkflow()` gets called and signals the appropriate queue.

- [ ] **Step 4: Run watchdog tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowWatchdogTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt
git add src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt
git add src/main/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdog.kt
git add src/test/kotlin/workflow/usecase/service/orchestration/WorkflowWatchdogTest.kt
git commit -m "feat: implement DAG-aware recoverStuckWorkflow and findStuck query"
```
