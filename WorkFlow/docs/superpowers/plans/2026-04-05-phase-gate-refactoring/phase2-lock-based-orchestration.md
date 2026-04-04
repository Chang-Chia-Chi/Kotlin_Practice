# Phase 2: Lock-Based Orchestration (Replace CAS)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CAS retry mechanism in `DefaultPhaseGate` with pessimistic `SELECT ... FOR UPDATE` locking. Wire `DefaultPhaseGate` to use `DagRouter` from Phase 1. Eliminate `withCasRetry`, `requireCasWin`, `RetryableException`, and `MAX_CAS_RETRIES`.

**Architecture:** The workflow row lock serializes all DAG evaluations for a workflow. A threshold-based fast path keeps the common case (many tasks still pending) lock-free. `DefaultPhaseGate` becomes a thin transaction orchestrator that delegates routing decisions to `DagRouter`.

**Tech Stack:** Kotlin, JDBI, Oracle (`SELECT ... FOR UPDATE`), SmallRye Config

**Spec:** `docs/superpowers/specs/2026-04-05-phase-gate-refactoring-design.md` — Phase 2

**Depends on:** Phase 1 (DagRouter must exist)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/main/kotlin/workflow/config/PhaseGateConfig.kt` | Create | Config interface for `last-mile-threshold` |
| `src/main/kotlin/workflow/usecase/port/outbound/persistent/WorkflowRepository.kt` | Modify | Add `findByIdForUpdate`, remove `casVersion`, `casVersionWithHandle` |
| `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt` | Modify | Implement `findByIdForUpdate`, add `incrementVersionWithHandle`, remove CAS methods |
| `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt` | Rewrite | Replace CAS with lock, delegate to DagRouter |
| `src/main/resources/application.properties` | Modify | Add `framework.phase-gate.last-mile-threshold` |
| `src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt` | Modify | Remove version assertions tied to CAS semantics |
| `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt` | Modify | Update version assertions |

---

### Task 1: Add `PhaseGateConfig` and config property

**Files:**
- Create: `src/main/kotlin/workflow/config/PhaseGateConfig.kt`
- Modify: `src/main/resources/application.properties`

- [ ] **Step 1: Create PhaseGateConfig**

Create `src/main/kotlin/workflow/config/PhaseGateConfig.kt`:

```kotlin
package com.workflow.workflow.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault

@ConfigMapping(prefix = "framework.phase-gate")
interface PhaseGateConfig {
    @WithDefault("4")
    fun lastMileThreshold(): Int
}
```

- [ ] **Step 2: Add config to application.properties**

Append to `src/main/resources/application.properties`:

```properties
framework.phase-gate.last-mile-threshold=4
```

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/config/PhaseGateConfig.kt src/main/resources/application.properties
git commit -m "feat(workflow): add PhaseGateConfig with last-mile-threshold"
```

---

### Task 2: Add `findByIdForUpdate` and `incrementVersionWithHandle` to WorkflowRepository

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/port/outbound/persistent/WorkflowRepository.kt`
- Modify: `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt`

- [ ] **Step 1: Add interface methods**

In `src/main/kotlin/workflow/usecase/port/outbound/persistent/WorkflowRepository.kt`, add two new methods and remove CAS methods:

```kotlin
package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import org.jdbi.v3.core.Handle
import java.time.Duration

interface WorkflowRepository {
    suspend fun insert(run: WorkflowRun)
    suspend fun findById(id: String): WorkflowRun?
    suspend fun updateStatus(id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    suspend fun findStuck(gracePeriod: Duration): List<WorkflowRun>

    fun insertWithHandle(handle: Handle, run: WorkflowRun)
    fun findByIdWithHandle(handle: Handle, id: String): WorkflowRun?
    fun findByIdForUpdate(handle: Handle, id: String): WorkflowRun?
    fun incrementVersionWithHandle(handle: Handle, id: String)
    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: WorkflowStatus, expectedStatus: WorkflowStatus): Boolean
    fun mergeIdempotentWithHandle(handle: Handle, run: WorkflowRun, idempotencyKey: String): Pair<String, Boolean>
    fun expireOverdueWithHandle(handle: Handle, now: java.time.LocalDateTime): Int
}
```

- [ ] **Step 2: Implement in JdbiWorkflowRepository**

In `src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt`:

Remove `casVersion` suspend method (L30-33) and `casVersionWithHandle` (L100-113).

Add `findByIdForUpdate` after `findByIdWithHandle`:

```kotlin
    override fun findByIdForUpdate(handle: Handle, id: String): WorkflowRun? =
        handle.createQuery("SELECT * FROM workflow WHERE id = :id FOR UPDATE")
            .bind("id", id)
            .mapToMap()
            .findOne()
            .map(::mapWorkflowRow)
            .orElse(null)
```

Add `incrementVersionWithHandle` after `findByIdForUpdate`:

```kotlin
    override fun incrementVersionWithHandle(handle: Handle, id: String) {
        handle.createUpdate(
            "UPDATE workflow SET version = version + 1, updated_at = :now WHERE id = :id",
        )
            .bind("id", id)
            .bind("now", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS))
            .execute()
    }
```

- [ ] **Step 3: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl .`
Expected: Compilation failure — `DefaultPhaseGate` still references `casVersionWithHandle`. This is expected; Task 3 fixes it.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/workflow/usecase/port/outbound/persistent/WorkflowRepository.kt src/main/kotlin/workflow/adapter/persistent/JdbiWorkflowRepository.kt
git commit -m "refactor(workflow): add findByIdForUpdate, remove CAS from WorkflowRepository"
```

---

### Task 3: Rewrite DefaultPhaseGate to use lock + DagRouter

**Files:**
- Rewrite: `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`

- [ ] **Step 1: Rewrite DefaultPhaseGate**

Replace the entire content of `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`:

```kotlin
package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.workflow.config.PhaseGateConfig
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.createSkippedTaskForActivity
import com.workflow.workflow.model.createTaskForActivity
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * DAG-aware phase gate that evaluates successor activities after each task completion.
 *
 * Uses pessimistic locking (SELECT ... FOR UPDATE on the workflow row) to serialize
 * DAG evaluations. A threshold-based fast path keeps the common case lock-free:
 * only tasks completing near the sequence boundary acquire the lock.
 *
 * All routing decisions are delegated to [DagRouter] (pure functions).
 */
@ApplicationScoped
class DefaultPhaseGate(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: WorkerNotifier,
    private val phaseGateConfig: PhaseGateConfig,
) : PhaseGate {

    private val log = LoggerFactory.getLogger(DefaultPhaseGate::class.java)

    override suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String?,
        claimedAt: Instant?,
    ) {
        val signalQueues = jdbi.inTransactionSuspend<List<String>, Exception> { handle ->
            // Step 1: Fenced task update
            val updated = taskRepo.updateStatusWithHandle(
                handle, taskId, status, resultJson, claimedBy, claimedAt,
            )
            if (!updated) return@inTransactionSuspend emptyList()

            // Step 2: Cheap barrier probe (no lock)
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > phaseGateConfig.lastMileThreshold()) return@inTransactionSuspend emptyList()

            // Step 3: Acquire workflow lock and recount
            val workflow = workflowRepo.findByIdForUpdate(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend emptyList()

            val confirmedNonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (confirmedNonTerminal > 0) return@inTransactionSuspend emptyList()

            // Step 4: Build snapshot and route
            val snapshot = buildSnapshot(handle, workflowId, workflow.definitionJson)
            val seqInfo = snapshot.sequenceMap[sequenceNumber]
                ?: throw IllegalStateException("Seq $sequenceNumber not in definition for $workflowId")

            val scatterItems = if (seqInfo.phaseType == PhaseType.SCATTER && status == TaskStatus.COMPLETED) {
                objectMapper.readValue<List<String>>(
                    resultJson ?: throw IllegalStateException(
                        "SCATTER phase requires scatter result for workflow $workflowId",
                    ),
                )
            } else null

            val decision = resolvePhaseDecision(snapshot, seqInfo, status, scatterItems)

            when (decision) {
                PhaseDecision.Abort -> {
                    abortWorkflow(handle, workflowId)
                    return@inTransactionSuspend emptyList()
                }
                is PhaseDecision.ScatterExpand -> {
                    val parallelTasks = decision.items.map {
                        createTaskForActivity(
                            workflowId, decision.parallelInfo.activityName,
                            decision.parallelInfo.sequenceNumber,
                            decision.parallelInfo.activity, snapshot.now, item = it,
                        )
                    }
                    taskRepo.insertBatchWithHandle(handle, parallelTasks)
                    workflowRepo.incrementVersionWithHandle(handle, workflowId)
                    return@inTransactionSuspend listOf(decision.parallelInfo.activity.queue)
                }
                PhaseDecision.ForceDefaultBranch,
                PhaseDecision.Normal -> { /* fall through to successor evaluation */ }
            }

            // Step 5: Dispatch successors
            val forceDefault = decision == PhaseDecision.ForceDefaultBranch
            val result = dispatchSuccessors(snapshot, seqInfo, forceDefault)

            if (result.tasksToInsert.isNotEmpty()) {
                insertMixedTaskBatch(handle, result.tasksToInsert)
            }

            // Step 6: Check global completion
            val checkCompletion = result.hasTerminalCompletion || seqInfo.activity.isTerminal
            if (checkCompletion) {
                val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
                if (globalNonTerminal == 0) {
                    workflowRepo.updateStatusWithHandle(
                        handle, workflowId, WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING,
                    )
                    return@inTransactionSuspend emptyList()
                }
            }

            workflowRepo.incrementVersionWithHandle(handle, workflowId)
            result.signalQueues.toList()
        }

        signalQueues.forEach { notifier.signal(it) }
    }

    override suspend fun recoverStuckWorkflow(workflowId: String) {
        val signalQueues = jdbi.inTransactionSuspend<List<String>, Exception> { handle ->
            val workflow = workflowRepo.findByIdForUpdate(handle, workflowId)
                ?: run {
                    log.warn("Workflow not found during recovery: {}", workflowId)
                    return@inTransactionSuspend emptyList()
                }
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend emptyList()

            val snapshot = buildSnapshot(handle, workflowId, workflow.definitionJson)
            val signalQueueSet = mutableSetOf<String>()

            for ((seq, seqInfo) in snapshot.sequenceMap.entries.sortedBy { it.key }) {
                if ((snapshot.allCounts[seq]?.total ?: 0) > 0) continue

                val allPredTerminal = seqInfo.predecessorSequences.isEmpty() ||
                    seqInfo.predecessorSequences.all { predSeq ->
                        (snapshot.allCounts[predSeq]?.total ?: 0) > 0 &&
                            (snapshot.allCounts[predSeq]?.nonTerminal ?: 0) == 0
                    }
                if (!allPredTerminal) continue

                when (seqInfo.phaseType) {
                    PhaseType.SCATTER -> {
                        val task = createTaskForActivity(
                            workflowId, seqInfo.activityName, seq, seqInfo.activity, snapshot.now,
                        )
                        taskRepo.insertBatchWithHandle(handle, listOf(task))
                        signalQueueSet += seqInfo.activity.queue
                    }
                    PhaseType.PARALLEL -> continue
                    PhaseType.LINEAR -> {
                        val edgeTaken = isAnyEdgeTaken(
                            snapshot.tasksBySeq, snapshot.resultBranches, seqInfo, snapshot.sequenceMap, snapshot.definition,
                        )
                        if (edgeTaken || seqInfo.predecessorSequences.isEmpty()) {
                            val task = createTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, snapshot.now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(task))
                            signalQueueSet += seqInfo.activity.queue
                        } else {
                            val skipped = createSkippedTaskForActivity(
                                workflowId, seqInfo.activityName, seq, seqInfo.activity, snapshot.now,
                            )
                            taskRepo.insertBatchWithHandle(handle, listOf(skipped))
                        }
                    }
                }
            }

            val globalNonTerminal = taskRepo.countAllNonTerminalWithHandle(handle, workflowId)
            if (globalNonTerminal == 0) {
                val abortFailure = snapshot.sequenceMap.entries.any { (seq, seqInfo) ->
                    seqInfo.phaseType != PhaseType.PARALLEL &&
                        seqInfo.activity.failurePolicy == FailurePolicy.ABORT &&
                        (snapshot.allCounts[seq]?.failed ?: 0) > 0
                }
                val terminalStatus = if (abortFailure) WorkflowStatus.FAILED else WorkflowStatus.COMPLETED
                workflowRepo.updateStatusWithHandle(
                    handle, workflowId, terminalStatus, WorkflowStatus.RUNNING,
                )
                return@inTransactionSuspend emptyList()
            }

            if (signalQueueSet.isEmpty()) return@inTransactionSuspend emptyList()

            workflowRepo.incrementVersionWithHandle(handle, workflowId)
            signalQueueSet.toList()
        }

        signalQueues.forEach { notifier.signal(it) }
    }

    // -- Snapshot builder ---------------------------------------------------------

    private fun buildSnapshot(
        handle: Handle,
        workflowId: String,
        definitionJson: String,
    ): GateSnapshot {
        val definition = objectMapper.readValue<WorkflowDefinition>(definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqByName = sequenceMap.values
            .filter { it.phaseType != PhaseType.PARALLEL }
            .associateBy { it.activityName }
        val allCounts = taskRepo.countStatusSummariesByWorkflowWithHandle(handle, workflowId)
        val allTasks = taskRepo.findByWorkflowIdWithHandle(handle, workflowId)
        val tasksBySeq = allTasks.groupBy { it.sequenceNumber }
        val resultBranches = allTasks.associate { task ->
            task.id to task.resultJson?.let { json ->
                try {
                    objectMapper.readValue<Map<String, Any>>(json)["branch"]?.toString()
                } catch (_: Exception) {
                    null
                }
            }
        }
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

        return GateSnapshot(
            workflowId, definition, sequenceMap, seqByName,
            allCounts, tasksBySeq, resultBranches, now,
        )
    }

    // -- Transaction helpers ------------------------------------------------------

    private fun abortWorkflow(handle: Handle, workflowId: String) {
        val statusUpdated = workflowRepo.updateStatusWithHandle(
            handle, workflowId, WorkflowStatus.FAILED, WorkflowStatus.RUNNING,
        )
        if (statusUpdated) taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
    }

    private fun insertMixedTaskBatch(handle: Handle, tasks: List<com.workflow.workflow.model.Task>) {
        val (skipped, pending) = tasks.partition { it.completedAt != null }
        if (pending.isNotEmpty()) taskRepo.insertBatchWithHandle(handle, pending)
        if (skipped.isNotEmpty()) taskRepo.insertBatchWithHandle(handle, skipped)
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl .`
Expected: SUCCESS. All CAS references removed, DagRouter functions used.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt
git commit -m "refactor(workflow): replace CAS retry with lock-based orchestration in DefaultPhaseGate"
```

---

### Task 4: Update tests for lock-based DefaultPhaseGate

**Files:**
- Modify: `src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt`
- Modify: `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt`
- Modify: `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`

- [ ] **Step 1: Fix DefaultPhaseGateTest constructor**

`DefaultPhaseGate` now takes `PhaseGateConfig` as a constructor parameter. In `DefaultPhaseGateTest.kt`, update the `setup()` method:

Replace the `gate = DefaultPhaseGate(...)` line in `@BeforeAll fun setup()`:

```kotlin
    private val testPhaseGateConfig = object : PhaseGateConfig {
        override fun lastMileThreshold(): Int = 4
    }

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        notifier = FakeWorkerNotifier()
        gate = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier, testPhaseGateConfig)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
    }
```

- [ ] **Step 2: Fix WorkflowIntegrationTest constructor**

In `WorkflowIntegrationTest.kt`, update `@BeforeAll fun setup()` — same pattern:

```kotlin
    private val testPhaseGateConfig = object : PhaseGateConfig {
        override fun lastMileThreshold(): Int = 4
    }

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
        barrier = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier, testPhaseGateConfig)
        watchdog = WorkflowWatchdog(jdbi, workflowRepo, taskRepo, barrier, testWatchdogConfig)
    }
```

- [ ] **Step 3: Update version assertions in WorkflowIntegrationTest**

The version field is now an audit counter incremented unconditionally. Some test assertions check exact version numbers. Update tests that assert specific version values:

In `WorkerDeathSimulation` test (`worker death after CAS before task insert sweeper re-dispatches`, around L912):
- The test simulates CAS-specific behavior (incrementing version manually then checking watchdog idempotency). The test still works because `recoverStuckWorkflow` now uses `FOR UPDATE` lock + unconditional increment. The version assertions may need adjustment — verify they still match the new increment pattern.

In `LinearWorkflowE2E` test (around L174, L189):
- `assertEquals(1, (wf["VERSION"] as Number).toInt())` — version is still incremented, this should still pass.

- [ ] **Step 4: Remove CAS-specific repository tests**

In `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`, remove the `casVersion` test block (around L279-347). These tests exercise `casVersion` and `casVersionWithHandle` which no longer exist.

- [ ] **Step 5: Fix any other test files that construct DefaultPhaseGate**

Search for other construction sites:

```bash
grep -rn "DefaultPhaseGate(" src/test/kotlin/
```

Update each to include `testPhaseGateConfig`. Common locations:
- `src/test/kotlin/stress/StressTestBase.kt`
- `src/test/kotlin/benchmark/InstrumentedComponents.kt`

- [ ] **Step 6: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt
git add -u  # catch any other modified test files
git commit -m "test(workflow): update tests for lock-based DefaultPhaseGate"
```
