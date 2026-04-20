# Phase 1: Extract DagRouter (Pure Domain Logic)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract all pure DAG routing logic from `DefaultPhaseGate` into a standalone `DagRouter` with zero framework dependencies, fully covered by unit tests.

**Architecture:** `DagRouter` is a collection of top-level pure functions operating on an immutable `GateSnapshot`. No CDI, no JDBI, no Jackson. The caller (`DefaultPhaseGate`) builds the snapshot from DB state and calls `DagRouter`. This task is additive only — `DefaultPhaseGate` is not modified.

**Tech Stack:** Kotlin (pure functions, data classes), JUnit 5, kotlinx-coroutines-test

**Spec:** `docs/superpowers/specs/2026-04-05-phase-gate-refactoring-design.md` — Phase 1

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt` | Create | Pure DAG routing: phase decisions, successor dispatch (Kahn's BFS), edge evaluation, join policy |
| `src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt` | Create | Pure unit tests — construct snapshots directly, no DB, no mocks |

No existing files are modified. This is purely additive.

---

### Task 1: Create DagRouter with data types and `evaluateJoinPolicy`

**Files:**
- Create: `src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt`
- Create: `src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt`

- [ ] **Step 1: Write failing tests for `evaluateJoinPolicy`**

Create `src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt`:

```kotlin
package com.workflow.workflow.usecase.service.orchestration

import com.workflow.workflow.model.JoinPolicy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DagRouterTest {

    @Nested
    inner class EvaluateJoinPolicyTest {

        @Test
        fun `All policy passes when completed equals total`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.All, completedCount = 5, totalCount = 5))
        }

        @Test
        fun `All policy fails when completed less than total`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.All, completedCount = 4, totalCount = 5))
        }

        @Test
        fun `Threshold policy passes when completed meets threshold`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.Threshold(3), completedCount = 3, totalCount = 5))
        }

        @Test
        fun `Threshold policy fails when completed below threshold`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.Threshold(3), completedCount = 2, totalCount = 5))
        }

        @Test
        fun `Percentage policy passes at exact boundary`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.Percentage(80), completedCount = 4, totalCount = 5))
        }

        @Test
        fun `Percentage policy fails below boundary`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.Percentage(80), completedCount = 3, totalCount = 5))
        }

        @Test
        fun `Percentage policy handles zero total`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.Percentage(50), completedCount = 0, totalCount = 0))
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: Compilation failure — `evaluateJoinPolicy` not defined.

- [ ] **Step 3: Create DagRouter with data types and `evaluateJoinPolicy`**

Create `src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt`:

```kotlin
package com.workflow.workflow.usecase.service.orchestration

import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.createSkippedTaskForActivity
import com.workflow.workflow.model.createTaskForActivity
import java.time.Instant

/**
 * Immutable snapshot of workflow state at transaction-read time.
 * Built by the transaction orchestrator, consumed by pure routing functions.
 */
data class GateSnapshot(
    val workflowId: String,
    val definition: WorkflowDefinition,
    val sequenceMap: Map<Int, SequenceInfo>,
    val seqByName: Map<String, SequenceInfo>,
    val allCounts: Map<Int, TaskStatusCounts>,
    val tasksBySeq: Map<Int, List<Task>>,
    /** Pre-extracted branch labels from task results. Key = task ID, value = branch label or null. */
    val resultBranches: Map<String, String?>,
    val now: Instant,
)

sealed interface PhaseDecision {
    data object Abort : PhaseDecision
    data class ScatterExpand(val items: List<String>, val parallelInfo: SequenceInfo) : PhaseDecision
    data object ForceDefaultBranch : PhaseDecision
    data object Normal : PhaseDecision
}

data class SuccessorResult(
    val tasksToInsert: List<Task>,
    val signalQueues: Set<String>,
    val hasTerminalCompletion: Boolean,
)

// -- Join policy --------------------------------------------------------------

fun evaluateJoinPolicy(joinPolicy: JoinPolicy, completedCount: Int, totalCount: Int): Boolean =
    when (joinPolicy) {
        is JoinPolicy.All -> completedCount == totalCount
        is JoinPolicy.Threshold -> completedCount >= joinPolicy.n
        is JoinPolicy.Percentage -> {
            val pct = if (totalCount > 0) (completedCount * 100) / totalCount else 0
            pct >= joinPolicy.pct
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: All 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt
git commit -m "refactor(workflow): add DagRouter skeleton with evaluateJoinPolicy"
```

---

### Task 2: Add `isEdgeTaken` and `isAnyEdgeTaken`

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt`
- Modify: `src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt`

- [ ] **Step 1: Write failing tests for edge evaluation**

Add to `DagRouterTest.kt`:

```kotlin
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import java.time.Duration
import java.time.Instant

// Inside DagRouterTest class:

    // -- Test helpers ----------------------------------------------------------

    private val now = Instant.parse("2026-01-01T00:00:00Z")

    private fun activity(
        name: String,
        transition: String = "$name.handler",
        successors: List<Edge> = emptyList(),
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    ) = ActivityDefinition(
        name = name,
        transition = transition,
        successors = successors,
        failurePolicy = failurePolicy,
    )

    private fun task(
        id: String = "t-1",
        workflowId: String = "wf-1",
        seq: Int = 1,
        status: TaskStatus = TaskStatus.COMPLETED,
        handlerKey: String = "h",
        resultJson: String? = null,
    ) = Task(
        id = id, workflowId = workflowId, activityName = "", sequenceNumber = seq,
        status = status, handlerKey = handlerKey, resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = if (status.isTerminal) now else null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    @Nested
    inner class IsEdgeTakenTest {

        @Test
        fun `COMPLETED task with DEFAULT_BRANCH edge is taken`() {
            assertTrue(isEdgeTaken(TaskStatus.COMPLETED, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `COMPLETED task with matching branch label is taken`() {
            assertTrue(isEdgeTaken(TaskStatus.COMPLETED, "OK", "OK", FailurePolicy.ABORT))
        }

        @Test
        fun `COMPLETED task with non-matching branch label is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.COMPLETED, "OK", "FAIL", FailurePolicy.ABORT))
        }

        @Test
        fun `FAILED task with BEST_EFFORT takes DEFAULT_BRANCH`() {
            assertTrue(isEdgeTaken(TaskStatus.FAILED, null, DEFAULT_BRANCH, FailurePolicy.BEST_EFFORT))
        }

        @Test
        fun `FAILED task with BEST_EFFORT does not take conditional edge`() {
            assertFalse(isEdgeTaken(TaskStatus.FAILED, null, "OK", FailurePolicy.BEST_EFFORT))
        }

        @Test
        fun `FAILED task with ABORT is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.FAILED, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `non-terminal task is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.PROCESSING, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `COMPLETED task with null branch and conditional edge is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.COMPLETED, null, "SOME_LABEL", FailurePolicy.ABORT))
        }
    }

    @Nested
    inner class IsAnyEdgeTakenTest {

        @Test
        fun `returns true when predecessor completed with default edge`() {
            val predActivity = activity("pred", successors = listOf(Edge("succ")))
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf("pred" to predActivity, "succ" to succActivity),
                start = "pred",
            )
            val sequenceMap = mapOf(
                1 to SequenceInfo(1, "pred", predActivity, PhaseType.LINEAR, emptyList()),
                2 to SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1)),
            )
            val predTask = task(id = "t-pred", seq = 1, status = TaskStatus.COMPLETED)
            val tasksBySeq = mapOf(1 to listOf(predTask))
            val resultBranches = mapOf("t-pred" to null as String?)

            assertTrue(isAnyEdgeTaken(tasksBySeq, resultBranches, sequenceMap.getValue(2), sequenceMap, definition))
        }

        @Test
        fun `returns false when predecessor not completed`() {
            val predActivity = activity("pred", successors = listOf(Edge("succ")))
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf("pred" to predActivity, "succ" to succActivity),
                start = "pred",
            )
            val sequenceMap = mapOf(
                1 to SequenceInfo(1, "pred", predActivity, PhaseType.LINEAR, emptyList()),
                2 to SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1)),
            )
            val predTask = task(id = "t-pred", seq = 1, status = TaskStatus.PROCESSING)
            val tasksBySeq = mapOf(1 to listOf(predTask))
            val resultBranches = emptyMap<String, String?>()

            assertFalse(isAnyEdgeTaken(tasksBySeq, resultBranches, sequenceMap.getValue(2), sequenceMap, definition))
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: Compilation failure — `isEdgeTaken`, `isAnyEdgeTaken` not defined.

- [ ] **Step 3: Implement `isEdgeTaken`, `hasDefaultBranchEdge`, `successorsOf`, `isAnyEdgeTaken`**

Append to `DagRouter.kt`:

```kotlin
// -- Edge evaluation ----------------------------------------------------------

/**
 * Determines if a single edge is "taken" given a predecessor task's terminal state.
 * Pure — uses pre-extracted [resultBranch] instead of parsing JSON.
 */
fun isEdgeTaken(
    taskStatus: TaskStatus,
    resultBranch: String?,
    edgeLabel: String,
    predFailurePolicy: FailurePolicy,
): Boolean {
    if (!taskStatus.isTerminal) return false
    if (taskStatus == TaskStatus.FAILED && predFailurePolicy == FailurePolicy.BEST_EFFORT) {
        return edgeLabel == DEFAULT_BRANCH
    }
    if (taskStatus != TaskStatus.COMPLETED) return false
    if (edgeLabel == DEFAULT_BRANCH) return true
    return resultBranch == edgeLabel
}

/**
 * Checks whether any predecessor edge to [successor] is "taken" based on
 * predecessor task results and failure policies.
 */
fun isAnyEdgeTaken(
    tasksBySeq: Map<Int, List<Task>>,
    resultBranches: Map<String, String?>,
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
            val branch = resultBranches[predTask.id]
            for (edge in edgesToTarget) {
                if (isEdgeTaken(predTask.status, branch, edge.label, predActivity.failurePolicy)) return true
            }
        }
    }
    return false
}

/**
 * Checks whether any predecessor has a DEFAULT_BRANCH edge to the given [successor].
 */
fun hasDefaultBranchEdge(
    successor: SequenceInfo,
    definition: WorkflowDefinition,
): Boolean {
    val targetActName = successor.activityName
    for ((_, predActivity) in definition.activities) {
        if (predActivity.successors.any { it.target == targetActName && it.label == DEFAULT_BRANCH }) {
            return true
        }
    }
    return false
}

/**
 * Returns the [SequenceInfo] entries for all successor activities of the given sequence.
 */
fun successorsOf(
    seqInfo: SequenceInfo,
    seqByName: Map<String, SequenceInfo>,
    definition: WorkflowDefinition,
): List<SequenceInfo> {
    val actName = seqInfo.activityName.removeSuffix(".__parallel__")
    val activity = definition.activities[actName] ?: return emptyList()
    return activity.successors.mapNotNull { edge ->
        seqByName[edge.target]
    }.distinctBy { it.sequenceNumber }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt
git commit -m "refactor(workflow): add edge evaluation functions to DagRouter"
```

---

### Task 3: Add `resolvePhaseDecision`

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt`
- Modify: `src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt`

- [ ] **Step 1: Write failing tests for phase decision**

Add to `DagRouterTest.kt`:

```kotlin
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.TaskStatusCounts
import com.workflow.workflow.model.buildSequenceMap

// Inside DagRouterTest class:

    private fun emptySnapshot(
        workflowId: String = "wf-1",
        definition: WorkflowDefinition,
        allCounts: Map<Int, TaskStatusCounts> = emptyMap(),
        tasksBySeq: Map<Int, List<Task>> = emptyMap(),
        resultBranches: Map<String, String?> = emptyMap(),
    ): GateSnapshot {
        val sequenceMap = buildSequenceMap(definition)
        val seqByName = sequenceMap.values
            .filter { it.phaseType != PhaseType.PARALLEL }
            .associateBy { it.activityName }
        return GateSnapshot(
            workflowId = workflowId,
            definition = definition,
            sequenceMap = sequenceMap,
            seqByName = seqByName,
            allCounts = allCounts,
            tasksBySeq = tasksBySeq,
            resultBranches = resultBranches,
            now = now,
        )
    }

    @Nested
    inner class ResolvePhaseDecisionTest {

        @Test
        fun `LINEAR COMPLETED returns Normal`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a")),
                start = "a",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.getValue(1)
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.Normal, decision)
        }

        @Test
        fun `LINEAR FAILED with ABORT returns Abort`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a", failurePolicy = FailurePolicy.ABORT)),
                start = "a",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.getValue(1)
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.FAILED, scatterItems = null)
            assertEquals(PhaseDecision.Abort, decision)
        }

        @Test
        fun `SCATTER COMPLETED returns ScatterExpand`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h"))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.SCATTER }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = listOf("a", "b"))
            assertTrue(decision is PhaseDecision.ScatterExpand)
            assertEquals(listOf("a", "b"), (decision as PhaseDecision.ScatterExpand).items)
        }

        @Test
        fun `PARALLEL join passed returns Normal`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h", joinPolicy = JoinPolicy.All))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(2 to TaskStatusCounts(total = 3, completed = 3, nonTerminal = 0, failed = 0)),
            )
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.Normal, decision)
        }

        @Test
        fun `PARALLEL join failed with ABORT returns Abort`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h", joinPolicy = JoinPolicy.All, failurePolicy = FailurePolicy.ABORT))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(2 to TaskStatusCounts(total = 3, completed = 2, nonTerminal = 0, failed = 1)),
            )
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.Abort, decision)
        }

        @Test
        fun `PARALLEL join failed with BEST_EFFORT returns ForceDefaultBranch`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h", joinPolicy = JoinPolicy.All, failurePolicy = FailurePolicy.BEST_EFFORT))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(2 to TaskStatusCounts(total = 3, completed = 2, nonTerminal = 0, failed = 1)),
            )
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.ForceDefaultBranch, decision)
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: Compilation failure — `resolvePhaseDecision` not defined.

- [ ] **Step 3: Implement `resolvePhaseDecision`**

Add to `DagRouter.kt`:

```kotlin
// -- Phase decision -----------------------------------------------------------

/**
 * Resolves the phase-type-specific decision for the completing sequence.
 * Pure — scatter items are pre-deserialized by the caller.
 */
fun resolvePhaseDecision(
    snapshot: GateSnapshot,
    seqInfo: SequenceInfo,
    status: TaskStatus,
    scatterItems: List<String>?,
): PhaseDecision = when (seqInfo.phaseType) {
    PhaseType.SCATTER -> if (status == TaskStatus.COMPLETED) {
        val items = requireNotNull(scatterItems) {
            "SCATTER phase requires scatter result for workflow ${snapshot.workflowId}"
        }
        require(items.isNotEmpty()) {
            "Fan-out produced 0 items for workflow ${snapshot.workflowId}"
        }
        val parallelSeq = seqInfo.sequenceNumber + 1
        val parallelInfo = snapshot.sequenceMap[parallelSeq]!!
        PhaseDecision.ScatterExpand(items, parallelInfo)
    } else {
        resolveFailureFallback(seqInfo.activity.failurePolicy)
    }

    PhaseType.PARALLEL -> {
        val counts = snapshot.allCounts[seqInfo.sequenceNumber] ?: TaskStatusCounts(0, 0, 0, 0)
        val scatterActName = seqInfo.activityName.removeSuffix(".__parallel__")
        val scatterActivity = snapshot.definition.activities[scatterActName]
        val joinPolicy = scatterActivity?.fanOut?.joinPolicy ?: JoinPolicy.All
        val joinPassed = evaluateJoinPolicy(joinPolicy, counts.completed, counts.total)
        if (joinPassed) PhaseDecision.Normal
        else resolveFailureFallback(seqInfo.activity.failurePolicy)
    }

    PhaseType.LINEAR -> {
        if (status != TaskStatus.COMPLETED && status != TaskStatus.SKIPPED &&
            seqInfo.activity.failurePolicy == FailurePolicy.ABORT
        ) {
            PhaseDecision.Abort
        } else {
            PhaseDecision.Normal
        }
    }
}

private fun resolveFailureFallback(failurePolicy: FailurePolicy): PhaseDecision =
    if (failurePolicy == FailurePolicy.ABORT) PhaseDecision.Abort
    else PhaseDecision.ForceDefaultBranch
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt
git commit -m "refactor(workflow): add resolvePhaseDecision to DagRouter"
```

---

### Task 4: Add `dispatchSuccessors` (Kahn's BFS)

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt`
- Modify: `src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt`

- [ ] **Step 1: Write failing tests for successor dispatch**

Add to `DagRouterTest.kt`:

```kotlin
    @Nested
    inner class DispatchSuccessorsTest {

        @Test
        fun `linear chain dispatches next activity`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"))),
                    "b" to activity("b"),
                ),
                start = "a",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            assertEquals(1, result.tasksToInsert.size)
            assertEquals(TaskStatus.PENDING, result.tasksToInsert[0].status)
            assertTrue(result.signalQueues.contains("default"))
        }

        @Test
        fun `conditional routing skips unmatched branch`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b", "OK"), Edge("c", "FAIL"))),
                    "b" to activity("b"),
                    "c" to activity("c"),
                ),
                start = "a",
            )
            val predTask = task(id = "t-a", seq = 1, status = TaskStatus.COMPLETED, resultJson = """{"branch":"OK"}""")
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-a" to "OK"),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            val pending = result.tasksToInsert.filter { it.status == TaskStatus.PENDING }
            val skipped = result.tasksToInsert.filter { it.status == TaskStatus.SKIPPED }
            assertEquals(1, pending.size)
            assertEquals(1, skipped.size)
            assertEquals("b", pending[0].activityName)
            assertEquals("c", skipped[0].activityName)
        }

        @Test
        fun `diamond join waits for all predecessors`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"), Edge("c"))),
                    "b" to activity("b", successors = listOf(Edge("join"))),
                    "c" to activity("c", successors = listOf(Edge("join"))),
                    "join" to activity("join"),
                ),
                start = "a",
            )
            // Only b completed, c not yet
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(
                    1 to TaskStatusCounts(1, 1, 0, 0),
                    2 to TaskStatusCounts(1, 1, 0, 0),
                    // seq 3 (c) has no counts — not yet dispatched or still pending
                ),
                tasksBySeq = mapOf(
                    1 to listOf(task(id = "t-a", seq = 1)),
                    2 to listOf(task(id = "t-b", seq = 2)),
                ),
                resultBranches = mapOf("t-a" to null, "t-b" to null),
            )
            val seqInfo = snap.sequenceMap.getValue(2) // b completing
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            // join should NOT be dispatched (c not resolved)
            assertTrue(result.tasksToInsert.isEmpty())
        }

        @Test
        fun `cascade skip propagates through chain`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b", "OK"), Edge("x", "NO"))),
                    "b" to activity("b", successors = listOf(Edge("c"))),
                    "c" to activity("c"),
                    "x" to activity("x"),
                ),
                start = "a",
            )
            val predTask = task(id = "t-a", seq = 1, status = TaskStatus.COMPLETED, resultJson = """{"branch":"NO"}""")
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-a" to "NO"),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            val names = result.tasksToInsert.map { it.activityName to it.status }
            assertTrue(names.contains("b" to TaskStatus.SKIPPED))
            assertTrue(names.contains("c" to TaskStatus.SKIPPED))
            assertTrue(names.contains("x" to TaskStatus.PENDING))
        }

        @Test
        fun `scatter skip cascades to companion parallel node`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("done")))
                .copy(fanOut = FanOutDefinition(transition = "par.h"))
            val def = WorkflowDefinition(
                activities = mapOf(
                    "route" to activity("route", successors = listOf(Edge("scatter", "RUN"), Edge("done", "SKIP"))),
                    "scatter" to scatterAct,
                    "done" to activity("done"),
                ),
                start = "route",
            )
            val predTask = task(id = "t-route", seq = 1, status = TaskStatus.COMPLETED, resultJson = """{"branch":"SKIP"}""")
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-route" to "SKIP"),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            val skipped = result.tasksToInsert.filter { it.status == TaskStatus.SKIPPED }
            assertTrue(skipped.any { it.activityName == "scatter" })
            assertTrue(skipped.any { it.activityName == "scatter.__parallel__" })
        }

        @Test
        fun `terminal activity produces empty result`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a")),
                start = "a",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            assertTrue(result.tasksToInsert.isEmpty())
            assertTrue(result.signalQueues.isEmpty())
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: Compilation failure — `dispatchSuccessors` not defined.

- [ ] **Step 3: Implement `dispatchSuccessors`**

Add to `DagRouter.kt`:

```kotlin
// -- Successor dispatch -------------------------------------------------------

/**
 * Walks DAG successors using indegree-based topological BFS (Kahn's algorithm).
 *
 * A node is enqueued only when all its predecessors are "resolved" (terminal in
 * DB or decided as SKIPPED in this loop). PENDING nodes stop the walk.
 * SKIPPED nodes propagate: they decrement their successors' indegrees and
 * enqueue any that reach zero.
 */
fun dispatchSuccessors(
    snapshot: GateSnapshot,
    seqInfo: SequenceInfo,
    forceDefault: Boolean,
): SuccessorResult {
    val resolvedSeqs = mutableSetOf<Int>()
    for ((seq, counts) in snapshot.allCounts) {
        if (counts.total > 0 && counts.nonTerminal == 0) resolvedSeqs += seq
    }
    resolvedSeqs += seqInfo.sequenceNumber

    val pendingInserts = mutableListOf<Task>()
    val visitedSeqs = mutableSetOf<Int>()
    val signalQueues = mutableSetOf<String>()
    var hasTerminalCompletion = false

    val indegree = mutableMapOf<Int, Int>()
    val discovered = mutableMapOf<Int, SequenceInfo>()
    val evalQueue = ArrayDeque<Int>()

    fun discoverSuccessors(successors: List<SequenceInfo>) {
        for (succ in successors) {
            val sSeq = succ.sequenceNumber
            if ((snapshot.allCounts[sSeq]?.total ?: 0) > 0 || sSeq in visitedSeqs) continue
            if (sSeq in discovered) {
                val newDeg = (indegree[sSeq] ?: 0) - 1
                indegree[sSeq] = newDeg
                if (newDeg <= 0) evalQueue += sSeq
            } else {
                discovered[sSeq] = succ
                val deg = succ.predecessorSequences.count { it !in resolvedSeqs }
                indegree[sSeq] = deg
                if (deg <= 0) evalQueue += sSeq
            }
        }
    }

    discoverSuccessors(successorsOf(seqInfo, snapshot.seqByName, snapshot.definition))

    while (evalQueue.isNotEmpty()) {
        val sSeq = evalQueue.removeFirst()
        if (sSeq in visitedSeqs) continue
        val successor = discovered[sSeq] ?: continue

        val edgeTaken = if (forceDefault) {
            hasDefaultBranchEdge(successor, snapshot.definition)
        } else {
            isAnyEdgeTaken(snapshot.tasksBySeq, snapshot.resultBranches, successor, snapshot.sequenceMap, snapshot.definition)
        }

        if (edgeTaken) {
            val task = createTaskForActivity(
                snapshot.workflowId, successor.activityName, sSeq, successor.activity, snapshot.now,
            )
            pendingInserts += task
            visitedSeqs += sSeq
            signalQueues += successor.activity.queue
        } else {
            val skipped = createSkippedTaskForActivity(
                snapshot.workflowId, successor.activityName, sSeq, successor.activity, snapshot.now,
            )
            pendingInserts += skipped
            visitedSeqs += sSeq
            resolvedSeqs += sSeq

            if (successor.phaseType == PhaseType.SCATTER) {
                val parallelSeq = sSeq + 1
                val parallelInfo = snapshot.sequenceMap[parallelSeq]
                if (parallelInfo != null && parallelInfo.phaseType == PhaseType.PARALLEL) {
                    val parallelSkipped = createSkippedTaskForActivity(
                        snapshot.workflowId, parallelInfo.activityName, parallelSeq,
                        parallelInfo.activity, snapshot.now,
                    )
                    pendingInserts += parallelSkipped
                    visitedSeqs += parallelSeq
                    resolvedSeqs += parallelSeq
                }
            }

            if (successor.activity.isTerminal) {
                hasTerminalCompletion = true
            } else {
                discoverSuccessors(successorsOf(successor, snapshot.seqByName, snapshot.definition))
            }
        }
    }

    return SuccessorResult(pendingInserts, signalQueues, hasTerminalCompletion)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DagRouterTest" -pl .`
Expected: All tests PASS.

- [ ] **Step 5: Run full test suite to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All existing tests PASS (DagRouter is additive, no production code modified).

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DagRouter.kt src/test/kotlin/workflow/usecase/service/orchestration/DagRouterTest.kt
git commit -m "refactor(workflow): add dispatchSuccessors (Kahn's BFS) to DagRouter"
```
