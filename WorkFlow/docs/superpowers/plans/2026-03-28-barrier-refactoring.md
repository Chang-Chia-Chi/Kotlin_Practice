# Barrier Service Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure BarrierService from a monolithic coordinator with hardcoded `when (phaseType)` switches and `seq + 1` arithmetic into a strategy-based architecture where each phase type owns its own logic.

**Architecture:** Extract private `PhaseType`, `SequenceInfo`, and `buildSequenceMap` into a shared `SequenceModel.kt` with an explicit `nextSequence` field. Create a `PhaseStrategy` interface with `LinearPhaseStrategy`, `ScatterPhaseStrategy`, `ParallelPhaseStrategy` implementations. BarrierService becomes a thin coordinator: probe -> load -> delegate to strategy -> execute decision. Two independent schema changes (queue_name, WAITING_FOR_SIGNAL) prepare for future enhancements.

**Tech Stack:** Kotlin, JDBI 3, Oracle (Testcontainers), JUnit 5, kotlinx-coroutines-test

**Spec:** `docs/superpowers/specs/2026-03-28-engine-enhancements-design.md` (Section 5 + Section 7)

**Dependency graph:**
```
Task 1 (CAS verify)         -- independent, do first
Task 2 (SequenceModel)      -- foundation
Task 3 (PhaseStrategy API)  -- depends on Task 2
Task 4 (LinearStrategy)     -- depends on Task 3
Task 5 (ScatterStrategy)    -- depends on Task 3
Task 6 (ParallelStrategy)   -- depends on Task 3
Task 7 (Registry + Barrier) -- depends on Tasks 4-6
Task 8 (PARALLEL payload)   -- depends on Task 7
Task 9 (queue_name)         -- independent
Task 10 (WAITING_FOR_SIGNAL)-- independent
```

Tasks 1, 9, 10 are independent and can run in parallel with the critical chain (2-8).

---

### Task 1: Verify CAS Supports Non-Linear Jumps

**Files:**
- Read: `src/main/kotlin/engine/WorkflowRepository.kt:100-117`
- Test: `src/test/kotlin/engine/RepositoryTest.kt` (add test)

Audit `casAdvanceWithHandle` SQL to confirm no `seq + 1` assumption, then add a regression test exercising a non-linear jump.

- [ ] **Step 1: Read and audit `casAdvanceWithHandle`**

Read `src/main/kotlin/engine/WorkflowRepository.kt:100-117`. Verify the SQL uses `:nextSequence` as a bind parameter with no arithmetic.

Expected finding: the SQL is `SET current_sequence = :nextSequence` with WHERE `current_sequence = :expectedSequence AND version = :expectedVersion`. No hidden `seq + 1`. Confirmed safe for non-linear jumps.

- [ ] **Step 2: Write regression test for non-linear CAS jump**

Add to `src/test/kotlin/engine/RepositoryTest.kt`:

```kotlin
@Nested
inner class CasNonLinearJump {

    @Test
    fun `casAdvance supports non-linear jump from seq 1 to seq 5`() = runTest {
        val wfId = randomId()
        insertWorkflow(wfId, currentSequence = 1, version = 0)

        val won = workflowRepo.casAdvance(wfId, expectedSequence = 1, nextSequence = 5, expectedVersion = 0)

        assertTrue(won)
        val wf = workflowRepo.findById(wfId)!!
        assertEquals(5, wf.currentSequence)
        assertEquals(1, wf.version)
    }
}
```

- [ ] **Step 3: Run test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="RepositoryTest" -pl .`
Expected: All tests PASS including the new non-linear CAS test.

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/engine/RepositoryTest.kt
git commit -m "test: verify CAS supports non-linear sequence jumps (R4)"
```

---

### Task 2: Extract SequenceModel.kt (PhaseType, SequenceInfo, buildSequenceMap)

**Files:**
- Create: `src/main/kotlin/engine/SequenceModel.kt`
- Create: `src/test/kotlin/engine/SequenceModelTest.kt`
- Modify: `src/main/kotlin/engine/BarrierService.kt:225-245` (remove private types, use public ones)

- [ ] **Step 1: Write tests for `buildSequenceMap`**

Create `src/test/kotlin/engine/SequenceModelTest.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.WorkflowDefinition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SequenceModelTest {

    @Test
    fun `single linear activity produces one entry with nextSequence null`() {
        val def = WorkflowDefinition(
            activities = listOf(ActivityDefinition(name = "a", transition = "a.handler")),
        )
        val map = buildSequenceMap(def)

        assertEquals(1, map.size)
        val seq1 = map[1]!!
        assertEquals(PhaseType.LINEAR, seq1.phaseType)
        assertEquals(0, seq1.activityIndex)
        assertEquals("a", seq1.activity.name)
        assertEquals(1, seq1.sequenceNumber)
        assertNull(seq1.nextSequence, "Last sequence should have null nextSequence")
        assertNull(seq1.branchSequences)
    }

    @Test
    fun `two linear activities produce seq 1 next 2, seq 2 next null`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(name = "b", transition = "b.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        assertEquals(2, map[1]!!.nextSequence)
        assertNull(map[2]!!.nextSequence)
    }

    @Test
    fun `fan-out activity produces SCATTER then PARALLEL`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "scatter-activity",
                    transition = "scatter.handler",
                    fanOut = FanOutDefinition(transition = "parallel.handler"),
                ),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        val scatter = map[1]!!
        assertEquals(PhaseType.SCATTER, scatter.phaseType)
        assertEquals(1, scatter.sequenceNumber)
        assertEquals(2, scatter.nextSequence, "SCATTER next should point to PARALLEL")

        val parallel = map[2]!!
        assertEquals(PhaseType.PARALLEL, parallel.phaseType)
        assertEquals(2, parallel.sequenceNumber)
        assertNull(parallel.nextSequence, "Last PARALLEL should have null nextSequence")
    }

    @Test
    fun `fan-out then linear produces SCATTER 1 next 2, PARALLEL 2 next 3, LINEAR 3 next null`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "scatter-activity",
                    transition = "scatter.handler",
                    fanOut = FanOutDefinition(transition = "parallel.handler"),
                ),
                ActivityDefinition(name = "final", transition = "final.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(3, map.size)
        assertEquals(2, map[1]!!.nextSequence) // SCATTER -> PARALLEL
        assertEquals(3, map[2]!!.nextSequence) // PARALLEL -> LINEAR
        assertNull(map[3]!!.nextSequence)       // LINEAR -> end
    }

    @Test
    fun `linear then fan-out then linear produces correct chain`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "step1", transition = "step1.handler"),
                ActivityDefinition(
                    name = "scatter-activity",
                    transition = "scatter.handler",
                    fanOut = FanOutDefinition(transition = "parallel.handler"),
                ),
                ActivityDefinition(name = "step3", transition = "step3.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(4, map.size)
        // step1: LINEAR seq 1 -> 2
        assertEquals(PhaseType.LINEAR, map[1]!!.phaseType)
        assertEquals(2, map[1]!!.nextSequence)
        // scatter: SCATTER seq 2 -> 3
        assertEquals(PhaseType.SCATTER, map[2]!!.phaseType)
        assertEquals(3, map[2]!!.nextSequence)
        // parallel: PARALLEL seq 3 -> 4
        assertEquals(PhaseType.PARALLEL, map[3]!!.phaseType)
        assertEquals(4, map[3]!!.nextSequence)
        // step3: LINEAR seq 4 -> null
        assertEquals(PhaseType.LINEAR, map[4]!!.phaseType)
        assertNull(map[4]!!.nextSequence)
    }

    @Test
    fun `sequenceNumber field matches map key`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(
                    name = "b",
                    transition = "b.handler",
                    fanOut = FanOutDefinition(transition = "b.fan.handler"),
                ),
            ),
        )
        val map = buildSequenceMap(def)
        map.forEach { (key, info) ->
            assertEquals(key, info.sequenceNumber, "Map key should match sequenceNumber")
        }
    }

    @Test
    fun `branchSequences is null for all current phase types`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(
                    name = "b",
                    transition = "b.handler",
                    fanOut = FanOutDefinition(transition = "b.fan.handler"),
                ),
            ),
        )
        val map = buildSequenceMap(def)
        map.values.forEach { info ->
            assertNull(info.branchSequences, "branchSequences should be null for ${info.phaseType}")
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SequenceModelTest" -pl .`
Expected: FAIL — `SequenceModel.kt` does not exist yet.

- [ ] **Step 3: Create `SequenceModel.kt`**

Create `src/main/kotlin/engine/SequenceModel.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.WorkflowDefinition

enum class PhaseType { LINEAR, SCATTER, PARALLEL }

data class SequenceInfo(
    val sequenceNumber: Int,
    val activityIndex: Int,
    val activity: com.workflow.dsl.ActivityDefinition,
    val phaseType: PhaseType,
    val nextSequence: Int?,
    val branchSequences: Map<String, Int>? = null,
)

fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
    // Pass 1: allocate sequence numbers
    data class Entry(val activityIndex: Int, val phaseType: PhaseType, val seq: Int)
    val entries = mutableListOf<Entry>()
    var seq = 1
    for ((i, activity) in definition.activities.withIndex()) {
        if (activity.fanOut == null) {
            entries += Entry(i, PhaseType.LINEAR, seq++)
        } else {
            entries += Entry(i, PhaseType.SCATTER, seq++)
            entries += Entry(i, PhaseType.PARALLEL, seq++)
        }
    }
    // Pass 2: build SequenceInfo with nextSequence
    val map = mutableMapOf<Int, SequenceInfo>()
    for ((idx, entry) in entries.withIndex()) {
        val nextSeq = if (idx + 1 < entries.size) entries[idx + 1].seq else null
        map[entry.seq] = SequenceInfo(
            sequenceNumber = entry.seq,
            activityIndex = entry.activityIndex,
            activity = definition.activities[entry.activityIndex],
            phaseType = entry.phaseType,
            nextSequence = nextSeq,
        )
    }
    return map
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SequenceModelTest" -pl .`
Expected: All PASS.

- [ ] **Step 5: Update BarrierService to use public types**

In `src/main/kotlin/engine/BarrierService.kt`, delete the private types (lines 225-245):

```kotlin
// DELETE these lines:
private enum class PhaseType { LINEAR, SCATTER, PARALLEL }

private data class SequenceInfo(
    val activityIndex: Int,
    val activity: ActivityDefinition,
    val phaseType: PhaseType,
)

private fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
    val map = mutableMapOf<Int, SequenceInfo>()
    var seq = 1
    for ((i, activity) in definition.activities.withIndex()) {
        if (activity.fanOut == null) {
            map[seq++] = SequenceInfo(i, activity, PhaseType.LINEAR)
        } else {
            map[seq++] = SequenceInfo(i, activity, PhaseType.SCATTER)
            map[seq++] = SequenceInfo(i, activity, PhaseType.PARALLEL)
        }
    }
    return map
}
```

The public `buildSequenceMap` from `SequenceModel.kt` is auto-imported (same package). All references to `SequenceInfo` and `PhaseType` resolve to the new public types.

Update all `seqInfo.activityIndex` references — the old `SequenceInfo` didn't have `sequenceNumber`, but now it does. No code changes needed since the new type is a superset.

- [ ] **Step 6: Run full barrier test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest,SequenceModelTest" -pl .`
Expected: All PASS. The barrier uses `seqInfo.phaseType` and `seqInfo.activity` which exist in both old and new `SequenceInfo`. The new `sequenceNumber` and `nextSequence` fields are present but unused by the barrier's existing code.

- [ ] **Step 7: Commit**

```bash
git add src/main/kotlin/engine/SequenceModel.kt src/test/kotlin/engine/SequenceModelTest.kt src/main/kotlin/engine/BarrierService.kt
git commit -m "refactor: extract PhaseType, SequenceInfo, buildSequenceMap to SequenceModel.kt (R1)"
```

---

### Task 3: Create PhaseStrategy Interface and Helpers

**Files:**
- Create: `src/main/kotlin/engine/PhaseStrategy.kt`

This task creates the types that strategies implement. No tests yet — strategies are tested individually in Tasks 4-6.

- [ ] **Step 1: Create `PhaseStrategy.kt`**

Create `src/main/kotlin/engine/PhaseStrategy.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.WorkflowDefinition
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

interface PhaseStrategy {
    fun resolve(context: PhaseContext): AdvancementDecision
}

data class PhaseContext(
    val workflow: WorkflowRun,
    val definition: WorkflowDefinition,
    val currentSeqInfo: SequenceInfo,
    val sequenceMap: Map<Int, SequenceInfo>,
    val failedCount: Int,
    val totalCount: Int,
    val tasks: List<Task>,
)

sealed interface AdvancementDecision {
    data class Advance(val nextSequence: Int, val tasks: List<Task>) : AdvancementDecision
    data object Complete : AdvancementDecision
    /**
     * Abort the workflow — mark it as FAILED and cancel pending tasks.
     * Only returned when the phase failed AND [FailurePolicy.ABORT] is set.
     * BEST_EFFORT failures return [Advance] or [Complete] instead.
     */
    data class Abort(val reason: String) : AdvancementDecision
}

private val FAILED_STATUSES = setOf(TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.DEAD_LETTER)

/**
 * Shared failure-policy check. Returns null if no failure (caller continues to normal advance).
 * Returns [AdvancementDecision.Abort] for [FailurePolicy.ABORT], or [AdvancementDecision.Advance]/[AdvancementDecision.Complete]
 * for BEST_EFFORT (treat failure as success, advance with the given payload).
 */
fun PhaseContext.failOrAdvance(payload: String?): AdvancementDecision? {
    if (failedCount == 0) return null
    return when (currentSeqInfo.activity.failurePolicy) {
        FailurePolicy.ABORT -> AdvancementDecision.Abort(
            "$failedCount task(s) failed at sequence ${currentSeqInfo.sequenceNumber}",
        )
        FailurePolicy.BEST_EFFORT -> advanceOrComplete(payload)
    }
}

/**
 * Build an [AdvancementDecision.Advance] to the next sequence, or [AdvancementDecision.Complete]
 * if this is the last sequence. Creates a single task for the next sequence's activity.
 */
fun PhaseContext.advanceOrComplete(payload: String?): AdvancementDecision {
    val nextSeq = currentSeqInfo.nextSequence ?: return AdvancementDecision.Complete
    val nextSeqInfo = sequenceMap[nextSeq]!!
    val task = createTaskForActivity(
        workflowId = workflow.id,
        sequenceNumber = nextSeq,
        activity = nextSeqInfo.activity,
        payload = payload,
        now = Instant.now().truncatedTo(ChronoUnit.MICROS),
    )
    return AdvancementDecision.Advance(nextSeq, listOf(task))
}
```

- [ ] **Step 2: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl .`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/engine/PhaseStrategy.kt
git commit -m "feat: add PhaseStrategy interface, PhaseContext, AdvancementDecision, helpers"
```

---

### Task 4: Implement LinearPhaseStrategy

**Files:**
- Create: `src/main/kotlin/engine/LinearPhaseStrategy.kt`
- Create: `src/test/kotlin/engine/LinearPhaseStrategyTest.kt`

- [ ] **Step 1: Write tests**

Create `src/test/kotlin/engine/LinearPhaseStrategyTest.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.WorkflowDefinition
import kotlinx.coroutines.test.runTest
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class LinearPhaseStrategyTest {

    private val strategy = LinearPhaseStrategy()
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun activity(name: String = "step1", failurePolicy: FailurePolicy = FailurePolicy.ABORT) =
        ActivityDefinition(name = name, transition = "$name.handler", failurePolicy = failurePolicy)

    private fun task(
        status: TaskStatus = TaskStatus.COMPLETED,
        resultJson: String? = null,
    ) = Task(
        id = "t1", workflowId = "wf1", sequenceNumber = 1, status = status,
        handlerKey = "step1.handler", payloadJson = null, resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun context(
        activity: ActivityDefinition = activity(),
        nextSequence: Int? = 2,
        failedCount: Int = 0,
        tasks: List<Task> = listOf(task()),
    ): PhaseContext {
        val seqInfo = SequenceInfo(1, 0, activity, PhaseType.LINEAR, nextSequence)
        val nextAct = ActivityDefinition(name = "step2", transition = "step2.handler")
        val sequenceMap = mutableMapOf(1 to seqInfo)
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 1, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(activity, nextAct))
        val wf = WorkflowRun("wf1", "{}", 1, 0, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, seqInfo, sequenceMap, failedCount, tasks.size, tasks)
    }

    @Test
    fun `success with next sequence returns Advance`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(2, advance.nextSequence)
        assertEquals(1, advance.tasks.size)
        assertEquals("step2.handler", advance.tasks[0].handlerKey)
    }

    @Test
    fun `success propagates resultJson as next task payload`() {
        val ctx = context(tasks = listOf(task(resultJson = """{"out":"data"}""")))
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals("""{"out":"data"}""", advance.tasks[0].payloadJson)
    }

    @Test
    fun `success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with ABORT returns Abort`() {
        val ctx = context(failedCount = 1, tasks = listOf(task(status = TaskStatus.FAILED)))
        val fail = assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
        assert(fail.reason.contains("1 task(s) failed"))
    }

    @Test
    fun `failure with BEST_EFFORT returns Advance with null payload`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            failedCount = 1,
            tasks = listOf(task(status = TaskStatus.FAILED)),
        )
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(2, advance.nextSequence)
        assertNull(advance.tasks[0].payloadJson)
    }

    @Test
    fun `failure with BEST_EFFORT at last sequence returns Complete`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            nextSequence = null,
            failedCount = 1,
            tasks = listOf(task(status = TaskStatus.FAILED)),
        )
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="LinearPhaseStrategyTest" -pl .`
Expected: FAIL — `LinearPhaseStrategy` does not exist.

- [ ] **Step 3: Implement LinearPhaseStrategy**

Create `src/main/kotlin/engine/LinearPhaseStrategy.kt`:

```kotlin
package com.workflow.engine

class LinearPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        // Check failure
        context.failOrAdvance(payload = null)?.let { return it }

        // Normal advance
        val payload = context.tasks.firstOrNull { it.status == TaskStatus.COMPLETED }?.resultJson
        return context.advanceOrComplete(payload)
    }
}
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="LinearPhaseStrategyTest" -pl .`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/LinearPhaseStrategy.kt src/test/kotlin/engine/LinearPhaseStrategyTest.kt
git commit -m "feat: add LinearPhaseStrategy with tests"
```

---

### Task 5: Implement ScatterPhaseStrategy

**Files:**
- Create: `src/main/kotlin/engine/ScatterPhaseStrategy.kt`
- Create: `src/test/kotlin/engine/ScatterPhaseStrategyTest.kt`

- [ ] **Step 1: Write tests**

Create `src/test/kotlin/engine/ScatterPhaseStrategyTest.kt`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.WorkflowDefinition
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class ScatterPhaseStrategyTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    private val strategy = ScatterPhaseStrategy(objectMapper)
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private val fanOutActivity = ActivityDefinition(
        name = "scatter-activity",
        transition = "scatter.handler",
        fanOut = FanOutDefinition(transition = "parallel.handler", retries = 2),
    )

    private fun scatterTask(
        status: TaskStatus = TaskStatus.COMPLETED,
        resultJson: String? = null,
    ) = Task(
        id = "t1", workflowId = "wf1", sequenceNumber = 1, status = status,
        handlerKey = "scatter.handler", payloadJson = null, resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun context(
        failedCount: Int = 0,
        tasks: List<Task> = listOf(scatterTask(resultJson = """["a","b","c"]""")),
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    ): PhaseContext {
        val act = fanOutActivity.copy(failurePolicy = failurePolicy)
        val scatterSeq = SequenceInfo(1, 0, act, PhaseType.SCATTER, nextSequence = 2)
        val parallelSeq = SequenceInfo(2, 0, act, PhaseType.PARALLEL, nextSequence = null)
        val sequenceMap = mapOf(1 to scatterSeq, 2 to parallelSeq)
        val def = WorkflowDefinition(activities = listOf(act))
        val wf = WorkflowRun("wf1", "{}", 1, 0, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, scatterSeq, sequenceMap, failedCount, tasks.size, tasks)
    }

    @Test
    fun `success creates fan-out tasks from scatter result`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(2, advance.nextSequence)
        assertEquals(3, advance.tasks.size)
        advance.tasks.forEach { task ->
            assertEquals("parallel.handler", task.handlerKey)
            assertEquals(2, task.sequenceNumber)
            assertEquals(TaskStatus.PENDING, task.status)
            assertEquals(2, task.maxRetries)
        }
        assertEquals("a", advance.tasks[0].payloadJson)
        assertEquals("b", advance.tasks[1].payloadJson)
        assertEquals("c", advance.tasks[2].payloadJson)
    }

    @Test
    fun `failure with ABORT returns Abort`() {
        val ctx = context(failedCount = 1, tasks = listOf(scatterTask(status = TaskStatus.FAILED)))
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with BEST_EFFORT returns Advance to parallel with empty task list`() {
        val ctx = context(
            failedCount = 1,
            tasks = listOf(scatterTask(status = TaskStatus.FAILED)),
            failurePolicy = FailurePolicy.BEST_EFFORT,
        )
        // BEST_EFFORT on SCATTER with no result: advance with null payload (single task for next seq)
        val advance = assertIs<AdvancementDecision.Advance>(ctx.failOrAdvance(null))
        assertNull(advance.tasks[0].payloadJson)
    }

    @Test
    fun `scatter result with null resultJson returns Abort`() {
        val ctx = context(tasks = listOf(scatterTask(resultJson = null)))
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ScatterPhaseStrategyTest" -pl .`
Expected: FAIL.

- [ ] **Step 3: Implement ScatterPhaseStrategy**

Create `src/main/kotlin/engine/ScatterPhaseStrategy.kt`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

class ScatterPhaseStrategy(
    private val objectMapper: ObjectMapper,
) : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        // Check failure
        context.failOrAdvance(payload = null)?.let { return it }

        // Read scatter result
        val scatterTask = context.tasks.firstOrNull { it.status == TaskStatus.COMPLETED }
            ?: return AdvancementDecision.Abort("No completed scatter task at sequence ${context.currentSeqInfo.sequenceNumber}")
        val scatterResult = scatterTask.resultJson
            ?: return AdvancementDecision.Abort("Scatter task ${scatterTask.id} has no result")

        // Deserialize payload array
        val payloads: List<String> = objectMapper.readValue(scatterResult)
        val parallelSeq = context.currentSeqInfo.nextSequence!!
        val parallelSeqInfo = context.sequenceMap[parallelSeq]!!
        val fanOut = parallelSeqInfo.activity.fanOut!!
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

        // Create fan-out tasks
        val tasks = payloads.map { payload ->
            Task(
                id = UUID.randomUUID().toString(),
                workflowId = context.workflow.id,
                sequenceNumber = parallelSeq,
                status = TaskStatus.PENDING,
                handlerKey = fanOut.transition,
                payloadJson = payload,
                resultJson = null,
                claimedBy = null,
                claimedAt = null,
                completedAt = null,
                retryCount = 0,
                maxRetries = fanOut.retries,
                deadlineAt = now.plus(fanOut.deadline),
                backoffBase = fanOut.backoffBase.seconds.toInt(),
                backoffCap = fanOut.backoffCap.seconds.toInt(),
            )
        }
        return AdvancementDecision.Advance(parallelSeq, tasks)
    }
}
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ScatterPhaseStrategyTest" -pl .`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/ScatterPhaseStrategy.kt src/test/kotlin/engine/ScatterPhaseStrategyTest.kt
git commit -m "feat: add ScatterPhaseStrategy with tests"
```

---

### Task 6: Implement ParallelPhaseStrategy

**Files:**
- Create: `src/main/kotlin/engine/ParallelPhaseStrategy.kt`
- Create: `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`

Note: This task implements the PARALLEL strategy with the **current behavior** (null payload after join). Task 8 updates it to aggregate results.

- [ ] **Step 1: Write tests**

Create `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class ParallelPhaseStrategyTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    private val strategy = ParallelPhaseStrategy(objectMapper)
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun parallelTask(status: TaskStatus = TaskStatus.COMPLETED, resultJson: String? = null) = Task(
        id = "t-${System.nanoTime()}", workflowId = "wf1", sequenceNumber = 2, status = status,
        handlerKey = "parallel.handler", payloadJson = null, resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun context(
        joinPolicy: JoinPolicy = JoinPolicy.All,
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
        nextSequence: Int? = 3,
        failedCount: Int = 0,
        tasks: List<Task> = listOf(parallelTask(), parallelTask(), parallelTask()),
    ): PhaseContext {
        val act = ActivityDefinition(
            name = "scatter-activity",
            transition = "scatter.handler",
            failurePolicy = failurePolicy,
            fanOut = FanOutDefinition(transition = "parallel.handler", joinPolicy = joinPolicy),
        )
        val nextAct = ActivityDefinition(name = "final", transition = "final.handler")
        val parallelSeq = SequenceInfo(2, 0, act, PhaseType.PARALLEL, nextSequence)
        val sequenceMap = mutableMapOf(2 to parallelSeq)
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 1, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(act, nextAct))
        val wf = WorkflowRun("wf1", "{}", 2, 1, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, parallelSeq, sequenceMap, failedCount, tasks.size, tasks)
    }

    @Test
    fun `JoinPolicy All success advances to next sequence`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(3, advance.nextSequence)
        assertEquals(1, advance.tasks.size)
        assertEquals("final.handler", advance.tasks[0].handlerKey)
    }

    @Test
    fun `JoinPolicy All success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy All with failure returns Abort`() {
        val tasks = listOf(parallelTask(), parallelTask(status = TaskStatus.FAILED), parallelTask())
        val ctx = context(failedCount = 1, tasks = tasks)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold success when enough tasks succeed`() {
        val tasks = listOf(
            parallelTask(), parallelTask(), parallelTask(status = TaskStatus.FAILED),
        )
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 1, tasks = tasks)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold failure when not enough succeed`() {
        val tasks = listOf(
            parallelTask(), parallelTask(status = TaskStatus.FAILED), parallelTask(status = TaskStatus.FAILED),
        )
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 2, tasks = tasks)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage success at boundary`() {
        // 95 completed, 5 failed out of 100 = 95% >= 95 -> success
        val tasks = (1..95).map { parallelTask() } + (1..5).map { parallelTask(status = TaskStatus.FAILED) }
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 5, tasks = tasks)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage failure below boundary`() {
        // 94 completed, 6 failed out of 100 = 94% < 95 -> failure
        val tasks = (1..94).map { parallelTask() } + (1..6).map { parallelTask(status = TaskStatus.FAILED) }
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 6, tasks = tasks)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with BEST_EFFORT advances with null payload`() {
        val tasks = listOf(parallelTask(), parallelTask(status = TaskStatus.FAILED))
        val ctx = context(failedCount = 1, tasks = tasks, failurePolicy = FailurePolicy.BEST_EFFORT)
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertNull(advance.tasks[0].payloadJson)
    }

    @Test
    fun `success propagates null payload (pre-R3 behavior)`() {
        val tasks = listOf(
            parallelTask(resultJson = """{"r":"one"}"""),
            parallelTask(resultJson = """{"r":"two"}"""),
        )
        val ctx = context(tasks = tasks)
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        // Pre-R3: null payload after PARALLEL join
        assertNull(advance.tasks[0].payloadJson)
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ParallelPhaseStrategyTest" -pl .`
Expected: FAIL.

- [ ] **Step 3: Implement ParallelPhaseStrategy**

Create `src/main/kotlin/engine/ParallelPhaseStrategy.kt`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.dsl.JoinPolicy

class ParallelPhaseStrategy(
    private val objectMapper: ObjectMapper,
) : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val joinPolicy = context.currentSeqInfo.activity.fanOut!!.joinPolicy
        val succeeded = evaluateJoinPolicy(joinPolicy, context.failedCount, context.totalCount)

        if (!succeeded) {
            // failOrAdvance handles ABORT vs BEST_EFFORT
            context.failOrAdvance(payload = null)?.let { return it }
        }

        // Pre-R3: null payload after PARALLEL join (Task 8 changes this)
        return context.advanceOrComplete(payload = null)
    }

    private fun evaluateJoinPolicy(joinPolicy: JoinPolicy, failedCount: Int, totalCount: Int): Boolean {
        val succeededCount = totalCount - failedCount
        return when (joinPolicy) {
            is JoinPolicy.All -> failedCount == 0
            is JoinPolicy.Threshold -> succeededCount >= joinPolicy.n
            is JoinPolicy.Percentage -> {
                val successPct = if (totalCount > 0) (succeededCount * 100) / totalCount else 0
                successPct >= joinPolicy.pct
            }
        }
    }
}
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ParallelPhaseStrategyTest" -pl .`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/ParallelPhaseStrategy.kt src/test/kotlin/engine/ParallelPhaseStrategyTest.kt
git commit -m "feat: add ParallelPhaseStrategy with tests"
```

---

### Task 7: PhaseStrategyRegistry + Refactor BarrierService

**Files:**
- Create: `src/main/kotlin/engine/PhaseStrategyRegistry.kt`
- Modify: `src/main/kotlin/engine/BarrierService.kt` (full rewrite to coordinator)
- Modify: `src/test/kotlin/engine/BarrierServiceTest.kt` (update constructor)

This is the highest-risk task. It replaces all `when (phaseType)` switches and `seq + 1` arithmetic in BarrierService with strategy delegation.

- [ ] **Step 1: Create PhaseStrategyRegistry**

Create `src/main/kotlin/engine/PhaseStrategyRegistry.kt`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class PhaseStrategyRegistry(objectMapper: ObjectMapper) {

    private val strategies = ConcurrentHashMap<PhaseType, PhaseStrategy>()

    init {
        register(PhaseType.LINEAR, LinearPhaseStrategy())
        register(PhaseType.SCATTER, ScatterPhaseStrategy(objectMapper))
        register(PhaseType.PARALLEL, ParallelPhaseStrategy(objectMapper))
    }

    fun register(type: PhaseType, strategy: PhaseStrategy) {
        strategies[type] = strategy
    }

    fun resolve(type: PhaseType): PhaseStrategy =
        strategies[type] ?: throw IllegalStateException("No strategy registered for phase type: $type")
}
```

- [ ] **Step 2: Rewrite BarrierService**

Replace the entire content of `src/main/kotlin/engine/BarrierService.kt` with:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.dsl.WorkflowDefinition
import com.workflow.extension.inTransactionSuspend
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant

private val FAILED_STATUSES = setOf(TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.DEAD_LETTER)

@ApplicationScoped
class BarrierService(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val strategyRegistry: PhaseStrategyRegistry,
) {

    private val log = LoggerFactory.getLogger(BarrierService::class.java)

    suspend fun onTaskCompleted(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        status: TaskStatus,
        resultJson: String?,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
    ) {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            // 1. Self-update
            val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
            if (!updated) return@inTransactionSuspend

            // 2. Lock-free probe (lightweight, fast bail for common case)
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            // 3. All tasks terminal — evaluate and advance
            evaluateAndAdvance(handle, workflowId, sequenceNumber)
        }
    }

    internal suspend fun recoverStuckWorkflow(workflowId: String) {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: run {
                    log.warn("Workflow not found during recovery: {}", workflowId)
                    return@inTransactionSuspend
                }
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

            val seq = workflow.currentSequence

            // TOCTOU safety: re-probe all tasks terminal
            val tasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, seq)
            if (tasks.any { !it.status.isTerminal }) return@inTransactionSuspend

            // Delegate to strategy
            val failedCount = tasks.count { it.status in FAILED_STATUSES }
            resolveAndExecute(handle, workflow, seq, tasks, failedCount)
        }
    }

    private fun evaluateAndAdvance(handle: Handle, workflowId: String, sequenceNumber: Int) {
        // Load workflow
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: throw IllegalStateException("Workflow not found: $workflowId")
        if (workflow.status != WorkflowStatus.RUNNING) return

        // Load tasks for strategy context (replaces countFailed + countTotal)
        val tasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, sequenceNumber)
        val failedCount = tasks.count { it.status in FAILED_STATUSES }

        resolveAndExecute(handle, workflow, sequenceNumber, tasks, failedCount)
    }

    private fun resolveAndExecute(
        handle: Handle,
        workflow: WorkflowRun,
        sequenceNumber: Int,
        tasks: List<Task>,
        failedCount: Int,
    ) {
        val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqInfo = sequenceMap[sequenceNumber]
            ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

        val strategy = strategyRegistry.resolve(seqInfo.phaseType)
        val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, tasks.size, tasks)
        val decision = strategy.resolve(context)

        executeDecision(handle, workflow, seqInfo, decision)
    }

    private fun executeDecision(
        handle: Handle,
        workflow: WorkflowRun,
        seqInfo: SequenceInfo,
        decision: AdvancementDecision,
    ) {
        when (decision) {
            is AdvancementDecision.Advance -> {
                val casWon = workflowRepo.casAdvanceWithHandle(
                    handle, workflow.id, seqInfo.sequenceNumber, decision.nextSequence, workflow.version,
                )
                if (!casWon) {
                    log.debug("CAS lost for workflow {} at sequence {}", workflow.id, seqInfo.sequenceNumber)
                    return
                }
                taskRepo.insertBatchWithHandle(handle, decision.tasks)
            }
            is AdvancementDecision.Complete -> {
                workflowRepo.updateStatusWithHandle(
                    handle, workflow.id, WorkflowStatus.COMPLETED, expectedStatus = WorkflowStatus.RUNNING,
                )
            }
            is AdvancementDecision.Abort -> {
                log.warn("Workflow {} failed at sequence {}: {}", workflow.id, seqInfo.sequenceNumber, decision.reason)
                val updated = workflowRepo.updateStatusWithHandle(
                    handle, workflow.id, WorkflowStatus.FAILED, expectedStatus = WorkflowStatus.RUNNING,
                )
                if (updated) {
                    taskRepo.cancelPendingTasksWithHandle(handle, workflow.id)
                }
            }
        }
    }
}
```

- [ ] **Step 3: Update BarrierServiceTest constructor**

In `src/test/kotlin/engine/BarrierServiceTest.kt`, update the `setup()` method. The test needs a real `PhaseStrategyRegistry`.

Change the `@BeforeAll` setup from:

```kotlin
private lateinit var barrier: BarrierService

@BeforeAll
fun setup() {
    jdbi = OracleTestContainer.jdbi
    workflowRepo = WorkflowRepository(jdbi)
    taskRepo = TaskRepository(jdbi)
    barrier = BarrierService(jdbi, workflowRepo, taskRepo, objectMapper)
    engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
}
```

to:

```kotlin
private lateinit var barrier: BarrierService
private lateinit var strategyRegistry: PhaseStrategyRegistry

@BeforeAll
fun setup() {
    jdbi = OracleTestContainer.jdbi
    workflowRepo = WorkflowRepository(jdbi)
    taskRepo = TaskRepository(jdbi)
    strategyRegistry = PhaseStrategyRegistry(objectMapper)
    barrier = BarrierService(jdbi, workflowRepo, taskRepo, objectMapper, strategyRegistry)
    engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
}
```

- [ ] **Step 4: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest,SequenceModelTest,LinearPhaseStrategyTest,ScatterPhaseStrategyTest,ParallelPhaseStrategyTest" -pl .`
Expected: All PASS. The refactored BarrierService delegates to the same logic, just distributed across strategy classes.

**If any test fails:** The most likely failures are:
- `BEST_EFFORT with non-null resultJson` (Test 9b): Currently propagates failed task's result. Strategy reads from `context.tasks`, which includes the self-updated task. If the result was written by `updateStatusWithHandle`, it should be visible in `findByWorkflowAndSequenceWithHandle`. Check that the strategy reads the payload correctly.
- `PARALLEL→LINEAR payload propagation` (Test 10): Currently asserts null payload. Strategy also returns null (pre-R3). Should pass as-is.

- [ ] **Step 5: Run the full project test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All PASS. WorkflowEngineTest, SweeperTest, WorkerLoopTest, WorkflowIntegrationTest must still pass since they call `onTaskCompleted` and `recoverStuckWorkflow` through the same public API.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/engine/PhaseStrategyRegistry.kt src/main/kotlin/engine/BarrierService.kt src/test/kotlin/engine/BarrierServiceTest.kt
git commit -m "refactor: replace BarrierService internals with strategy delegation (R2)"
```

---

### Task 8: Fix PARALLEL Payload Propagation (R3)

**Files:**
- Modify: `src/main/kotlin/engine/ParallelPhaseStrategy.kt`
- Modify: `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`
- Modify: `src/test/kotlin/engine/BarrierServiceTest.kt` (update Test 10 assertion)

- [ ] **Step 1: Update ParallelPhaseStrategyTest for aggregated payload**

In `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`, replace the last test:

```kotlin
@Test
fun `success propagates null payload (pre-R3 behavior)`() {
```

with:

```kotlin
@Test
fun `success aggregates completed task results as JSON array payload`() {
    val tasks = listOf(
        parallelTask(resultJson = """{"r":"one"}"""),
        parallelTask(resultJson = """{"r":"two"}"""),
    )
    val ctx = context(tasks = tasks)
    val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    // R3: aggregated results as JSON array
    val expected = """[{"r":"one"},{"r":"two"}]"""
    assertEquals(expected, advance.tasks[0].payloadJson)
}

@Test
fun `success with mixed null results only includes non-null`() {
    val tasks = listOf(
        parallelTask(resultJson = """{"r":"one"}"""),
        parallelTask(resultJson = null),
        parallelTask(resultJson = """{"r":"three"}"""),
    )
    val ctx = context(tasks = tasks)
    val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    val expected = """[{"r":"one"},{"r":"three"}]"""
    assertEquals(expected, advance.tasks[0].payloadJson)
}

@Test
fun `success with join policy filters only completed results`() {
    val tasks = listOf(
        parallelTask(resultJson = """{"r":"ok"}"""),
        parallelTask(status = TaskStatus.FAILED, resultJson = """{"r":"err"}"""),
        parallelTask(resultJson = """{"r":"also-ok"}"""),
    )
    // Threshold(2): 2 succeeded >= 2 -> success, but only include COMPLETED results
    val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 1, tasks = tasks)
    val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    val expected = """[{"r":"ok"},{"r":"also-ok"}]"""
    assertEquals(expected, advance.tasks[0].payloadJson)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ParallelPhaseStrategyTest" -pl .`
Expected: FAIL — strategy still returns null payload.

- [ ] **Step 3: Update ParallelPhaseStrategy to aggregate results**

In `src/main/kotlin/engine/ParallelPhaseStrategy.kt`, replace the `resolve` method:

```kotlin
override fun resolve(context: PhaseContext): AdvancementDecision {
    val joinPolicy = context.currentSeqInfo.activity.fanOut!!.joinPolicy
    val succeeded = evaluateJoinPolicy(joinPolicy, context.failedCount, context.totalCount)

    if (!succeeded) {
        context.failOrAdvance(payload = null)?.let { return it }
    }

    // Aggregate completed task results into JSON array
    val results = context.tasks
        .filter { it.status == TaskStatus.COMPLETED }
        .mapNotNull { it.resultJson }
    val aggregatedPayload = objectMapper.writeValueAsString(results)

    return context.advanceOrComplete(payload = aggregatedPayload)
}
```

- [ ] **Step 4: Run strategy test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ParallelPhaseStrategyTest" -pl .`
Expected: All PASS.

- [ ] **Step 5: Update BarrierServiceTest Test 10**

In `src/test/kotlin/engine/BarrierServiceTest.kt`, find `ParallelToLinearPayload` (Test 10). Update the assertion from:

```kotlin
assertTrue(seq3Tasks[0]["PAYLOAD"] == null,
    "PARALLEL→LINEAR: next task payload should be null")
```

to:

```kotlin
val payload = seq3Tasks[0]["PAYLOAD"] as? String
assertNotNull(payload, "PARALLEL→LINEAR: next task payload should be aggregated results")
val results: List<String> = objectMapper.readValue(payload)
// Only the first task had a non-null resultJson
assertEquals(1, results.size)
assertEquals("""{"r":"one"}""", results[0])
```

Add the import:
```kotlin
import com.fasterxml.jackson.module.kotlin.readValue
```

- [ ] **Step 6: Run full barrier test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="BarrierServiceTest,ParallelPhaseStrategyTest" -pl .`
Expected: All PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/kotlin/engine/ParallelPhaseStrategy.kt src/test/kotlin/engine/ParallelPhaseStrategyTest.kt src/test/kotlin/engine/BarrierServiceTest.kt
git commit -m "fix: aggregate PARALLEL results into JSON array payload (R3)"
```

---

### Task 9: Add `queue_name` Column (R5)

**Files:**
- Create: `src/main/resources/db/migration/V6__queue_name.sql`
- Modify: `src/main/kotlin/engine/WorkflowModels.kt` (add `queueName` to Task)
- Modify: `src/main/kotlin/dsl/WorkflowDsl.kt` (add `queue` to ActivityDefinition, FanOutDefinition)
- Modify: `src/main/kotlin/dsl/WorkflowDslBuilders.kt` (add `queue` builder method)
- Modify: `src/main/kotlin/engine/TaskRepository.kt` (add `queue_name` to claimNext, insertBatch, mapTaskRow)
- Modify: `src/test/kotlin/engine/OracleTestContainer.kt` (run V6 migration)
- Modify: `src/test/kotlin/engine/BarrierServiceTest.kt` (add queue_name to insertTaskDirect)

- [ ] **Step 1: Create V6 migration**

Create `src/main/resources/db/migration/V6__queue_name.sql`:

```sql
ALTER TABLE task ADD queue_name VARCHAR2(100) DEFAULT 'default' NOT NULL;
CREATE INDEX idx_task_queue_status ON task (queue_name, status, not_before, claimed_at);
```

- [ ] **Step 2: Add `queue` field to model and DSL**

In `src/main/kotlin/dsl/WorkflowDsl.kt`, add `queue` to both `ActivityDefinition` and `FanOutDefinition`:

Add to `ActivityDefinition`:
```kotlin
val queue: String = "default",
```

Add to `FanOutDefinition`:
```kotlin
val queue: String = "default",
```

In `src/main/kotlin/engine/WorkflowModels.kt`, add to `Task`:
```kotlin
val queueName: String = "default",
```

In `createTaskForActivity`, propagate the queue:
```kotlin
queueName = activity.queue,
```

In `src/main/kotlin/dsl/WorkflowDslBuilders.kt`, add to `ActivityBuilder`:
```kotlin
private var queue: String = "default"
fun queue(q: String) { queue = q }
```
And in `ActivityBuilder.build()`, add `queue = queue` to the constructor call.

Add to `FanOutBuilder`:
```kotlin
private var queue: String = "default"
fun queue(q: String) { queue = q }
```
And in `FanOutBuilder.build()`, add `queue = queue` to the constructor call.

- [ ] **Step 3: Update TaskRepository**

In `src/main/kotlin/engine/TaskRepository.kt`:

**claimNext** — add `queueName` parameter and WHERE clause:

Change the method signature to:
```kotlin
suspend fun claimNext(workerId: String, limit: Int, queueName: String = "default"): List<Task>
```

Add to the WHERE clause:
```sql
AND queue_name = :queueName
```

And bind it:
```kotlin
.bind("queueName", queueName)
```

**insertBatchWithHandle** — add `queue_name` to the INSERT:

Add `queue_name` to the column list and `:queueName` to the values. Add the bind:
```kotlin
.bind("queueName", task.queueName)
```

**mapTaskRow** — add `queueName` to the mapping:
```kotlin
queueName = (ci["QUEUE_NAME"] as? String) ?: "default",
```

- [ ] **Step 4: Update ScatterPhaseStrategy fan-out task creation**

In `src/main/kotlin/engine/ScatterPhaseStrategy.kt`, add `queueName` to the fan-out Task constructor. The queue comes from `fanOut.queue`:

```kotlin
queueName = fanOut.queue,
```

- [ ] **Step 5: Update test infrastructure**

In `src/test/kotlin/engine/OracleTestContainer.kt`, add V6 migration:
```kotlin
handle.createScript(loader.getResource("db/migration/V6__queue_name.sql")!!.readText()).execute()
```

In `src/test/kotlin/engine/BarrierServiceTest.kt`, update `insertTaskDirect` to include `queue_name`:

Add to the INSERT SQL: `, queue_name` in column list and `, :queueName` in values.
Add the bind: `stmt.bind("queueName", task.queueName)`

- [ ] **Step 6: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All PASS. All existing tests use default queue, backward-compatible.

- [ ] **Step 7: Commit**

```bash
git add src/main/resources/db/migration/V6__queue_name.sql src/main/kotlin/dsl/WorkflowDsl.kt src/main/kotlin/dsl/WorkflowDslBuilders.kt src/main/kotlin/engine/WorkflowModels.kt src/main/kotlin/engine/TaskRepository.kt src/main/kotlin/engine/ScatterPhaseStrategy.kt src/test/kotlin/engine/OracleTestContainer.kt src/test/kotlin/engine/BarrierServiceTest.kt
git commit -m "feat: add queue_name column and routing support (R5)"
```

---

### Task 10: Add `WAITING_FOR_SIGNAL` Status (R6)

**Files:**
- Create: `src/main/resources/db/migration/V7__waiting_for_signal.sql`
- Modify: `src/main/kotlin/engine/WorkflowModels.kt` (add status + transitions)
- Modify: `src/test/kotlin/engine/OracleTestContainer.kt` (run V7 migration)
- Modify: `src/test/kotlin/engine/WorkflowModelsTest.kt` (update assertions)

- [ ] **Step 1: Create V7 migration**

Create `src/main/resources/db/migration/V7__waiting_for_signal.sql`:

```sql
-- Drop and recreate CHECK constraint to include WAITING_FOR_SIGNAL.
-- Original V1 constraint: status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')
-- V2 added DEAD_LETTER, V4 added CANCELLED and TIMED_OUT.
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status CHECK (
    status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'WAITING_FOR_SIGNAL')
);
```

- [ ] **Step 2: Update TaskStatus enum**

In `src/main/kotlin/engine/WorkflowModels.kt`:

Add `WAITING_FOR_SIGNAL` to the enum:
```kotlin
enum class TaskStatus {
    PENDING, PROCESSING, WAITING_FOR_SIGNAL, COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED;
```

`WAITING_FOR_SIGNAL` is NOT terminal (not in `terminalStatuses`). Add transitions:
```kotlin
private val allowed = setOf(
    PENDING to PROCESSING,
    PENDING to CANCELLED,
    PROCESSING to COMPLETED,
    PROCESSING to FAILED,
    PROCESSING to TIMED_OUT,
    PROCESSING to PENDING,
    PROCESSING to DEAD_LETTER,
    PROCESSING to WAITING_FOR_SIGNAL,      // handler suspends task
    WAITING_FOR_SIGNAL to COMPLETED,        // signal: approved
    WAITING_FOR_SIGNAL to FAILED,           // signal: rejected
    WAITING_FOR_SIGNAL to TIMED_OUT,        // sweeper: deadline expired
    WAITING_FOR_SIGNAL to CANCELLED,        // workflow cancelled
    FAILED to PENDING,
    FAILED to DEAD_LETTER,
)
```

- [ ] **Step 3: Update tests**

In `src/test/kotlin/engine/WorkflowModelsTest.kt`:

Update `TaskStatus contains exactly seven values` to eight:
```kotlin
@Test
fun `TaskStatus contains exactly eight values`() {
    assertEquals(
        setOf("PENDING", "PROCESSING", "WAITING_FOR_SIGNAL", "COMPLETED", "FAILED", "TIMED_OUT", "DEAD_LETTER", "CANCELLED"),
        TaskStatus.entries.map { it.name }.toSet(),
    )
}
```

Add `WAITING_FOR_SIGNAL` to the non-terminal check:
```kotlin
@Test
fun `WAITING_FOR_SIGNAL is not terminal`() {
    assertEquals(false, TaskStatus.WAITING_FOR_SIGNAL.isTerminal)
}
```

Add the new transitions to the legal transitions test:
```kotlin
TaskStatus.PROCESSING to TaskStatus.WAITING_FOR_SIGNAL,
TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.COMPLETED,
TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.FAILED,
TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.TIMED_OUT,
TaskStatus.WAITING_FOR_SIGNAL to TaskStatus.CANCELLED,
```

Update OracleTestContainer to run V7:
```kotlin
handle.createScript(loader.getResource("db/migration/V7__waiting_for_signal.sql")!!.readText()).execute()
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl .`
Expected: All PASS.

- [ ] **Step 5: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All PASS. `countNonTerminalWithHandle` WHERE clause uses `status NOT IN (terminal statuses)` which is hardcoded in SQL. `WAITING_FOR_SIGNAL` is not in the hardcoded list, so it counts as non-terminal in the DB query too.

**Important:** Verify the SQL `NOT IN` list in `countNonTerminalWithHandle` (TaskRepository.kt:258):
```sql
AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED')
```
`WAITING_FOR_SIGNAL` is NOT in this list, so it counts as non-terminal. Correct behavior — the barrier will block while a task is waiting for a signal.

- [ ] **Step 6: Commit**

```bash
git add src/main/resources/db/migration/V7__waiting_for_signal.sql src/main/kotlin/engine/WorkflowModels.kt src/test/kotlin/engine/WorkflowModelsTest.kt src/test/kotlin/engine/OracleTestContainer.kt
git commit -m "feat: add WAITING_FOR_SIGNAL status to task model and schema (R6)"
```

---

## Verification Checklist

After all tasks complete, run the full test suite and verify:

```bash
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .
```

Verify no `when (phaseType)` switches remain in BarrierService:
```bash
grep -n "when.*phaseType\|PhaseType\.\(LINEAR\|SCATTER\|PARALLEL\)" src/main/kotlin/engine/BarrierService.kt
```
Expected: No matches.

Verify no `seq + 1` or `seq - 1` arithmetic in BarrierService:
```bash
grep -n "seq.*+.*1\|seq.*-.*1\|sequence.*+.*1\|sequence.*-.*1" src/main/kotlin/engine/BarrierService.kt
```
Expected: No matches.
