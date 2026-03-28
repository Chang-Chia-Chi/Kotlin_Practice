# Independent Fan-Out Activity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote fan-out from a nested `FanOutDefinition` to an independent peer activity, eliminating bulk task loading at barrier time.

**Architecture:** Remove `FanOutDefinition` and `PhaseType.SCATTER`. Each activity gets one sequence. Fan-out targets are identified by being referenced in another activity's `fanOut: String?` field. Strategies use counts only. Fan-out task creation uses SQL `INSERT...SELECT` with `JSON_TABLE`. `InputResolver` uses `PhaseType` (LINEAR vs PARALLEL) to decide single-fetch vs aggregation.

**Tech Stack:** Kotlin, JDBI 3, Oracle (JSON_TABLE), JUnit 5, kotlinx-coroutines-test

---

### Task 1: Model & DSL Layer

**Files:**
- Modify: `src/main/kotlin/dsl/WorkflowDsl.kt`
- Modify: `src/main/kotlin/dsl/WorkflowDslBuilders.kt`
- Modify: `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt`
- Modify: `src/test/kotlin/dsl/WorkflowDslTest.kt`

- [ ] **Step 1: Update `ActivityDefinition` and delete `FanOutDefinition`**

In `src/main/kotlin/dsl/WorkflowDsl.kt`, delete `FanOutDefinition` (lines 31-40) and change `ActivityDefinition`:

```kotlin
data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: String? = null,
    val joinPolicy: JoinPolicy = JoinPolicy.All,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
)
```

- [ ] **Step 2: Add validation to `WorkflowDefinition.init`**

In `src/main/kotlin/dsl/WorkflowDsl.kt`, add after the existing duplicate-name check:

```kotlin
data class WorkflowDefinition(
    val activities: List<ActivityDefinition>,
    val deadline: Duration = Duration.ofHours(1),
) {
    init {
        require(activities.isNotEmpty()) { "Workflow must have at least one activity" }
        require(deadline > Duration.ZERO) { "Workflow deadline must be positive" }
        val names = activities.map { it.name }
        require(names.size == names.toSet().size) {
            "Activity names must be unique, found duplicates: ${names.groupBy { it }.filter { it.value.size > 1 }.keys}"
        }
        for (activity in activities) {
            val target = activity.fanOut ?: continue
            require(activities.any { it.name == target }) {
                "Activity '${activity.name}' fanOut references unknown activity '$target'"
            }
        }
        for ((i, activity) in activities.withIndex()) {
            val target = activity.fanOut ?: continue
            require(i + 1 < activities.size && activities[i + 1].name == target) {
                "fanOut target '$target' must be the next activity after '${activity.name}'"
            }
        }
        for (activity in activities) {
            val target = activity.fanOut ?: continue
            val targetActivity = activities.first { it.name == target }
            require(targetActivity.fanOut == null) {
                "fanOut target '$target' cannot itself be a fanOut source"
            }
        }
    }
}
```

- [ ] **Step 3: Update DSL builders**

In `src/main/kotlin/dsl/WorkflowDslBuilders.kt`, delete `FanOutBuilder` class (lines 8-41). Update `ActivityBuilder`:

```kotlin
@WorkflowDsl
class ActivityBuilder {
    private var transition: String? = null
    private var retries: Int = 0
    private var failurePolicy: FailurePolicy = FailurePolicy.ABORT
    private var deadline: Duration = Duration.ofMinutes(30)
    private var fanOutTarget: String? = null
    private var joinPolicy: JoinPolicy = JoinPolicy.All
    private var backoffBase: Duration = Duration.ofSeconds(1)
    private var backoffCap: Duration = Duration.ofSeconds(300)
    private var queue: String = "default"
    private var inputsDef: Map<String, String> = emptyMap()

    fun transition(t: String) { transition = t }
    fun retries(n: Int) { retries = n }
    fun failurePolicy(p: FailurePolicy) { failurePolicy = p }
    fun deadline(d: Duration) { deadline = d }
    fun fanOut(target: String) { fanOutTarget = target }
    fun joinPolicy(p: JoinPolicy) { joinPolicy = p }
    fun backoffBase(d: Duration) { backoffBase = d }
    fun backoffCap(d: Duration) { backoffCap = d }
    fun queue(q: String) { queue = q }

    fun inputs(block: InputsBuilder.() -> Unit) {
        inputsDef = InputsBuilder().apply(block).build()
    }

    fun build(name: String): ActivityDefinition {
        requireNotNull(transition) { "Activity '$name' transition is required" }
        return ActivityDefinition(
            name = name,
            transition = transition!!,
            retries = retries,
            failurePolicy = failurePolicy,
            deadline = deadline,
            fanOut = fanOutTarget,
            joinPolicy = joinPolicy,
            backoffBase = backoffBase,
            backoffCap = backoffCap,
            queue = queue,
            inputs = inputsDef,
        )
    }
}
```

- [ ] **Step 4: Update DSL builder tests**

In `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt`, update `fan-out with Percentage join policy` test:

```kotlin
@Test
fun `fan-out with Percentage join policy`() {
    val definition = workflow {
        activity("scatter") {
            transition("scatter.dispatch")
            fanOut("parallel")
        }
        activity("parallel") {
            transition("scatter.process")
            retries(3)
            failurePolicy(FailurePolicy.BEST_EFFORT)
            deadline(Duration.ofMinutes(15))
            joinPolicy(JoinPolicy.Percentage(95))
        }
    }

    assertEquals(2, definition.activities.size)
    val scatter = definition.activities[0]
    assertEquals("scatter", scatter.name)
    assertEquals("scatter.dispatch", scatter.transition)
    assertEquals("parallel", scatter.fanOut)

    val parallel = definition.activities[1]
    assertEquals("scatter.process", parallel.transition)
    assertEquals(3, parallel.retries)
    assertEquals(FailurePolicy.BEST_EFFORT, parallel.failurePolicy)
    assertEquals(Duration.ofMinutes(15), parallel.deadline)
    assertEquals(JoinPolicy.Percentage(95), parallel.joinPolicy)
}
```

Update `fan-out with default joinPolicy when omitted` test:

```kotlin
@Test
fun `fan-out with default joinPolicy when omitted`() {
    val definition = workflow {
        activity("scatter") {
            transition("scatter.dispatch")
            fanOut("parallel")
        }
        activity("parallel") {
            transition("parallel.process")
        }
    }

    assertEquals(JoinPolicy.All, definition.activities[1].joinPolicy)
}
```

Update `DslMarker prevents calling activity inside fanOut` — delete this test entirely, it no longer applies since `fanOut` is a string, not a builder block.

Update `linear workflow with two activities` — change assertions from `assertNull(first.fanOut)` to keep the same assertion (fanOut is now `String?` so `assertNull` still works).

Add new validation tests:

```kotlin
@Test
fun `fanOut target must reference existing activity`() {
    assertThrows<IllegalArgumentException> {
        workflow {
            activity("scatter") {
                transition("scatter.handler")
                fanOut("nonexistent")
            }
        }
    }
}

@Test
fun `fanOut target must be next activity`() {
    assertThrows<IllegalArgumentException> {
        workflow {
            activity("scatter") {
                transition("scatter.handler")
                fanOut("join")
            }
            activity("parallel") { transition("parallel.handler") }
            activity("join") { transition("join.handler") }
        }
    }
}

@Test
fun `chained fanOut not allowed`() {
    assertThrows<IllegalArgumentException> {
        workflow {
            activity("scatter1") {
                transition("s1.handler")
                fanOut("scatter2")
            }
            activity("scatter2") {
                transition("s2.handler")
                fanOut("parallel")
            }
            activity("parallel") { transition("p.handler") }
        }
    }
}
```

- [ ] **Step 5: Verify DSL tests compile and pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -Dtest="WorkflowDslBuildersTest,WorkflowDslTest" test`

Expected: All DSL tests pass (some engine tests may fail due to `FanOutDefinition` removal — that's expected at this stage).

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/dsl/WorkflowDsl.kt src/main/kotlin/dsl/WorkflowDslBuilders.kt src/test/kotlin/dsl/WorkflowDslBuildersTest.kt src/test/kotlin/dsl/WorkflowDslTest.kt
git commit -m "refactor: promote fan-out to independent activity in DSL layer"
```

---

### Task 2: Sequence Map & PhaseType

**Files:**
- Modify: `src/main/kotlin/engine/SequenceModel.kt`
- Modify: `src/test/kotlin/engine/SequenceModelTest.kt`

- [ ] **Step 1: Update `PhaseType` and `buildSequenceMap`**

In `src/main/kotlin/engine/SequenceModel.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.WorkflowDefinition

enum class PhaseType { LINEAR, PARALLEL }

data class SequenceInfo(
    val sequenceNumber: Int,
    val activityIndex: Int,
    val activity: com.workflow.dsl.ActivityDefinition,
    val phaseType: PhaseType,
    val nextSequence: Int?,
    val branchSequences: Map<String, Int>? = null,
)

fun buildSequenceMap(definition: WorkflowDefinition): Map<Int, SequenceInfo> {
    val fanOutTargets = definition.activities.mapNotNull { it.fanOut }.toSet()
    val entries = definition.activities.mapIndexed { i, activity ->
        val phaseType = if (activity.name in fanOutTargets) PhaseType.PARALLEL else PhaseType.LINEAR
        Triple(i, phaseType, activity)
    }
    val map = mutableMapOf<Int, SequenceInfo>()
    for ((idx, entry) in entries.withIndex()) {
        val (activityIndex, phaseType, _) = entry
        val seq = idx + 1
        val nextSeq = if (idx + 1 < entries.size) idx + 2 else null
        map[seq] = SequenceInfo(
            sequenceNumber = seq,
            activityIndex = activityIndex,
            activity = definition.activities[activityIndex],
            phaseType = phaseType,
            nextSequence = nextSeq,
        )
    }
    return map
}
```

- [ ] **Step 2: Update sequence model tests**

In `src/test/kotlin/engine/SequenceModelTest.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
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
        assertNull(seq1.nextSequence)
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
    fun `fan-out activity is PARALLEL when referenced by another activity`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "scatter", transition = "scatter.handler", fanOut = "parallel"),
                ActivityDefinition(name = "parallel", transition = "parallel.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        val scatter = map[1]!!
        assertEquals(PhaseType.LINEAR, scatter.phaseType)
        assertEquals("scatter", scatter.activity.name)
        assertEquals(2, scatter.nextSequence)

        val parallel = map[2]!!
        assertEquals(PhaseType.PARALLEL, parallel.phaseType)
        assertEquals("parallel", parallel.activity.name)
        assertNull(parallel.nextSequence)
    }

    @Test
    fun `scatter then parallel then join produces LINEAR PARALLEL LINEAR`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "scatter", transition = "scatter.handler", fanOut = "parallel"),
                ActivityDefinition(name = "parallel", transition = "parallel.handler"),
                ActivityDefinition(name = "join", transition = "join.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(3, map.size)
        assertEquals(PhaseType.LINEAR, map[1]!!.phaseType)
        assertEquals(2, map[1]!!.nextSequence)
        assertEquals(PhaseType.PARALLEL, map[2]!!.phaseType)
        assertEquals(3, map[2]!!.nextSequence)
        assertEquals(PhaseType.LINEAR, map[3]!!.phaseType)
        assertNull(map[3]!!.nextSequence)
    }

    @Test
    fun `linear then scatter then parallel then join produces correct chain`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "step1", transition = "step1.handler"),
                ActivityDefinition(name = "scatter", transition = "scatter.handler", fanOut = "parallel"),
                ActivityDefinition(name = "parallel", transition = "parallel.handler"),
                ActivityDefinition(name = "step3", transition = "step3.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(4, map.size)
        assertEquals(PhaseType.LINEAR, map[1]!!.phaseType)
        assertEquals(2, map[1]!!.nextSequence)
        assertEquals(PhaseType.LINEAR, map[2]!!.phaseType)
        assertEquals(3, map[2]!!.nextSequence)
        assertEquals(PhaseType.PARALLEL, map[3]!!.phaseType)
        assertEquals(4, map[3]!!.nextSequence)
        assertEquals(PhaseType.LINEAR, map[4]!!.phaseType)
        assertNull(map[4]!!.nextSequence)
    }

    @Test
    fun `sequenceNumber field matches map key`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(name = "b", transition = "b.handler", fanOut = "c"),
                ActivityDefinition(name = "c", transition = "c.handler"),
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
                ActivityDefinition(name = "b", transition = "b.handler", fanOut = "c"),
                ActivityDefinition(name = "c", transition = "c.handler"),
            ),
        )
        val map = buildSequenceMap(def)
        map.values.forEach { info ->
            assertNull(info.branchSequences, "branchSequences should be null for ${info.phaseType}")
        }
    }
}
```

- [ ] **Step 3: Verify sequence model tests pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -Dtest="SequenceModelTest" test`

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/engine/SequenceModel.kt src/test/kotlin/engine/SequenceModelTest.kt
git commit -m "refactor: remove SCATTER phase type, derive PARALLEL from fanOut target"
```

---

### Task 3: Strategy Layer

**Files:**
- Modify: `src/main/kotlin/engine/PhaseStrategy.kt`
- Delete: `src/main/kotlin/engine/ScatterPhaseStrategy.kt`
- Modify: `src/main/kotlin/engine/ParallelPhaseStrategy.kt`
- Modify: `src/main/kotlin/engine/PhaseStrategyRegistry.kt`
- Delete: `src/test/kotlin/engine/ScatterPhaseStrategyTest.kt`
- Modify: `src/test/kotlin/engine/LinearPhaseStrategyTest.kt`
- Modify: `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`
- Modify: `src/test/kotlin/engine/PhaseStrategyRegistryTest.kt`

- [ ] **Step 1: Update `PhaseContext` and `AdvancementDecision`**

In `src/main/kotlin/engine/PhaseStrategy.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.WorkflowDefinition

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
)

sealed interface AdvancementDecision {
    data class Advance(val nextSequence: Int) : AdvancementDecision
    data object Complete : AdvancementDecision
    data class Abort(val reason: String) : AdvancementDecision
}

fun PhaseContext.failOrAdvance(): AdvancementDecision? {
    if (failedCount == 0) return null
    return when (currentSeqInfo.activity.failurePolicy) {
        FailurePolicy.ABORT -> AdvancementDecision.Abort(
            "$failedCount task(s) failed at sequence ${currentSeqInfo.sequenceNumber}",
        )
        FailurePolicy.BEST_EFFORT -> advanceOrComplete()
    }
}

fun PhaseContext.advanceOrComplete(): AdvancementDecision {
    val nextSeq = currentSeqInfo.nextSequence ?: return AdvancementDecision.Complete
    return AdvancementDecision.Advance(nextSeq)
}
```

- [ ] **Step 2: Delete `ScatterPhaseStrategy.kt` and its test**

Delete `src/main/kotlin/engine/ScatterPhaseStrategy.kt`.
Delete `src/test/kotlin/engine/ScatterPhaseStrategyTest.kt`.

- [ ] **Step 3: Update `ParallelPhaseStrategy`**

In `src/main/kotlin/engine/ParallelPhaseStrategy.kt`:

```kotlin
package com.workflow.engine

import com.workflow.dsl.JoinPolicy

class ParallelPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val joinPolicy = context.currentSeqInfo.activity.joinPolicy
        val succeeded = evaluateJoinPolicy(joinPolicy, context.failedCount, context.totalCount)

        if (!succeeded) {
            context.failOrAdvance()?.let { return it }
        }

        return context.advanceOrComplete()
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

- [ ] **Step 4: Update `PhaseStrategyRegistry`**

In `src/main/kotlin/engine/PhaseStrategyRegistry.kt`:

```kotlin
package com.workflow.engine

import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class PhaseStrategyRegistry {

    private val strategies = ConcurrentHashMap<PhaseType, PhaseStrategy>()

    init {
        register(PhaseType.LINEAR, LinearPhaseStrategy())
        register(PhaseType.PARALLEL, ParallelPhaseStrategy())
    }

    fun register(type: PhaseType, strategy: PhaseStrategy) {
        strategies[type] = strategy
    }

    fun resolve(type: PhaseType): PhaseStrategy =
        strategies[type] ?: throw IllegalStateException("No strategy registered for phase type: $type")
}
```

Note: The `ObjectMapper` constructor parameter is removed since `ScatterPhaseStrategy` no longer exists.

- [ ] **Step 5: Update `LinearPhaseStrategyTest`**

In `src/test/kotlin/engine/LinearPhaseStrategyTest.kt`, remove `tasks` from `PhaseContext` construction and `advance.tasks` assertions:

```kotlin
package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.WorkflowDefinition
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class LinearPhaseStrategyTest {

    private val strategy = LinearPhaseStrategy()
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun activity(name: String = "step1", failurePolicy: FailurePolicy = FailurePolicy.ABORT) =
        ActivityDefinition(name = name, transition = "$name.handler", failurePolicy = failurePolicy)

    private fun context(
        activity: ActivityDefinition = activity(),
        nextSequence: Int? = 2,
        failedCount: Int = 0,
        totalCount: Int = 1,
    ): PhaseContext {
        val seqInfo = SequenceInfo(1, 0, activity, PhaseType.LINEAR, nextSequence)
        val nextAct = ActivityDefinition(name = "step2", transition = "step2.handler")
        val sequenceMap = mutableMapOf(1 to seqInfo)
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 1, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(activity, nextAct))
        val wf = WorkflowRun("wf1", "{}", 1, 0, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, seqInfo, sequenceMap, failedCount, totalCount)
    }

    @Test
    fun `success with next sequence returns Advance`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(2, advance.nextSequence)
    }

    @Test
    fun `success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with ABORT returns Abort`() {
        val ctx = context(failedCount = 1)
        val fail = assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
        assert(fail.reason.contains("1 task(s) failed"))
    }

    @Test
    fun `failure with BEST_EFFORT advances to next sequence`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            failedCount = 1,
        )
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(2, advance.nextSequence)
    }

    @Test
    fun `failure with BEST_EFFORT at last sequence returns Complete`() {
        val ctx = context(
            activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
            nextSequence = null,
            failedCount = 1,
        )
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }
}
```

- [ ] **Step 6: Update `ParallelPhaseStrategyTest`**

In `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`, replace `FanOutDefinition` with `joinPolicy` on `ActivityDefinition` and remove `tasks`/`advance.tasks` references:

```kotlin
package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ParallelPhaseStrategyTest {

    private val strategy = ParallelPhaseStrategy()
    private val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun context(
        joinPolicy: JoinPolicy = JoinPolicy.All,
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
        nextSequence: Int? = 3,
        failedCount: Int = 0,
        totalCount: Int = 3,
    ): PhaseContext {
        val scatterAct = ActivityDefinition(
            name = "scatter", transition = "scatter.handler", fanOut = "parallel",
        )
        val parallelAct = ActivityDefinition(
            name = "parallel", transition = "parallel.handler",
            failurePolicy = failurePolicy, joinPolicy = joinPolicy,
        )
        val nextAct = ActivityDefinition(name = "final", transition = "final.handler")
        val parallelSeq = SequenceInfo(2, 1, parallelAct, PhaseType.PARALLEL, nextSequence)
        val sequenceMap = mutableMapOf(
            1 to SequenceInfo(1, 0, scatterAct, PhaseType.LINEAR, 2),
            2 to parallelSeq,
        )
        if (nextSequence != null) {
            sequenceMap[nextSequence] = SequenceInfo(nextSequence, 2, nextAct, PhaseType.LINEAR, null)
        }
        val def = WorkflowDefinition(activities = listOf(scatterAct, parallelAct, nextAct))
        val wf = WorkflowRun("wf1", "{}", 2, 1, WorkflowStatus.RUNNING, now, now, now.plus(Duration.ofHours(1)))
        return PhaseContext(wf, def, parallelSeq, sequenceMap, failedCount, totalCount)
    }

    @Test
    fun `JoinPolicy All success advances to next sequence`() {
        val decision = strategy.resolve(context())
        val advance = assertIs<AdvancementDecision.Advance>(decision)
        assertEquals(3, advance.nextSequence)
    }

    @Test
    fun `JoinPolicy All success at last sequence returns Complete`() {
        val ctx = context(nextSequence = null)
        assertIs<AdvancementDecision.Complete>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy All with failure returns Abort`() {
        val ctx = context(failedCount = 1, totalCount = 3)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold success when enough tasks succeed`() {
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 1, totalCount = 3)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Threshold failure when not enough succeed`() {
        val ctx = context(joinPolicy = JoinPolicy.Threshold(2), failedCount = 2, totalCount = 3)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage success at boundary`() {
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 5, totalCount = 100)
        assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    }

    @Test
    fun `JoinPolicy Percentage failure below boundary`() {
        val ctx = context(joinPolicy = JoinPolicy.Percentage(95), failedCount = 6, totalCount = 100)
        assertIs<AdvancementDecision.Abort>(strategy.resolve(ctx))
    }

    @Test
    fun `failure with BEST_EFFORT advances to next sequence`() {
        val ctx = context(failedCount = 1, totalCount = 2, failurePolicy = FailurePolicy.BEST_EFFORT)
        val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
        assertEquals(3, advance.nextSequence)
    }
}
```

- [ ] **Step 7: Update `PhaseStrategyRegistryTest`**

Remove SCATTER references. Update to match new constructor (no `ObjectMapper`):

```kotlin
// Remove any test that registers/resolves PhaseType.SCATTER
// Update registry instantiation: PhaseStrategyRegistry() instead of PhaseStrategyRegistry(objectMapper)
```

- [ ] **Step 8: Verify strategy tests pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -Dtest="LinearPhaseStrategyTest,ParallelPhaseStrategyTest,PhaseStrategyRegistryTest" test`

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: remove ScatterPhaseStrategy, strategies use counts only"
```

---

### Task 4: BarrierService & SQL Fan-Out

**Files:**
- Modify: `src/main/kotlin/engine/BarrierService.kt`
- Modify: `src/main/kotlin/engine/TaskRepository.kt`

- [ ] **Step 1: Add `insertFanOutFromScatter` to `TaskRepository`**

Add this method to `TaskRepository`:

```kotlin
fun insertFanOutFromScatter(
    handle: Handle,
    workflowId: String,
    scatterSequence: Int,
    targetSeqInfo: SequenceInfo,
    now: Instant,
) {
    val activity = targetSeqInfo.activity
    val deadlineAt = LocalDateTime.ofInstant(now.plus(activity.deadline), ZoneOffset.UTC)
        .truncatedTo(java.time.temporal.ChronoUnit.MICROS)
    handle.createUpdate(
        """
        INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, item,
                          result, claimed_by, claimed_at, completed_at,
                          retry_count, max_retries, deadline_at, not_before,
                          backoff_base, backoff_cap, queue_name)
        SELECT SYS_GUID(), :workflowId, :nextSeq, 'PENDING', :handlerKey,
               jt.item,
               NULL, NULL, NULL, NULL,
               0, :maxRetries, :deadlineAt, NULL,
               :backoffBase, :backoffCap, :queueName
        FROM task t
        CROSS JOIN JSON_TABLE(t.result, '$[*]' COLUMNS (item CLOB PATH '$')) jt
        WHERE t.workflow_id = :workflowId
          AND t.sequence_number = :scatterSeq
          AND t.status = 'COMPLETED'
        """,
    )
        .bind("workflowId", workflowId)
        .bind("nextSeq", targetSeqInfo.sequenceNumber)
        .bind("handlerKey", activity.transition)
        .bind("maxRetries", activity.retries)
        .bind("deadlineAt", deadlineAt)
        .bind("scatterSeq", scatterSequence)
        .bind("backoffBase", activity.backoffBase.seconds.toInt())
        .bind("backoffCap", activity.backoffCap.seconds.toInt())
        .bind("queueName", activity.queue)
        .execute()
}
```

- [ ] **Step 2: Update `BarrierService`**

Rewrite `evaluateAndAdvance`, `resolveAndExecute`, and `executeDecision`:

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
import java.time.temporal.ChronoUnit

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
            val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
            if (!updated) return@inTransactionSuspend

            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

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
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, seq)
            if (nonTerminal > 0) return@inTransactionSuspend

            val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, seq)
            val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, seq)
            resolveAndExecute(handle, workflow, seq, failedCount, totalCount)
        }
    }

    private fun evaluateAndAdvance(handle: Handle, workflowId: String, sequenceNumber: Int) {
        val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
            ?: throw IllegalStateException("Workflow not found: $workflowId")
        if (workflow.status != WorkflowStatus.RUNNING) return
        if (sequenceNumber != workflow.currentSequence) return

        val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
        val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)

        resolveAndExecute(handle, workflow, sequenceNumber, failedCount, totalCount)
    }

    private fun resolveAndExecute(
        handle: Handle,
        workflow: WorkflowRun,
        sequenceNumber: Int,
        failedCount: Int,
        totalCount: Int,
    ) {
        val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
        val sequenceMap = buildSequenceMap(definition)
        val seqInfo = sequenceMap[sequenceNumber]
            ?: throw IllegalStateException("Sequence $sequenceNumber not in definition for workflow ${workflow.id}")

        val strategy = strategyRegistry.resolve(seqInfo.phaseType)
        val context = PhaseContext(workflow, definition, seqInfo, sequenceMap, failedCount, totalCount)
        val decision = strategy.resolve(context)

        executeDecision(handle, workflow, seqInfo, sequenceMap, decision)
    }

    private fun executeDecision(
        handle: Handle,
        workflow: WorkflowRun,
        seqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
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
                val nextSeqInfo = sequenceMap[decision.nextSequence]!!
                val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
                when (nextSeqInfo.phaseType) {
                    PhaseType.PARALLEL -> taskRepo.insertFanOutFromScatter(
                        handle, workflow.id, seqInfo.sequenceNumber, nextSeqInfo, now,
                    )
                    PhaseType.LINEAR -> taskRepo.insertBatchWithHandle(
                        handle, listOf(createTaskForActivity(workflow.id, nextSeqInfo.sequenceNumber, nextSeqInfo.activity, now)),
                    )
                }
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

- [ ] **Step 3: Verify compilation**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -q`

Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/engine/BarrierService.kt src/main/kotlin/engine/TaskRepository.kt
git commit -m "refactor: barrier uses counts only, fan-out via SQL INSERT...SELECT"
```

---

### Task 5: InputResolver

**Files:**
- Modify: `src/main/kotlin/engine/InputResolver.kt`
- Modify: `src/test/kotlin/engine/InputResolverTest.kt`

- [ ] **Step 1: Update `InputResolver.resolveActivity`**

Replace the `isFanOut` check with `PhaseType`-based dispatch. Delete `findParallelSequence`:

```kotlin
package com.workflow.engine

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory

@ApplicationScoped
class InputResolver(
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(InputResolver::class.java)

    suspend fun resolve(
        inputs: Map<String, String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: suspend (Int) -> List<Task>,
    ): String? {
        if (inputs.isEmpty()) return null

        val resultNode = objectMapper.createObjectNode()

        for ((inputName, ref) in inputs) {
            val (activityName, fieldPath) = parseRef(ref)
            val resolved = resolveActivity(activityName, fieldPath, sequenceMap, tasksBySequence)
            resultNode.set<JsonNode>(inputName, resolved)
        }

        return objectMapper.writeValueAsString(resultNode)
    }

    private fun parseRef(ref: String): Pair<String, List<String>> {
        val parts = ref.split('.')
        return parts.first() to parts.drop(1)
    }

    private suspend fun resolveActivity(
        activityName: String,
        fieldPath: List<String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: suspend (Int) -> List<Task>,
    ): JsonNode {
        val seqEntry = sequenceMap.values.firstOrNull { it.activity.name == activityName }
            ?: throw IllegalArgumentException(
                "Input reference '$activityName' does not match any activity in the workflow. " +
                    "Available activities: ${sequenceMap.values.map { it.activity.name }}"
            )

        return when (seqEntry.phaseType) {
            PhaseType.PARALLEL -> {
                val tasks = tasksBySequence(seqEntry.sequenceNumber)
                    .filter { it.status == TaskStatus.COMPLETED }
                aggregateFanOut(tasks, fieldPath)
            }
            PhaseType.LINEAR -> {
                val task = tasksBySequence(seqEntry.sequenceNumber)
                    .firstOrNull { it.status == TaskStatus.COMPLETED }
                val resultJson = task?.resultJson
                if (resultJson == null) return objectMapper.nullNode()
                val resultTree = objectMapper.readTree(resultJson)
                traversePath(resultTree, fieldPath)
            }
        }
    }

    private fun traversePath(node: JsonNode, fieldPath: List<String>): JsonNode {
        var current = node
        for (key in fieldPath) {
            current = current.path(key)
            if (current.isMissingNode) {
                log.warn("Field path segment '{}' not found in result. Full path: {}", key, fieldPath.joinToString("."))
                return current
            }
        }
        return current
    }

    private fun aggregateFanOut(
        tasks: List<Task>,
        fieldPath: List<String>,
    ): ArrayNode {
        val arrayNode = objectMapper.createArrayNode()
        for (task in tasks) {
            val resultJson = task.resultJson ?: continue
            val resultTree = objectMapper.readTree(resultJson)
            arrayNode.add(traversePath(resultTree, fieldPath))
        }
        return arrayNode
    }
}
```

- [ ] **Step 2: Update `InputResolverTest`**

Update `fanOutSequenceMap` helper and tests to use the new model (separate activities, no `FanOutDefinition`, no `PhaseType.SCATTER`):

```kotlin
private fun fanOutSequenceMap(): Map<Int, SequenceInfo> {
    val scatterAct = ActivityDefinition(
        name = "scatter", transition = "scatter.handler", fanOut = "split",
    )
    val splitAct = ActivityDefinition(name = "split", transition = "parallel.handler")
    val notifyAct = ActivityDefinition(name = "notify", transition = "notify.handler")
    return mapOf(
        1 to SequenceInfo(1, 0, scatterAct, PhaseType.LINEAR, 2),
        2 to SequenceInfo(2, 1, splitAct, PhaseType.PARALLEL, 3),
        3 to SequenceInfo(3, 2, notifyAct, PhaseType.LINEAR, null),
    )
}
```

Update all fan-out tests to use `"split"` (the PARALLEL activity name) instead of `"split"` (the scatter activity name) for aggregation references. Since the scatter activity now has `PhaseType.LINEAR`, referencing `"scatter"` fetches 1 task. Referencing `"split"` aggregates from PARALLEL.

In the test `whole-result reference from fan-out activity aggregates parallel results`, change the input reference from `"split"` to `"split"`:

```kotlin
@Test
fun `whole-result reference from fan-out activity aggregates parallel results`() = runTest {
    val inputs = mapOf("results" to "split")
    val tasksBySeq: suspend (Int) -> List<Task> = { seq ->
        if (seq == 2) listOf(
            task(2, resultJson = """{"r":"one"}"""),
            task(2, resultJson = """{"r":"two"}"""),
        ) else emptyList()
    }
    val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
    val parsed = objectMapper.readTree(result)
    val arr = parsed.get("results")
    assertEquals(2, arr.size())
    assertEquals("one", arr[0].get("r").asText())
    assertEquals("two", arr[1].get("r").asText())
}
```

Add a new test for referencing the scatter activity (LINEAR, single result):

```kotlin
@Test
fun `reference to scatter activity returns single result, not aggregation`() = runTest {
    val inputs = mapOf("token" to "scatter.batchId")
    val tasksBySeq: suspend (Int) -> List<Task> = { seq ->
        if (seq == 1) listOf(task(1, resultJson = """{"batchId":"batch-123"}"""))
        else emptyList()
    }
    val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
    val parsed = objectMapper.readTree(result)
    assertEquals("batch-123", parsed.get("token").asText())
}
```

Remove the `FanOutDefinition` import from the test file.

- [ ] **Step 3: Verify InputResolver tests pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -Dtest="InputResolverTest" test`

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/engine/InputResolver.kt src/test/kotlin/engine/InputResolverTest.kt
git commit -m "refactor: InputResolver uses PhaseType for single vs aggregate resolution"
```

---

### Task 6: Update Remaining Tests

**Files:**
- Modify: `src/test/kotlin/engine/BarrierServiceTest.kt`
- Modify: `src/test/kotlin/engine/WorkflowIntegrationTest.kt`
- Modify: `src/test/kotlin/worker/WorkerLoopTest.kt`
- Modify: any stress tests referencing `FanOutDefinition`

- [ ] **Step 1: Find all remaining references to deleted types**

Search for `FanOutDefinition`, `FanOutBuilder`, `PhaseType.SCATTER`, `ScatterPhaseStrategy`, `advance.tasks`, and `decision.tasks` across all test files and fix each reference.

Key patterns to fix:
- `FanOutDefinition(transition = ...)` → split into separate `ActivityDefinition` for the parallel activity
- `PhaseType.SCATTER` → `PhaseType.LINEAR`
- `fanOut = FanOutDefinition(...)` → `fanOut = "target-name"`
- `advance.tasks.size` / `advance.tasks[0]` → remove these assertions (tasks no longer on decision)
- `PhaseContext(..., tasks.size, tasks)` → `PhaseContext(..., failedCount, totalCount)` (6 args, no task list)
- `PhaseStrategyRegistry(objectMapper)` → `PhaseStrategyRegistry()`
- Workflow definitions using the old DSL: `fanOut { transition("...") }` → two separate activities with `fanOut("target")`

- [ ] **Step 2: Update `BarrierServiceTest`**

This is the largest test file. Key changes:
- All workflow definitions using the old `fanOut { }` block must use two separate activities
- `PhaseStrategyRegistry(objectMapper)` → `PhaseStrategyRegistry()`
- Assertions about fan-out task counts should verify tasks in DB rather than in the decision object
- The scatter→parallel flow now produces different sequence numbers (1 activity = 1 sequence, not 2)

- [ ] **Step 3: Update `WorkflowIntegrationTest`**

Similar changes — workflow definitions using old DSL must be updated to new format.

- [ ] **Step 4: Update stress tests**

Search `src/test/kotlin/stress/` for `FanOutDefinition` references and update.

- [ ] **Step 5: Full test suite verification**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test`

Expected: All tests pass. If any fail, investigate and fix.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: update all tests for independent fan-out model"
```

---

### Task 7: Final Verification & Cleanup

**Files:**
- All modified files

- [ ] **Step 1: Verify no references to deleted types remain**

Search for: `FanOutDefinition`, `FanOutBuilder`, `PhaseType.SCATTER`, `ScatterPhaseStrategy`, `ScatterPhaseStrategyTest`

Expected: Zero matches.

- [ ] **Step 2: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test`

Expected: BUILD SUCCESS with all tests passing.

- [ ] **Step 3: Run coverage check**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`

Expected: Coverage meets thresholds.

- [ ] **Step 4: Commit any final fixes**

```bash
git add -A
git commit -m "chore: cleanup after independent fan-out refactor"
```
