# Explicit Inputs & Item Column Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace implicit payload auto-forwarding with explicit DSL-declared inputs and a scatter-only `item` field, following the Kestra-style data-passing pattern.

**Architecture:** Remove `payloadJson` from `Task`, add `item: String?` for scatter→parallel only. Add `inputs: Map<String, String>` to `ActivityDefinition`. New `InputResolver` resolves declared inputs at worker execution time (not barrier time). Phase strategies become pure state-machine logic with no payload forwarding.

**Tech Stack:** Kotlin, JDBI, Jackson, Oracle, JUnit 5, Mockito-Kotlin

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/main/kotlin/dsl/WorkflowDsl.kt` | Add `inputs` field to `ActivityDefinition` |
| `src/main/kotlin/dsl/WorkflowDslBuilders.kt` | Add `InputsBuilder` + `inputs {}` block to `ActivityBuilder` |
| `src/main/kotlin/engine/WorkflowModels.kt` | Replace `payloadJson` with `item` on `Task`, update `createTaskForActivity` |
| `src/main/kotlin/engine/PhaseStrategy.kt` | Remove `payload` param from `failOrAdvance`/`advanceOrComplete` |
| `src/main/kotlin/engine/LinearPhaseStrategy.kt` | Remove payload forwarding |
| `src/main/kotlin/engine/ScatterPhaseStrategy.kt` | Use `item` instead of `payloadJson` for parallel tasks |
| `src/main/kotlin/engine/ParallelPhaseStrategy.kt` | Remove result aggregation and payload forwarding |
| `src/main/kotlin/engine/InputResolver.kt` | NEW: resolve declared inputs from previous activities' resultJson |
| `src/main/kotlin/engine/TaskRepository.kt` | Update SQL: replace `payload` with `item` in insert/map |
| `src/main/kotlin/engine/WorkflowEngine.kt` | Update `startWorkflow` to not pass payload |
| `src/main/kotlin/worker/TransitionHandler.kt` | Update `HandlerInput` to carry `inputs` + `item` instead of `payload` |
| `src/main/kotlin/worker/WorkerLoop.kt` | Add InputResolver call before handler invocation |
| `src/main/resources/db/migration/V8__explicit_inputs.sql` | Add `item CLOB`, drop `payload CLOB` |
| `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt` | NEW: test `inputs {}` DSL block |
| `src/test/kotlin/engine/InputResolverTest.kt` | NEW: test resolution logic |
| `src/test/kotlin/engine/WorkflowModelsTest.kt` | Update Task tests for `item` replacing `payloadJson` |
| `src/test/kotlin/engine/LinearPhaseStrategyTest.kt` | Update for no-payload signatures |
| `src/test/kotlin/engine/ScatterPhaseStrategyTest.kt` | Update: assert `item` instead of `payloadJson` |
| `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt` | Update: remove aggregated payload assertions |
| `src/test/kotlin/engine/BarrierServiceTest.kt` | Update: remove `payloadJson` from helpers |
| `src/test/kotlin/engine/WorkflowIntegrationTest.kt` | Update: remove `payloadJson` references |
| `src/test/kotlin/worker/WorkerLoopTest.kt` | Update: new handler input contract |

---

### Task 1: DSL — Add `inputs` to ActivityDefinition and InputsBuilder

**Files:**
- Modify: `src/main/kotlin/dsl/WorkflowDsl.kt:42-52`
- Modify: `src/main/kotlin/dsl/WorkflowDslBuilders.kt:43-80`
- Create: `src/test/kotlin/dsl/WorkflowDslBuildersTest.kt`

- [ ] **Step 1: Write failing tests for the inputs DSL**

```kotlin
// src/test/kotlin/dsl/WorkflowDslBuildersTest.kt
package com.workflow.dsl

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkflowDslBuildersTest {

    @Test
    fun `activity with no inputs has empty inputs map`() {
        val def = workflow {
            activity("step1") {
                transition("step1.handler")
            }
        }
        assertTrue(def.activities[0].inputs.isEmpty())
    }

    @Test
    fun `activity with field-level inputs`() {
        val def = workflow {
            activity("notify") {
                transition("notify.handler")
                inputs {
                    "chunks" from "split.uri"
                    "count" from "split.total"
                }
            }
        }
        val inputs = def.activities[0].inputs
        assertEquals(2, inputs.size)
        assertEquals("split.uri", inputs["chunks"])
        assertEquals("split.total", inputs["count"])
    }

    @Test
    fun `activity with whole-result input`() {
        val def = workflow {
            activity("aggregate") {
                transition("agg.handler")
                inputs {
                    "data" from "split"
                }
            }
        }
        assertEquals("split", def.activities[0].inputs["data"])
    }

    @Test
    fun `inputs from multiple activities`() {
        val def = workflow {
            activity("final") {
                transition("final.handler")
                inputs {
                    "a" from "step1.field"
                    "b" from "step2"
                }
            }
        }
        val inputs = def.activities[0].inputs
        assertEquals("step1.field", inputs["a"])
        assertEquals("step2", inputs["b"])
    }

    @Test
    fun `inputs serializes correctly via Jackson`() {
        val objectMapper = com.fasterxml.jackson.databind.ObjectMapper()
            .registerModule(com.fasterxml.jackson.module.kotlin.KotlinModule.Builder().build())
        val def = workflow {
            activity("step") {
                transition("s.handler")
                inputs {
                    "x" from "prev.field"
                }
            }
        }
        val json = objectMapper.writeValueAsString(def)
        val restored = objectMapper.readValue(json, WorkflowDefinition::class.java)
        assertEquals("prev.field", restored.activities[0].inputs["x"])
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowDslBuildersTest" -pl WorkFlow -f pom.xml`
Expected: Compilation errors — `inputs` does not exist on `ActivityDefinition`, `InputsBuilder` does not exist

- [ ] **Step 3: Add `inputs` field to ActivityDefinition**

In `src/main/kotlin/dsl/WorkflowDsl.kt`, add `inputs` to the data class:

```kotlin
data class ActivityDefinition(
    val name: String,
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val fanOut: FanOutDefinition? = null,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
    val inputs: Map<String, String> = emptyMap(),
)
```

- [ ] **Step 4: Add InputsBuilder and wire into ActivityBuilder**

In `src/main/kotlin/dsl/WorkflowDslBuilders.kt`, add:

```kotlin
@WorkflowDsl
class InputsBuilder {
    private val entries = mutableMapOf<String, String>()

    infix fun String.from(ref: String) {
        entries[this] = ref
    }

    fun build(): Map<String, String> = entries.toMap()
}
```

Then in `ActivityBuilder`, add the `inputs` field and DSL function:

```kotlin
// Add to ActivityBuilder's private fields:
private var inputsDef: Map<String, String> = emptyMap()

// Add DSL function:
fun inputs(block: InputsBuilder.() -> Unit) {
    inputsDef = InputsBuilder().apply(block).build()
}

// Update build() to include inputs:
fun build(name: String): ActivityDefinition {
    requireNotNull(transition) { "Activity '$name' transition is required" }
    return ActivityDefinition(
        name = name,
        transition = transition!!,
        retries = retries,
        failurePolicy = failurePolicy,
        deadline = deadline,
        fanOut = fanOutDef,
        backoffBase = backoffBase,
        backoffCap = backoffCap,
        queue = queue,
        inputs = inputsDef,
    )
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowDslBuildersTest" -pl WorkFlow -f pom.xml`
Expected: All 5 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/dsl/WorkflowDsl.kt src/main/kotlin/dsl/WorkflowDslBuilders.kt src/test/kotlin/dsl/WorkflowDslBuildersTest.kt
git commit -m "feat: add inputs DSL to ActivityDefinition"
```

---

### Task 2: Data Model — Replace `payloadJson` with `item` on Task

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowModels.kt:74-120`
- Modify: `src/test/kotlin/engine/WorkflowModelsTest.kt`

- [ ] **Step 1: Update Task data class**

In `src/main/kotlin/engine/WorkflowModels.kt`, replace `payloadJson` with `item`:

```kotlin
data class Task(
    val id: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val status: TaskStatus,
    val handlerKey: String,
    val item: String? = null,
    val resultJson: String?,
    val claimedBy: String?,
    val claimedAt: Instant?,
    val completedAt: Instant?,
    val retryCount: Int,
    val maxRetries: Int,
    val deadlineAt: Instant?,
    val notBefore: Instant? = null,
    val backoffBase: Int = 1,
    val backoffCap: Int = 300,
    val enqueuedAt: Instant = Instant.EPOCH,
    val queueName: String = "default",
)
```

- [ ] **Step 2: Update `createTaskForActivity` to remove payload parameter**

Replace the `createTaskForActivity` function:

```kotlin
internal fun createTaskForActivity(
    workflowId: String,
    sequenceNumber: Int,
    activity: ActivityDefinition,
    now: Instant,
): Task {
    return Task(
        id = UUID.randomUUID().toString(),
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = TaskStatus.PENDING,
        handlerKey = activity.transition,
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

- [ ] **Step 3: Update WorkflowModelsTest**

Replace all `payloadJson` references with `item` in `src/test/kotlin/engine/WorkflowModelsTest.kt`:

In the `task()` helper function:
```kotlin
private fun task(
    id: String = "task-1",
    workflowId: String = "wf-1",
    sequenceNumber: Int = 1,
    status: TaskStatus = TaskStatus.PENDING,
    handlerKey: String = "process.step1",
    item: String? = null,
    resultJson: String? = null,
    claimedBy: String? = null,
    claimedAt: Instant? = null,
    completedAt: Instant? = null,
    retryCount: Int = 0,
    maxRetries: Int = 3,
    deadlineAt: Instant? = later,
) = Task(
    id, workflowId, sequenceNumber, status, handlerKey,
    item, resultJson, claimedBy, claimedAt, completedAt,
    retryCount, maxRetries, deadlineAt,
)
```

In `Task construction preserves all fields`:
```kotlin
@Test
fun `Task construction preserves all fields`() {
    val t = task(item = """{"key":"value"}""")
    assertEquals("task-1", t.id)
    assertEquals("wf-1", t.workflowId)
    assertEquals(1, t.sequenceNumber)
    assertEquals(TaskStatus.PENDING, t.status)
    assertEquals("process.step1", t.handlerKey)
    assertEquals("""{"key":"value"}""", t.item)
    assertNull(t.resultJson)
    assertNull(t.claimedBy)
    assertNull(t.claimedAt)
    assertNull(t.completedAt)
    assertEquals(0, t.retryCount)
    assertEquals(3, t.maxRetries)
    assertEquals(later, t.deadlineAt)
}
```

In `Task with all nullable fields null`:
```kotlin
@Test
fun `Task with all nullable fields null`() {
    val t = task(
        item = null,
        resultJson = null,
        claimedBy = null,
        claimedAt = null,
        completedAt = null,
        deadlineAt = null,
    )
    assertNull(t.item)
    assertNull(t.resultJson)
    assertNull(t.claimedBy)
    assertNull(t.claimedAt)
    assertNull(t.completedAt)
    assertNull(t.deadlineAt)
}
```

In `Task with all nullable fields populated`:
```kotlin
@Test
fun `Task with all nullable fields populated`() {
    val t = task(
        item = """{"data":1}""",
        resultJson = """{"result":"ok"}""",
        claimedBy = "worker-1",
        claimedAt = now,
        completedAt = later,
        deadlineAt = later,
    )
    assertEquals("""{"data":1}""", t.item)
    assertEquals("""{"result":"ok"}""", t.resultJson)
    assertEquals("worker-1", t.claimedBy)
    assertEquals(now, t.claimedAt)
    assertEquals(later, t.completedAt)
    assertEquals(later, t.deadlineAt)
}
```

- [ ] **Step 4: Run WorkflowModelsTest to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow -f pom.xml`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/WorkflowModels.kt src/test/kotlin/engine/WorkflowModelsTest.kt
git commit -m "refactor: replace payloadJson with item on Task"
```

---

### Task 3: Phase Strategy — Remove payload from shared interface

**Files:**
- Modify: `src/main/kotlin/engine/PhaseStrategy.kt:36-61`
- Modify: `src/test/kotlin/engine/LinearPhaseStrategyTest.kt`

- [ ] **Step 1: Update `failOrAdvance` and `advanceOrComplete` to remove payload parameter**

In `src/main/kotlin/engine/PhaseStrategy.kt`:

```kotlin
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
    val nextSeqInfo = sequenceMap[nextSeq]!!
    val task = createTaskForActivity(
        workflowId = workflow.id,
        sequenceNumber = nextSeq,
        activity = nextSeqInfo.activity,
        now = Instant.now().truncatedTo(ChronoUnit.MICROS),
    )
    return AdvancementDecision.Advance(nextSeq, listOf(task))
}
```

- [ ] **Step 2: Update LinearPhaseStrategy**

In `src/main/kotlin/engine/LinearPhaseStrategy.kt`:

```kotlin
class LinearPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        context.failOrAdvance()?.let { return it }
        return context.advanceOrComplete()
    }
}
```

- [ ] **Step 3: Update LinearPhaseStrategyTest**

In `src/test/kotlin/engine/LinearPhaseStrategyTest.kt`:

Update the `task()` helper — replace `payloadJson` with `item`:
```kotlin
private fun task(
    status: TaskStatus = TaskStatus.COMPLETED,
    resultJson: String? = null,
) = Task(
    id = "t1", workflowId = "wf1", sequenceNumber = 1, status = status,
    handlerKey = "step1.handler", resultJson = resultJson,
    claimedBy = null, claimedAt = null, completedAt = null,
    retryCount = 0, maxRetries = 0, deadlineAt = null,
)
```

Remove test `success propagates resultJson as next task payload` (this behavior no longer exists — payload forwarding is removed).

Update `failure with BEST_EFFORT returns Advance with null payload` — remove the `assertNull(advance.tasks[0].payloadJson)` line, just assert it advances:
```kotlin
@Test
fun `failure with BEST_EFFORT advances to next sequence`() {
    val ctx = context(
        activity = activity(failurePolicy = FailurePolicy.BEST_EFFORT),
        failedCount = 1,
        tasks = listOf(task(status = TaskStatus.FAILED)),
    )
    val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    assertEquals(2, advance.nextSequence)
}
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="LinearPhaseStrategyTest" -pl WorkFlow -f pom.xml`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/PhaseStrategy.kt src/main/kotlin/engine/LinearPhaseStrategy.kt src/test/kotlin/engine/LinearPhaseStrategyTest.kt
git commit -m "refactor: remove payload from phase strategy interface"
```

---

### Task 4: ScatterPhaseStrategy — Use `item` instead of `payloadJson`

**Files:**
- Modify: `src/main/kotlin/engine/ScatterPhaseStrategy.kt`
- Modify: `src/test/kotlin/engine/ScatterPhaseStrategyTest.kt`

- [ ] **Step 1: Update ScatterPhaseStrategy**

In `src/main/kotlin/engine/ScatterPhaseStrategy.kt`, change `payloadJson = payload` to `item = item` and update `failOrAdvance`:

```kotlin
class ScatterPhaseStrategy(
    private val objectMapper: ObjectMapper,
) : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        context.failOrAdvance()?.let { return it }

        val scatterTask = context.tasks.firstOrNull { it.status == TaskStatus.COMPLETED }
            ?: return AdvancementDecision.Abort("No completed scatter task at sequence ${context.currentSeqInfo.sequenceNumber}")
        val scatterResult = scatterTask.resultJson
            ?: return AdvancementDecision.Abort("Scatter task ${scatterTask.id} has no result")

        val items: List<String> = objectMapper.readValue(scatterResult)
        val parallelSeq = context.currentSeqInfo.nextSequence!!
        val parallelSeqInfo = context.sequenceMap[parallelSeq]!!
        val fanOut = parallelSeqInfo.activity.fanOut
            ?: throw IllegalStateException("SCATTER phase at seq ${context.currentSeqInfo.sequenceNumber} points to PARALLEL seqInfo with no fanOut definition")
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

        val tasks = items.map { item ->
            Task(
                id = UUID.randomUUID().toString(),
                workflowId = context.workflow.id,
                sequenceNumber = parallelSeq,
                status = TaskStatus.PENDING,
                handlerKey = fanOut.transition,
                item = item,
                resultJson = null,
                claimedBy = null,
                claimedAt = null,
                completedAt = null,
                retryCount = 0,
                maxRetries = fanOut.retries,
                deadlineAt = now.plus(fanOut.deadline),
                backoffBase = fanOut.backoffBase.seconds.toInt(),
                backoffCap = fanOut.backoffCap.seconds.toInt(),
                queueName = fanOut.queue,
            )
        }
        return AdvancementDecision.Advance(parallelSeq, tasks)
    }
}
```

- [ ] **Step 2: Update ScatterPhaseStrategyTest**

In `src/test/kotlin/engine/ScatterPhaseStrategyTest.kt`:

Update `scatterTask` helper — remove `payloadJson`:
```kotlin
private fun scatterTask(
    status: TaskStatus = TaskStatus.COMPLETED,
    resultJson: String? = null,
) = Task(
    id = "t1", workflowId = "wf1", sequenceNumber = 1, status = status,
    handlerKey = "scatter.handler", resultJson = resultJson,
    claimedBy = null, claimedAt = null, completedAt = null,
    retryCount = 0, maxRetries = 0, deadlineAt = null,
)
```

Update `success creates fan-out tasks from scatter result` — assert `item` instead of `payloadJson`:
```kotlin
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
    assertEquals("a", advance.tasks[0].item)
    assertEquals("b", advance.tasks[1].item)
    assertEquals("c", advance.tasks[2].item)
}
```

Update `failure with BEST_EFFORT` test — remove `assertNull(advance.tasks[0].payloadJson)`:
```kotlin
@Test
fun `failure with BEST_EFFORT advances to next sequence`() {
    val ctx = context(
        failedCount = 1,
        tasks = listOf(scatterTask(status = TaskStatus.FAILED)),
        failurePolicy = FailurePolicy.BEST_EFFORT,
    )
    val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    assertEquals(1, advance.tasks.size)
}
```

- [ ] **Step 3: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ScatterPhaseStrategyTest" -pl WorkFlow -f pom.xml`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/engine/ScatterPhaseStrategy.kt src/test/kotlin/engine/ScatterPhaseStrategyTest.kt
git commit -m "refactor: scatter strategy uses item instead of payloadJson"
```

---

### Task 5: ParallelPhaseStrategy — Remove aggregation and payload forwarding

**Files:**
- Modify: `src/main/kotlin/engine/ParallelPhaseStrategy.kt`
- Modify: `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`

- [ ] **Step 1: Simplify ParallelPhaseStrategy**

In `src/main/kotlin/engine/ParallelPhaseStrategy.kt`, remove the aggregation logic:

```kotlin
class ParallelPhaseStrategy(
    private val objectMapper: ObjectMapper,
) : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val fanOut = context.currentSeqInfo.activity.fanOut
            ?: throw IllegalStateException("PARALLEL phase at seq ${context.currentSeqInfo.sequenceNumber} has no fanOut definition")
        val joinPolicy = fanOut.joinPolicy
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

Note: `objectMapper` constructor parameter can be removed since aggregation is gone. But `PhaseStrategyRegistry` passes it, so keep it for now to avoid changing that file. The unused import warning is acceptable — or remove the parameter and update `PhaseStrategyRegistry` if preferred.

Actually, remove the `objectMapper` dependency since it's no longer used:

```kotlin
class ParallelPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val fanOut = context.currentSeqInfo.activity.fanOut
            ?: throw IllegalStateException("PARALLEL phase at seq ${context.currentSeqInfo.sequenceNumber} has no fanOut definition")
        val joinPolicy = fanOut.joinPolicy
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

Update `PhaseStrategyRegistry` to match:

In `src/main/kotlin/engine/PhaseStrategyRegistry.kt`:
```kotlin
@ApplicationScoped
class PhaseStrategyRegistry(objectMapper: ObjectMapper) {

    private val strategies = ConcurrentHashMap<PhaseType, PhaseStrategy>()

    init {
        register(PhaseType.LINEAR, LinearPhaseStrategy())
        register(PhaseType.SCATTER, ScatterPhaseStrategy(objectMapper))
        register(PhaseType.PARALLEL, ParallelPhaseStrategy())
    }

    fun register(type: PhaseType, strategy: PhaseStrategy) {
        strategies[type] = strategy
    }

    fun resolve(type: PhaseType): PhaseStrategy =
        strategies[type] ?: throw IllegalStateException("No strategy registered for phase type: $type")
}
```

- [ ] **Step 2: Update ParallelPhaseStrategyTest**

In `src/test/kotlin/engine/ParallelPhaseStrategyTest.kt`:

Remove `objectMapper` and update strategy instantiation:
```kotlin
private val strategy = ParallelPhaseStrategy()
```

Remove the `objectMapper` field entirely.

Update `parallelTask` helper — remove `payloadJson`:
```kotlin
private fun parallelTask(status: TaskStatus = TaskStatus.COMPLETED, resultJson: String? = null) = Task(
    id = "t-${System.nanoTime()}", workflowId = "wf1", sequenceNumber = 2, status = status,
    handlerKey = "parallel.handler", resultJson = resultJson,
    claimedBy = null, claimedAt = null, completedAt = null,
    retryCount = 0, maxRetries = 0, deadlineAt = null,
)
```

Update `failure with BEST_EFFORT advances with null payload` — remove payload assertion:
```kotlin
@Test
fun `failure with BEST_EFFORT advances to next sequence`() {
    val tasks = listOf(parallelTask(), parallelTask(status = TaskStatus.FAILED))
    val ctx = context(failedCount = 1, tasks = tasks, failurePolicy = FailurePolicy.BEST_EFFORT)
    val advance = assertIs<AdvancementDecision.Advance>(strategy.resolve(ctx))
    assertEquals(3, advance.nextSequence)
}
```

Remove the three aggregated payload tests entirely:
- `success aggregates completed task results as JSON array payload`
- `success with mixed null results only includes non-null`
- `success with join policy filters only completed results`

These tests validated the aggregation behavior which no longer exists — `InputResolver` handles this lazily now (tested in Task 7).

- [ ] **Step 3: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ParallelPhaseStrategyTest,PhaseStrategyRegistryTest" -pl WorkFlow -f pom.xml`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/engine/ParallelPhaseStrategy.kt src/main/kotlin/engine/PhaseStrategyRegistry.kt src/test/kotlin/engine/ParallelPhaseStrategyTest.kt
git commit -m "refactor: remove aggregation from parallel strategy, simplify to pure join evaluation"
```

---

### Task 6: DB Migration and TaskRepository — Replace payload with item

**Files:**
- Create: `src/main/resources/db/migration/V8__explicit_inputs.sql`
- Modify: `src/main/kotlin/engine/TaskRepository.kt:324-365,406-428`

- [ ] **Step 1: Create migration file**

```sql
-- V8: Replace payload column with item column (explicit inputs design).
-- item stores the scatter chunk for parallel tasks only.
-- Input resolution now happens at execution time from previous activities' resultJson.

ALTER TABLE task ADD (item CLOB);

ALTER TABLE task DROP COLUMN payload;
```

Save to: `src/main/resources/db/migration/V8__explicit_inputs.sql`

- [ ] **Step 2: Update `insertBatchWithHandle` in TaskRepository**

Replace the batch SQL and binding in `src/main/kotlin/engine/TaskRepository.kt:324-365`:

```kotlin
fun insertBatchWithHandle(
    handle: Handle,
    tasks: List<Task>,
) {
    if (tasks.isEmpty()) return
    val batch =
        handle.prepareBatch(
            """
        INSERT INTO task (id, workflow_id, sequence_number, status, handler_key,
                          item, result, claimed_by, claimed_at, completed_at,
                          retry_count, max_retries, deadline_at, not_before, backoff_base, backoff_cap, queue_name)
        VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey,
                :item, :result, :claimedBy, :claimedAt, :completedAt,
                :retryCount, :maxRetries, :deadlineAt, :notBefore, :backoffBase, :backoffCap, :queueName)
        """,
        )
    for (task in tasks) {
        batch
            .bind("id", task.id)
            .bind("workflowId", task.workflowId)
            .bind("sequenceNumber", task.sequenceNumber)
            .bind("status", task.status.name)
            .bind("handlerKey", task.handlerKey)
        bindNullableClob(batch, "item", task.item)
        bindNullableClob(batch, "result", task.resultJson)
        batch
            .bind("claimedBy", task.claimedBy)
        bindNullableTimestamp(batch, "claimedAt", task.claimedAt)
        bindNullableTimestamp(batch, "completedAt", task.completedAt)
        batch
            .bind("retryCount", task.retryCount)
            .bind("maxRetries", task.maxRetries)
        bindNullableTimestamp(batch, "deadlineAt", task.deadlineAt)
        bindNullableTimestamp(batch, "notBefore", task.notBefore)
        batch
            .bind("backoffBase", task.backoffBase)
            .bind("backoffCap", task.backoffCap)
            .bind("queueName", task.queueName)
        batch.add()
    }
    batch.execute()
}
```

- [ ] **Step 3: Update `mapTaskRow` in TaskRepository**

Replace `payloadJson` with `item` in `src/main/kotlin/engine/TaskRepository.kt:406-428`:

```kotlin
private fun mapTaskRow(row: Map<String, Any?>): Task {
    val ci = caseInsensitive(row)
    return Task(
        id = ci["ID"] as String,
        workflowId = ci["WORKFLOW_ID"] as String,
        sequenceNumber = (ci["SEQUENCE_NUMBER"] as Number).toInt(),
        status = TaskStatus.valueOf(ci["STATUS"] as String),
        handlerKey = ci["HANDLER_KEY"] as String,
        item = ci["ITEM"]?.let { readClob(it) },
        resultJson = ci["RESULT"]?.let { readClob(it) },
        claimedBy = ci["CLAIMED_BY"] as String?,
        claimedAt = readNullableTimestamp(ci["CLAIMED_AT"]),
        completedAt = readNullableTimestamp(ci["COMPLETED_AT"]),
        retryCount = (ci["RETRY_COUNT"] as Number).toInt(),
        maxRetries = (ci["MAX_RETRIES"] as Number).toInt(),
        deadlineAt = readNullableTimestamp(ci["DEADLINE_AT"]),
        notBefore = readNullableTimestamp(ci["NOT_BEFORE"]),
        backoffBase = (ci["BACKOFF_BASE"] as Number).toInt(),
        backoffCap = (ci["BACKOFF_CAP"] as Number).toInt(),
        enqueuedAt = readTimestamp(ci["ENQUEUED_AT"]),
        queueName = (ci["QUEUE_NAME"] as? String) ?: "default",
    )
}
```

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/db/migration/V8__explicit_inputs.sql src/main/kotlin/engine/TaskRepository.kt
git commit -m "feat: add V8 migration replacing payload with item, update TaskRepository"
```

---

### Task 7: InputResolver — New class with TDD

**Files:**
- Create: `src/main/kotlin/engine/InputResolver.kt`
- Create: `src/test/kotlin/engine/InputResolverTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
// src/test/kotlin/engine/InputResolverTest.kt
package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.WorkflowDefinition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class InputResolverTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    private val resolver = InputResolver(objectMapper)

    // ── Helpers ──

    private fun task(
        sequenceNumber: Int,
        status: TaskStatus = TaskStatus.COMPLETED,
        resultJson: String? = null,
    ) = Task(
        id = "t-${sequenceNumber}-${System.nanoTime()}", workflowId = "wf1",
        sequenceNumber = sequenceNumber, status = status,
        handlerKey = "h", resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun linearSequenceMap(): Map<Int, SequenceInfo> {
        val act1 = ActivityDefinition(name = "step1", transition = "step1.handler")
        val act2 = ActivityDefinition(name = "step2", transition = "step2.handler")
        return mapOf(
            1 to SequenceInfo(1, 0, act1, PhaseType.LINEAR, 2),
            2 to SequenceInfo(2, 1, act2, PhaseType.LINEAR, null),
        )
    }

    private fun fanOutSequenceMap(): Map<Int, SequenceInfo> {
        val act = ActivityDefinition(
            name = "split", transition = "scatter.handler",
            fanOut = FanOutDefinition(transition = "parallel.handler"),
        )
        val act2 = ActivityDefinition(name = "notify", transition = "notify.handler")
        return mapOf(
            1 to SequenceInfo(1, 0, act, PhaseType.SCATTER, 2),
            2 to SequenceInfo(2, 0, act, PhaseType.PARALLEL, 3),
            3 to SequenceInfo(3, 1, act2, PhaseType.LINEAR, null),
        )
    }

    // ── Tests ──

    @Test
    fun `empty inputs returns null`() {
        val result = resolver.resolve(emptyMap(), linearSequenceMap()) { emptyList() }
        assertNull(result)
    }

    @Test
    fun `whole-result reference from linear activity`() {
        val inputs = mapOf("data" to "step1")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 1) listOf(task(1, resultJson = """{"uri":"s3://data","count":42}"""))
            else emptyList()
        }
        val result = resolver.resolve(inputs, linearSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("""{"uri":"s3://data","count":42}""", parsed.get("data").toString())
    }

    @Test
    fun `field-level reference from linear activity`() {
        val inputs = mapOf("uri" to "step1.uri", "count" to "step1.count")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 1) listOf(task(1, resultJson = """{"uri":"s3://data","count":42}"""))
            else emptyList()
        }
        val result = resolver.resolve(inputs, linearSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("s3://data", parsed.get("uri").asText())
        assertEquals(42, parsed.get("count").asInt())
    }

    @Test
    fun `whole-result reference from fan-out activity aggregates parallel results`() {
        val inputs = mapOf("results" to "split")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
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

    @Test
    fun `field-level reference from fan-out activity extracts per-element`() {
        val inputs = mapOf("uris" to "split.uri")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 2) listOf(
                task(2, resultJson = """{"uri":"s3://a","count":1}"""),
                task(2, resultJson = """{"uri":"s3://b","count":2}"""),
            ) else emptyList()
        }
        val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        val arr = parsed.get("uris")
        assertEquals(2, arr.size())
        assertEquals("s3://a", arr[0].asText())
        assertEquals("s3://b", arr[1].asText())
    }

    @Test
    fun `fan-out aggregation skips non-completed tasks`() {
        val inputs = mapOf("results" to "split")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 2) listOf(
                task(2, resultJson = """{"r":"ok"}"""),
                task(2, status = TaskStatus.FAILED, resultJson = null),
            ) else emptyList()
        }
        val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals(1, parsed.get("results").size())
    }

    @Test
    fun `inputs from multiple activities`() {
        val act1 = ActivityDefinition(name = "init", transition = "init.handler")
        val act2 = ActivityDefinition(name = "enrich", transition = "enrich.handler")
        val act3 = ActivityDefinition(name = "final", transition = "final.handler")
        val seqMap = mapOf(
            1 to SequenceInfo(1, 0, act1, PhaseType.LINEAR, 2),
            2 to SequenceInfo(2, 1, act2, PhaseType.LINEAR, 3),
            3 to SequenceInfo(3, 2, act3, PhaseType.LINEAR, null),
        )
        val inputs = mapOf("cfg" to "init.config", "meta" to "enrich.summary")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            when (seq) {
                1 -> listOf(task(1, resultJson = """{"config":"prod"}"""))
                2 -> listOf(task(2, resultJson = """{"summary":"done"}"""))
                else -> emptyList()
            }
        }
        val result = resolver.resolve(inputs, seqMap, tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("prod", parsed.get("cfg").asText())
        assertEquals("done", parsed.get("meta").asText())
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="InputResolverTest" -pl WorkFlow -f pom.xml`
Expected: Compilation error — `InputResolver` class does not exist

- [ ] **Step 3: Implement InputResolver**

```kotlin
// src/main/kotlin/engine/InputResolver.kt
package com.workflow.engine

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class InputResolver(
    private val objectMapper: ObjectMapper,
) {

    fun resolve(
        inputs: Map<String, String>,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: (Int) -> List<Task>,
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

    private fun parseRef(ref: String): Pair<String, String?> {
        val dot = ref.indexOf('.')
        return if (dot < 0) ref to null
        else ref.substring(0, dot) to ref.substring(dot + 1)
    }

    private fun resolveActivity(
        activityName: String,
        fieldPath: String?,
        sequenceMap: Map<Int, SequenceInfo>,
        tasksBySequence: (Int) -> List<Task>,
    ): JsonNode {
        val seqEntry = sequenceMap.values.first { it.activity.name == activityName }
        val isFanOut = seqEntry.activity.fanOut != null

        if (isFanOut) {
            val parallelSeq = findParallelSequence(seqEntry, sequenceMap)
            val tasks = tasksBySequence(parallelSeq)
                .filter { it.status == TaskStatus.COMPLETED }
            return aggregateFanOut(tasks, fieldPath)
        }

        val tasks = tasksBySequence(seqEntry.sequenceNumber)
        val task = tasks.firstOrNull { it.status == TaskStatus.COMPLETED }
        val resultJson = task?.resultJson

        if (resultJson == null) return objectMapper.nullNode()

        val resultTree = objectMapper.readTree(resultJson)
        return if (fieldPath != null) resultTree.path(fieldPath) else resultTree
    }

    private fun findParallelSequence(
        scatterSeqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
    ): Int {
        val nextSeq = scatterSeqInfo.nextSequence
            ?: throw IllegalStateException("Fan-out activity '${scatterSeqInfo.activity.name}' has no parallel sequence")
        val nextInfo = sequenceMap[nextSeq]!!
        require(nextInfo.phaseType == PhaseType.PARALLEL) {
            "Expected PARALLEL at sequence $nextSeq but found ${nextInfo.phaseType}"
        }
        return nextSeq
    }

    private fun aggregateFanOut(
        tasks: List<Task>,
        fieldPath: String?,
    ): ArrayNode {
        val arrayNode = objectMapper.createArrayNode()
        for (task in tasks) {
            val resultJson = task.resultJson ?: continue
            val resultTree = objectMapper.readTree(resultJson)
            if (fieldPath != null) {
                arrayNode.add(resultTree.path(fieldPath))
            } else {
                arrayNode.add(resultTree)
            }
        }
        return arrayNode
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="InputResolverTest" -pl WorkFlow -f pom.xml`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/engine/InputResolver.kt src/test/kotlin/engine/InputResolverTest.kt
git commit -m "feat: add InputResolver for explicit input resolution"
```

---

### Task 8: Handler Contract — Update HandlerInput and WorkerLoop

**Files:**
- Modify: `src/main/kotlin/worker/TransitionHandler.kt:46-51`
- Modify: `src/main/kotlin/worker/WorkerLoop.kt:196-226`
- Modify: `src/test/kotlin/worker/WorkerLoopTest.kt`

- [ ] **Step 1: Update HandlerInput**

In `src/main/kotlin/worker/TransitionHandler.kt`, replace `payload` with `inputs` and `item`:

```kotlin
/**
 * Input provided to a [TransitionHandler] for task execution.
 *
 * @property taskId Unique task identifier — use as idempotency key for external calls.
 * @property workflowId Parent workflow identifier.
 * @property sequenceNumber Position in the workflow DAG.
 * @property inputs Resolved input map from declared activity inputs. Null if no inputs declared.
 * @property item Scatter chunk for parallel tasks. Null for non-parallel tasks.
 */
data class HandlerInput(
    val taskId: String,
    val workflowId: String,
    val sequenceNumber: Int,
    val inputs: String?,
    val item: String?,
)
```

- [ ] **Step 2: Update WorkerLoop to resolve inputs**

In `src/main/kotlin/worker/WorkerLoop.kt`, add `InputResolver` dependency and update `processTask`:

Add to constructor:
```kotlin
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val taskRepo: TaskRepository,
    private val handlerRegistry: HandlerRegistry,
    private val barrierService: BarrierService,
    private val meterRegistry: MeterRegistry,
    private val inputResolver: InputResolver,
    private val workflowRepo: WorkflowRepository,
    private val objectMapper: ObjectMapper,
) : ShutdownParticipant {
```

Update `processTask` — add input resolution between claim and handler execution (lines 196-226):

```kotlin
private suspend fun processTask(task: Task) {
    val taskMdc = MDC.getCopyOfContextMap().orEmpty() + mapOf(
        "task_id" to task.id,
        "handler_key" to task.handlerKey,
        "workflow_id" to task.workflowId,
        "attempt" to task.retryCount.toString(),
    )
    withContext(MDCContext(taskMdc)) {
        _inFlightTasks.incrementAndGet()
        try {
            val handler = handlerRegistry.resolve(task.handlerKey)

            val resolvedInputs = resolveInputs(task)

            val input =
                HandlerInput(
                    taskId = task.id,
                    workflowId = task.workflowId,
                    sequenceNumber = task.sequenceNumber,
                    inputs = resolvedInputs,
                    item = task.item,
                )
            val output = handler.execute(input)

            try {
                barrierService.onTaskCompleted(
                    taskId = task.id,
                    workflowId = task.workflowId,
                    sequenceNumber = task.sequenceNumber,
                    status = TaskStatus.COMPLETED,
                    resultJson = output.result,
                    claimedBy = task.claimedBy,
                    claimedAt = task.claimedAt,
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Barrier failed for COMPLETED task {}, falling through to failure path", task.id, e)
                handleTaskFailure(task, e)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleTaskFailure(task, e)
        } finally {
            _inFlightTasks.decrementAndGet()
            _lastActivityTimestamp = Instant.now()
        }
    }
}

private suspend fun resolveInputs(task: Task): String? {
    val workflow = workflowRepo.findById(task.workflowId) ?: return null
    val definition = objectMapper.readValue<com.workflow.dsl.WorkflowDefinition>(workflow.definitionJson)
    val sequenceMap = buildSequenceMap(definition)
    val seqInfo = sequenceMap[task.sequenceNumber] ?: return null
    val activityInputs = seqInfo.activity.inputs
    if (activityInputs.isEmpty()) return null

    return inputResolver.resolve(activityInputs, sequenceMap) { seq ->
        taskRepo.findByWorkflowAndSequence(task.workflowId, seq)
    }
}
```

Note: Add the necessary import for `readValue` at top of file:
```kotlin
import com.fasterxml.jackson.module.kotlin.readValue
```

- [ ] **Step 3: Update WorkerLoopTest**

In `src/test/kotlin/worker/WorkerLoopTest.kt`:

Update `makeTask` helper — replace `payloadJson` with `item`:
```kotlin
private fun makeTask(
    id: String = UUID.randomUUID().toString(),
    workflowId: String = UUID.randomUUID().toString(),
    sequenceNumber: Int = 1,
    status: TaskStatus = TaskStatus.PROCESSING,
    handlerKey: String = "order.validate",
    item: String? = null,
    resultJson: String? = null,
    retryCount: Int = 0,
    maxRetries: Int = 3,
    deadlineAt: Instant? = Instant.now().plus(30, ChronoUnit.MINUTES),
): Task = Task(
    id = id,
    workflowId = workflowId,
    sequenceNumber = sequenceNumber,
    status = status,
    handlerKey = handlerKey,
    item = item,
    resultJson = resultJson,
    claimedBy = workerId,
    claimedAt = Instant.now(),
    completedAt = null,
    retryCount = retryCount,
    maxRetries = maxRetries,
    deadlineAt = deadlineAt,
)
```

Update `setup()` to add mock dependencies:
```kotlin
private lateinit var inputResolver: InputResolver
private lateinit var workflowRepo: WorkflowRepository
private lateinit var objectMapper: ObjectMapper

@BeforeEach
fun setup() {
    taskRepo = mock()
    handlerRegistry = mock()
    barrierService = mock()
    inputResolver = mock()
    workflowRepo = mock()
    objectMapper = ObjectMapper().registerModule(com.fasterxml.jackson.module.kotlin.KotlinModule.Builder().build())

    // ... existing config setup ...

    meterRegistry = SimpleMeterRegistry()
    workerLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService, meterRegistry, inputResolver, workflowRepo, objectMapper)
}
```

Update happy-path test assertions — check `inputs` and `item` instead of `payload`:
```kotlin
val input = inputCaptor.firstValue
assertEquals(task.id, input.taskId)
assertEquals(task.workflowId, input.workflowId)
assertEquals(task.sequenceNumber, input.sequenceNumber)
assertNull(input.inputs)  // no workflow found by mock → null
assertEquals(task.item, input.item)
```

- [ ] **Step 4: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkerLoopTest" -pl WorkFlow -f pom.xml`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/worker/TransitionHandler.kt src/main/kotlin/worker/WorkerLoop.kt src/test/kotlin/worker/WorkerLoopTest.kt
git commit -m "feat: update handler contract and worker loop for explicit inputs"
```

---

### Task 9: WorkflowEngine — Update startWorkflow

**Files:**
- Modify: `src/main/kotlin/engine/WorkflowEngine.kt:23-52`

- [ ] **Step 1: Update startWorkflow to not pass payload**

In `src/main/kotlin/engine/WorkflowEngine.kt`, the `startWorkflow` method currently passes `initialPayload` to `createTaskForActivity`. Since `createTaskForActivity` no longer takes a payload parameter, update:

```kotlin
suspend fun startWorkflow(definition: WorkflowDefinition): String {
    require(definition.activities.isNotEmpty()) { "WorkflowDefinition must have at least one activity" }

    val workflowId = UUID.randomUUID().toString()
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    val definitionJson = objectMapper.writeValueAsString(definition)

    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        val run = WorkflowRun(
            id = workflowId,
            definitionJson = definitionJson,
            currentSequence = 1,
            version = 0,
            status = WorkflowStatus.RUNNING,
            createdAt = now,
            updatedAt = now,
            deadlineAt = now.plus(definition.deadline),
        )
        workflowRepo.insertWithHandle(handle, run)

        val firstActivity = definition.activities.first()
        val task = createTaskForActivity(
            workflowId = workflowId,
            sequenceNumber = 1,
            activity = firstActivity,
            now = now,
        )
        taskRepo.insertBatchWithHandle(handle, listOf(task))
    }

    log.info("Started workflow {} with {} activities", workflowId, definition.activities.size)
    return workflowId
}
```

- [ ] **Step 2: Commit**

```bash
git add src/main/kotlin/engine/WorkflowEngine.kt
git commit -m "refactor: remove initialPayload from startWorkflow"
```

---

### Task 10: Fix remaining test files — BarrierServiceTest and WorkflowIntegrationTest

**Files:**
- Modify: `src/test/kotlin/engine/BarrierServiceTest.kt`
- Modify: `src/test/kotlin/engine/WorkflowIntegrationTest.kt`

- [ ] **Step 1: Update BarrierServiceTest `makeTask` helper**

Replace `payloadJson` with `item` in the `makeTask` function:

```kotlin
private fun makeTask(
    id: String = randomId(),
    workflowId: String,
    sequenceNumber: Int = 1,
    status: TaskStatus = TaskStatus.PENDING,
    handlerKey: String = "test.handler",
    item: String? = null,
    resultJson: String? = null,
    claimedBy: String? = null,
    claimedAt: Instant? = null,
    completedAt: Instant? = null,
    retryCount: Int = 0,
    maxRetries: Int = 0,
    deadlineAt: Instant? = null,
): Task = Task(
    id = id,
    workflowId = workflowId,
    sequenceNumber = sequenceNumber,
    status = status,
    handlerKey = handlerKey,
    item = item,
    resultJson = resultJson,
    claimedBy = claimedBy,
    claimedAt = claimedAt,
    completedAt = completedAt,
    retryCount = retryCount,
    maxRetries = maxRetries,
    deadlineAt = deadlineAt,
)
```

Search for any other `payloadJson` references in the file and replace with `item` (or remove if they were asserting auto-forwarded payloads).

- [ ] **Step 2: Update WorkflowIntegrationTest**

Search for any `payloadJson` or `payload` references in the integration test and update:
- Replace `payloadJson` with `item` in any Task construction helpers
- Remove assertions that check auto-forwarded payloads between sequences
- Update any `startWorkflow(definition, initialPayload)` calls to `startWorkflow(definition)`
- If tests read `payload` from raw SQL rows (e.g., `ci["PAYLOAD"]`), change to `ci["ITEM"]`

- [ ] **Step 3: Run the full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -f pom.xml`
Expected: All tests PASS. Fix any remaining compilation errors from `payloadJson` references.

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/engine/BarrierServiceTest.kt src/test/kotlin/engine/WorkflowIntegrationTest.kt
git commit -m "test: update barrier and integration tests for explicit inputs"
```

---

### Task 11: Final verification — Full build and coverage

- [ ] **Step 1: Run full build**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn clean test -pl WorkFlow -f pom.xml`
Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 2: Check coverage**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`
Expected: Coverage thresholds met

- [ ] **Step 3: Final commit (if any fixes)**

Only if coverage or build issues required fixes.
