# DAG Refactor — P6: dispatchWorkflow DSL Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `DispatchWorkflow.kt` from the old DSL (`fanOut("simulate")` / top-level `joinPolicy`) to the new DSL (`fanOut { transition(...) }` / embedded `FanOutDefinition`). Update all tests that construct the dispatch workflow. Verify spec item 35 (migrated dispatchWorkflow builds correctly).

**Architecture:** The `simulate` activity is no longer a named graph node — it becomes the fan-out embedded within `scatter`. The batchToken input ref changes from `simulate.batchToken` to `scatter.batchToken`. `ActivityInputResolver` may need an update if it resolves fan-out results differently.

**Tech Stack:** Kotlin 2.3, JUnit 5

---

### Task 1: Migrate `DispatchWorkflow.kt`

**Files:**
- Modify: `src/main/kotlin/dispatch/dsl/DispatchWorkflow.kt`

- [ ] **Step 1: Read current `DispatchWorkflow.kt`**

Current content:
```kotlin
val dispatchWorkflow: WorkflowDefinition = workflow {
    deadline(Duration.ofHours(2))
    activity("scatter") {
        transition("DispatchScatterHandler")
        fanOut("simulate")
    }
    activity("simulate") {
        transition("DispatchSimulationHandler")
        retries(2)
        deadline(Duration.ofMinutes(30))
        joinPolicy(JoinPolicy.All)
    }
    activity("join") {
        transition("DispatchJoinHandler")
        deadline(Duration.ofMinutes(10))
        inputs {
            "batchToken" from "simulate.batchToken"
        }
    }
}
```

- [ ] **Step 2: Write a test that asserts the migrated structure**

Add to `src/test/kotlin/dispatch/dsl/DispatchAlgorithmDslTest.kt` (or create a new `DispatchWorkflowTest.kt`):

```kotlin
package com.workflow.dispatch.dsl

import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.buildSequenceMap
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class DispatchWorkflowTest {

    @Test
    fun `dispatchWorkflow start is scatter`() {
        assertEquals("scatter", dispatchWorkflow.start)
    }

    @Test
    fun `dispatchWorkflow scatter has FanOutDefinition with DispatchSimulationHandler`() {
        val scatter = dispatchWorkflow.activities["scatter"]!!
        assertNotNull(scatter.fanOut)
        assertEquals("DispatchSimulationHandler", scatter.fanOut!!.transition)
        assertEquals(2, scatter.fanOut!!.retries)
        assertEquals(JoinPolicy.All, scatter.fanOut!!.joinPolicy)
    }

    @Test
    fun `dispatchWorkflow scatter successor is join`() {
        val scatter = dispatchWorkflow.activities["scatter"]!!
        assertEquals(1, scatter.successors.size)
        assertEquals("join", scatter.successors[0].target)
    }

    @Test
    fun `dispatchWorkflow no simulate activity exists as named node`() {
        assert("simulate" !in dispatchWorkflow.activities) {
            "simulate should be embedded in fanOut, not a named activity"
        }
    }

    @Test
    fun `dispatchWorkflow join batchToken resolves from scatter`() {
        val join = dispatchWorkflow.activities["join"]!!
        assertEquals("scatter.batchToken", join.inputs["batchToken"])
    }

    @Test
    fun `dispatchWorkflow builds valid sequence map`() {
        // Verifies no validation errors, cycle, or unreachable activities
        val seqMap = buildSequenceMap(dispatchWorkflow)
        assertEquals(3, seqMap.size) // scatter(SCATTER), scatter(PARALLEL), join(LINEAR)
    }
}
```

- [ ] **Step 3: Run test to confirm it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchWorkflowTest" -pl WorkFlow`

Expected: FAIL — old DSL syntax causes build errors

- [ ] **Step 4: Replace `DispatchWorkflow.kt` with new DSL**

```kotlin
package com.workflow.dispatch.dsl

import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.dsl.workflow
import java.time.Duration

val dispatchWorkflow: WorkflowDefinition = workflow {
    start("scatter")
    deadline(Duration.ofHours(2))

    activity("scatter") {
        transition("DispatchScatterHandler")
        fanOut {
            transition("DispatchSimulationHandler")
            retries(2)
            deadline(Duration.ofMinutes(30))
            joinPolicy(JoinPolicy.All)
        }
        next("join")
    }

    activity("join") {
        transition("DispatchJoinHandler")
        deadline(Duration.ofMinutes(10))
        inputs {
            "batchToken" from "scatter.batchToken"
        }
    }
}
```

- [ ] **Step 5: Run tests to confirm they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DispatchWorkflowTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/dispatch/dsl/DispatchWorkflow.kt
git add src/test/kotlin/dispatch/dsl/DispatchWorkflowTest.kt
git commit -m "feat: migrate dispatchWorkflow to new DAG DSL with embedded fanOut"
```

---

### Task 2: Update `ActivityInputResolver` for fan-out result sourcing

**Files:**
- Modify (if needed): `src/main/kotlin/workflow/usecase/service/orchestration/ActivityInputResolver.kt`
- Modify (if needed): `src/test/kotlin/workflow/usecase/service/orchestration/ActivityInputResolverTest.kt`

- [ ] **Step 1: Read `ActivityInputResolver.kt`**

Read `src/main/kotlin/workflow/usecase/service/orchestration/ActivityInputResolver.kt` to understand how it resolves inputs like `"scatter.batchToken"`.

- [ ] **Step 2: Check if resolution needs updating for fan-out results**

The spec notes:
> If batchToken originates from parallel task results, `ActivityInputResolver` must be extended to aggregate fan-out results.

Check if the current resolver looks up task results by `activityName` or by `sequenceNumber`. If it looks up by activity name `"scatter"`, it will now find the SCATTER phase task (not the PARALLEL phase tasks). The `batchToken` that joins care about may be in the scatter task's result (from scatter handler) OR aggregated from parallel task results.

For the dispatch use case: `DispatchScatterHandler` returns the `batchToken` in its result JSON. So `"scatter.batchToken"` resolves from the SCATTER phase task result. The resolver should look up tasks where `activity_name = 'scatter'` (not `'scatter.__parallel__'`).

- [ ] **Step 3: Update resolver if it uses `sequenceNumber` lookup instead of `activityName`**

If the resolver currently looks up by `sequence_number` (using `currentSequence` logic), it needs to switch to looking up by `activity_name`.

Update `TaskRepository` interface to add:
```kotlin
suspend fun findByWorkflowAndActivityName(workflowId: String, activityName: String): List<Task>
```

Update `JdbiTaskRepository`:
```kotlin
override suspend fun findByWorkflowAndActivityName(workflowId: String, activityName: String): List<Task> =
    jdbi.withHandleSuspend<List<Task>, Exception> { h ->
        h.createQuery(
            "SELECT * FROM task WHERE workflow_id = :wfId AND activity_name = :activityName"
        )
            .bind("wfId", workflowId)
            .bind("activityName", activityName)
            .mapToMap()
            .list()
            .map(::mapTaskRow)
    }
```

Update `ActivityInputResolver.resolve()` to use `findByWorkflowAndActivityName` instead of `findByWorkflowAndSequence`.

- [ ] **Step 4: Run input resolver tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="ActivityInputResolverTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/ActivityInputResolver.kt
git add src/main/kotlin/workflow/usecase/port/outbound/persistent/TaskRepository.kt
git add src/main/kotlin/workflow/adapter/persistent/JdbiTaskRepository.kt
git add src/test/kotlin/workflow/usecase/service/orchestration/ActivityInputResolverTest.kt
git commit -m "feat: update ActivityInputResolver to look up tasks by activityName for DAG input resolution"
```
