# DAG Refactor — P1: Foundation Types

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Edge`, `FanOutDefinition`, `SKIPPED` status, and `SCATTER` phase type as pure additive changes — zero existing test regressions.

**Architecture:** All changes are new declarations alongside existing code. Nothing is removed or modified beyond enum additions. The `AdvancementStrategyRegistryTest` "all known phase types" test is narrowed to exclude SCATTER (the strategy system is replaced in P3).

**Tech Stack:** Kotlin 2.3, JUnit 5, `kotlinx-coroutines-test`

---

### Task 1: Add `Edge` and `FanOutDefinition` model types

**Files:**
- Create: `src/main/kotlin/workflow/model/Edge.kt`
- Create: `src/main/kotlin/workflow/model/FanOutDefinition.kt`
- Modify: `src/test/kotlin/workflow/model/WorkflowModelsTest.kt` (add tests at bottom)

- [ ] **Step 1: Write failing tests for Edge and FanOutDefinition**

Append to `src/test/kotlin/workflow/model/WorkflowModelsTest.kt` after the last test:

```kotlin
    // ── Edge ────────────────────────────────────────────────────────────

    @Test
    fun `Edge defaults to DEFAULT_BRANCH label`() {
        val edge = Edge("fulfill")
        assertEquals("fulfill", edge.target)
        assertEquals(DEFAULT_BRANCH, edge.label)
    }

    @Test
    fun `Edge with explicit label preserves label`() {
        val edge = Edge("reject", "FAILED")
        assertEquals("reject", edge.target)
        assertEquals("FAILED", edge.label)
    }

    @Test
    fun `DEFAULT_BRANCH constant value is double-underscore default double-underscore`() {
        assertEquals("__default__", DEFAULT_BRANCH)
    }

    // ── FanOutDefinition ─────────────────────────────────────────────────

    @Test
    fun `FanOutDefinition defaults match spec`() {
        val fanOut = FanOutDefinition(transition = "MyHandler")
        assertEquals("MyHandler", fanOut.transition)
        assertEquals(0, fanOut.retries)
        assertEquals(FailurePolicy.ABORT, fanOut.failurePolicy)
        assertEquals(Duration.ofMinutes(30), fanOut.deadline)
        assertEquals(JoinPolicy.All, fanOut.joinPolicy)
        assertEquals(Duration.ofSeconds(1), fanOut.backoffBase)
        assertEquals(Duration.ofSeconds(300), fanOut.backoffCap)
        assertEquals("default", fanOut.queue)
    }

    @Test
    fun `FanOutDefinition preserves overridden fields`() {
        val fanOut = FanOutDefinition(
            transition = "Handler",
            retries = 3,
            failurePolicy = FailurePolicy.BEST_EFFORT,
            deadline = Duration.ofMinutes(5),
            joinPolicy = JoinPolicy.Percentage(80),
            backoffBase = Duration.ofSeconds(2),
            backoffCap = Duration.ofSeconds(60),
            queue = "priority",
        )
        assertEquals(3, fanOut.retries)
        assertEquals(FailurePolicy.BEST_EFFORT, fanOut.failurePolicy)
        assertEquals(Duration.ofMinutes(5), fanOut.deadline)
        assertEquals(JoinPolicy.Percentage(80), fanOut.joinPolicy)
        assertEquals(Duration.ofSeconds(2), fanOut.backoffBase)
        assertEquals(Duration.ofSeconds(60), fanOut.backoffCap)
        assertEquals("priority", fanOut.queue)
    }
```

Also add the required imports to `WorkflowModelsTest.kt`:
```kotlin
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FanOutDefinition
import java.time.Duration
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`

Expected: FAIL with `Unresolved reference: Edge`, `Unresolved reference: FanOutDefinition`, `Unresolved reference: DEFAULT_BRANCH`

- [ ] **Step 3: Create `Edge.kt`**

Create `src/main/kotlin/workflow/model/Edge.kt`:

```kotlin
package com.workflow.workflow.model

const val DEFAULT_BRANCH = "__default__"

data class Edge(
    val target: String,
    val label: String = DEFAULT_BRANCH,
)
```

- [ ] **Step 4: Create `FanOutDefinition.kt`**

Create `src/main/kotlin/workflow/model/FanOutDefinition.kt`:

```kotlin
package com.workflow.workflow.model

import java.time.Duration

data class FanOutDefinition(
    val transition: String,
    val retries: Int = 0,
    val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    val deadline: Duration = Duration.ofMinutes(30),
    val joinPolicy: JoinPolicy = JoinPolicy.All,
    val backoffBase: Duration = Duration.ofSeconds(1),
    val backoffCap: Duration = Duration.ofSeconds(300),
    val queue: String = "default",
)
```

- [ ] **Step 5: Run tests to confirm they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/workflow/model/Edge.kt
git add src/main/kotlin/workflow/model/FanOutDefinition.kt
git add src/test/kotlin/workflow/model/WorkflowModelsTest.kt
git commit -m "feat: add Edge and FanOutDefinition DAG foundation types"
```

---

### Task 2: Add `SKIPPED` to `TaskStatus`

**Files:**
- Modify: `src/main/kotlin/workflow/model/TaskStatus.kt`
- Modify: `src/test/kotlin/workflow/model/WorkflowModelsTest.kt`

- [ ] **Step 1: Write failing test for SKIPPED**

In `WorkflowModelsTest.kt`, replace the entry-count test:

```kotlin
// Old (8 values):
@Test
fun `TaskStatus contains exactly eight values`() {
    assertEquals(
        setOf("PENDING", "PROCESSING", "WAITING_FOR_SIGNAL", "COMPLETED", "FAILED", "TIMED_OUT", "DEAD_LETTER", "CANCELLED"),
        TaskStatus.entries.map { it.name }.toSet(),
    )
}
```

Replace with:

```kotlin
@Test
fun `TaskStatus contains exactly nine values`() {
    assertEquals(
        setOf("PENDING", "PROCESSING", "WAITING_FOR_SIGNAL", "COMPLETED", "FAILED",
              "TIMED_OUT", "DEAD_LETTER", "CANCELLED", "SKIPPED"),
        TaskStatus.entries.map { it.name }.toSet(),
    )
}
```

Also add below the existing `isTerminal` tests:

```kotlin
    @Test
    fun `SKIPPED is terminal`() {
        assertEquals(true, TaskStatus.SKIPPED.isTerminal)
    }
```

And update the `isTerminal returns true only for terminal statuses` test to include SKIPPED:

```kotlin
@Test
fun `isTerminal returns true only for terminal statuses`() {
    val expectedTerminal = setOf(
        TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMED_OUT,
        TaskStatus.DEAD_LETTER, TaskStatus.CANCELLED, TaskStatus.SKIPPED,
    )
    TaskStatus.entries.forEach { status ->
        assertEquals(
            status in expectedTerminal,
            status.isTerminal,
            "Expected isTerminal=${status in expectedTerminal} for $status",
        )
    }
}
```

- [ ] **Step 2: Run test to confirm it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`

Expected: FAIL — `SKIPPED` not found

- [ ] **Step 3: Add SKIPPED to `TaskStatus.kt`**

In `src/main/kotlin/workflow/model/TaskStatus.kt`, change the enum declaration and `terminalStatuses`:

```kotlin
package com.workflow.workflow.model

enum class TaskStatus {
    PENDING, PROCESSING, WAITING_FOR_SIGNAL, COMPLETED, FAILED,
    TIMED_OUT, DEAD_LETTER, CANCELLED,
    SKIPPED;   // terminal: inserted by phase gate when a conditional edge is not taken

    val isTerminal: Boolean get() = this in terminalStatuses

    companion object {
        private val terminalStatuses = setOf(COMPLETED, FAILED, TIMED_OUT, DEAD_LETTER, CANCELLED, SKIPPED)
        private val allowed = setOf(
            PENDING to PROCESSING,
            PENDING to CANCELLED,
            PROCESSING to COMPLETED,
            PROCESSING to FAILED,
            PROCESSING to TIMED_OUT,
            PROCESSING to PENDING,
            PROCESSING to DEAD_LETTER,
            PROCESSING to WAITING_FOR_SIGNAL,
            WAITING_FOR_SIGNAL to COMPLETED,
            WAITING_FOR_SIGNAL to FAILED,
            WAITING_FOR_SIGNAL to TIMED_OUT,
            WAITING_FOR_SIGNAL to CANCELLED,
            FAILED to PENDING,
            FAILED to DEAD_LETTER,
        )

        fun requireTransition(from: TaskStatus, to: TaskStatus) {
            require((from to to) in allowed) {
                "Illegal task transition: $from \u2192 $to"
            }
        }
    }
}
```

Note: SKIPPED is inserted directly by the phase gate (not via transition), so no transition rules are needed for it.

- [ ] **Step 4: Run all unit tests to confirm no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/workflow/model/TaskStatus.kt
git add src/test/kotlin/workflow/model/WorkflowModelsTest.kt
git commit -m "feat: add SKIPPED terminal status to TaskStatus"
```

---

### Task 3: Add `SCATTER` to `PhaseType` and update registry test

**Files:**
- Modify: `src/main/kotlin/workflow/model/SequenceModel.kt`
- Modify: `src/test/kotlin/workflow/usecase/service/phase/AdvancementStrategyRegistryTest.kt`

- [ ] **Step 1: Update `AdvancementStrategyRegistryTest` to not test SCATTER**

In `AdvancementStrategyRegistryTest.kt`, remove (or replace) the `all known phase types` test.

Replace:
```kotlin
@Test
fun `all known phase types resolve without error`() {
    PhaseType.entries.forEach { type ->
        registry.resolve(type) // should not throw
    }
}
```

With:
```kotlin
@Test
fun `LINEAR and PARALLEL resolve without error`() {
    // SCATTER has no strategy in the linear engine — strategy system is replaced in Plan 3
    registry.resolve(PhaseType.LINEAR)
    registry.resolve(PhaseType.PARALLEL)
}
```

- [ ] **Step 2: Add SCATTER to `PhaseType` in `SequenceModel.kt`**

In `src/main/kotlin/workflow/model/SequenceModel.kt`, change:

```kotlin
enum class PhaseType { LINEAR, PARALLEL }
```

To:

```kotlin
enum class PhaseType { LINEAR, SCATTER, PARALLEL }
```

No other changes to `buildSequenceMap()` — that is rewritten in Plan 3.

- [ ] **Step 3: Run all unit tests (no Oracle needed)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest,SequenceModelTest,WorkflowDslBuildersTest,AdvancementStrategyRegistryTest,LinearAdvancementStrategyTest,ParallelAdvancementStrategyTest" -pl WorkFlow`

Expected: `BUILD SUCCESS` — all pass

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/workflow/model/SequenceModel.kt
git add src/test/kotlin/workflow/usecase/service/phase/AdvancementStrategyRegistryTest.kt
git commit -m "feat: add SCATTER phase type; narrow registry test to exclude future SCATTER"
```
