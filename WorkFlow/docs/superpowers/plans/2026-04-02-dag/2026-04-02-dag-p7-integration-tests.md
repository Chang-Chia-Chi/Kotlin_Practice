# DAG Refactor — P7: Integration Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write Oracle-backed end-to-end integration tests covering spec items 37–50. Fix any remaining integration test regressions from P3–P6. Verify coverage thresholds pass.

**Architecture:** Tests run against `OracleTestContainer`. Each test starts a real workflow, drives tasks to completion via `DefaultPhaseGate`, and asserts DB state. Tests are organized by scenario: linear, conditional, fork+join, fan-out, CAS races, sweeper recovery, replay, deadline, cancel.

**Tech Stack:** Kotlin coroutines, JDBI 3, Oracle Free (Testcontainers), JUnit 5, Awaitility

---

### Task 1: Fix remaining integration test compile errors

Before writing new tests, ensure all existing integration tests compile and pass.

**Files to check and update:**
- `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt`
- `src/test/kotlin/workflow/usecase/service/orchestration/WorkflowEngineTest.kt`
- `src/test/kotlin/workflow/adapter/persistent/RepositoryTest.kt`
- `src/test/kotlin/stress/StressTestBase.kt` and all stress tests

- [ ] **Step 1: Read `WorkflowIntegrationTest.kt`**

Read the file to inventory what needs updating (old DSL syntax, `currentSequence` references, `AdvancementStrategyRegistry`, etc.)

- [ ] **Step 2: Update `WorkflowIntegrationTest.kt`**

Update all workflow definitions from old DSL (e.g., `fanOut("simulate")`) to new DSL. Update all `WorkflowRun` constructions to remove `currentSequence`. Remove `AdvancementStrategyRegistry` usages in `DefaultPhaseGate` construction.

- [ ] **Step 3: Update `WorkflowEngineTest.kt`**

Remove `run.currentSequence` assertion. Update workflow definitions to new DSL. Remove `AdvancementStrategyRegistry` from `DefaultPhaseGate` constructor call:
```kotlin
phaseGate = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
```

- [ ] **Step 4: Update `RepositoryTest.kt`**

Update any `WorkflowRun` constructions (remove `currentSequence`). Update `WorkflowDefinition` from `activities = listOf(...)` to `activities = mapOf(...)` with `start = "..."`.

- [ ] **Step 5: Update `StressTestBase.kt`**

Remove `AdvancementStrategyRegistry` import and usage. Update `DefaultPhaseGate` construction to remove the registry arg. Update any workflow definitions used in stress tests.

- [ ] **Step 6: Compile and run all unit tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`

Expected: `BUILD SUCCESS`

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowModelsTest,SequenceModelTest,WorkflowDslBuildersTest,WorkflowDslTest,DispatchWorkflowTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 7: Commit compile fixes**

```bash
git add src/test/kotlin/
git commit -m "fix: update integration test helpers to remove currentSequence and old DSL"
```

---

### Task 2: Write DAG integration tests (spec items 37–50)

**Files:**
- Modify: `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt`

Add the following test methods. Each test uses `OracleTestContainer`, `WorkflowEngine`, and `DefaultPhaseGate`.

- [ ] **Step 1: Add helper infrastructure**

Add these helpers to `WorkflowIntegrationTest.kt` (or to the test's companion/base):

```kotlin
private fun seqOf(def: WorkflowDefinition, activityName: String): Int =
    buildSequenceMap(def).values.first { it.activityName == activityName }.sequenceNumber

private fun taskStatusAt(wfId: String, seq: Int): List<String> =
    jdbi.withHandle<List<String>, Exception> { h ->
        h.createQuery("SELECT status FROM task WHERE workflow_id = :wf AND sequence_number = :seq ORDER BY enqueued_at")
            .bind("wf", wfId).bind("seq", seq).mapTo(String::class.java).list()
    }

private suspend fun complete(wfId: String, def: WorkflowDefinition, actName: String, result: String? = null) {
    val seq = seqOf(def, actName)
    val tasks = taskRepo.findByWorkflowAndSequence(wfId, seq)
    for (t in tasks.filter { it.status == TaskStatus.PENDING || it.status == TaskStatus.PROCESSING }) {
        gate.onTaskCompleted(t.id, wfId, seq, TaskStatus.COMPLETED, result)
    }
}
```

- [ ] **Step 2: Spec item 37 — linear DAG end-to-end → COMPLETED**

```kotlin
// ── Spec item 37 ─────────────────────────────────────────────────────

@Test
fun `linear DAG end-to-end reaches COMPLETED`() = runTest {
    val def = workflow {
        activity("step1") { transition("s1.h"); next("step2") }
        activity("step2") { transition("s2.h"); next("step3") }
        activity("step3") { transition("s3.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    complete(wfId, def, "step1")
    complete(wfId, def, "step2")
    complete(wfId, def, "step3")

    assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
}
```

- [ ] **Step 3: Spec items 38–39 — Conditional routing**

```kotlin
// ── Spec item 38 ─────────────────────────────────────────────────────

@Test
fun `conditional routing SUCCESS path correct branch runs other SKIPPED in DB`() = runTest {
    val def = workflow {
        activity("validate") {
            transition("v.h")
            on("OK") { next("charge") }
            on("INVALID") { next("reject") }
        }
        activity("charge") { transition("c.h"); next("done") }
        activity("reject") { transition("r.h"); next("done") }
        activity("done")   { transition("d.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    val seqV = seqOf(def, "validate")
    val vTask = taskRepo.findByWorkflowAndSequence(wfId, seqV)[0]
    gate.onTaskCompleted(vTask.id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"OK"}""")

    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "charge")))
    assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "reject")))

    complete(wfId, def, "charge")
    complete(wfId, def, "done")
    assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
}

// ── Spec item 39 ─────────────────────────────────────────────────────

@Test
fun `conditional routing FAIL path correct branch runs other SKIPPED in DB`() = runTest {
    val def = workflow {
        activity("validate") {
            transition("v.h")
            on("OK") { next("charge") }
            on("INVALID") { next("reject") }
        }
        activity("charge") { transition("c.h"); next("done") }
        activity("reject") { transition("r.h"); next("done") }
        activity("done")   { transition("d.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    val seqV = seqOf(def, "validate")
    val vTask = taskRepo.findByWorkflowAndSequence(wfId, seqV)[0]
    gate.onTaskCompleted(vTask.id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"INVALID"}""")

    assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "charge")))
    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "reject")))
}
```

- [ ] **Step 4: Spec items 40–42 — Fork and join**

```kotlin
// ── Spec item 40 ─────────────────────────────────────────────────────

@Test
fun `unconditional fork all branch tasks PENDING simultaneously`() = runTest {
    val def = workflow {
        activity("prepare") { transition("p.h"); next("email"); next("crm"); next("audit") }
        activity("email")   { transition("e.h") }
        activity("crm")     { transition("c.h") }
        activity("audit")   { transition("a.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    complete(wfId, def, "prepare")

    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "email")))
    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "crm")))
    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "audit")))
}

// ── Spec item 41 ─────────────────────────────────────────────────────

@Test
fun `fork and join dispatches join only after all branches COMPLETED`() = runTest {
    val def = workflow {
        activity("prepare") { transition("p.h"); next("b1"); next("b2") }
        activity("b1")      { transition("b1.h"); next("join") }
        activity("b2")      { transition("b2.h"); next("join") }
        activity("join")    { transition("j.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    complete(wfId, def, "prepare")
    complete(wfId, def, "b1")

    // Join not yet dispatched
    assertTrue(taskStatusAt(wfId, seqOf(def, "join")).isEmpty())

    complete(wfId, def, "b2")

    // Now join dispatched
    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "join")))
}

// ── Spec item 42 ─────────────────────────────────────────────────────

@Test
fun `asymmetric fork timing join waits for slow branch`() = runTest {
    val def = workflow {
        activity("start")  { transition("s.h"); next("fast"); next("slow") }
        activity("fast")   { transition("f.h"); next("join") }
        activity("slow")   { transition("sl.h"); next("join") }
        activity("join")   { transition("j.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    complete(wfId, def, "start")
    complete(wfId, def, "fast")
    assertTrue(taskStatusAt(wfId, seqOf(def, "join")).isEmpty(), "Join must wait for slow branch")

    complete(wfId, def, "slow")
    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "join")))
}
```

- [ ] **Step 5: Spec item 43 — Fan-out embedded in DAG**

```kotlin
// ── Spec item 43 ─────────────────────────────────────────────────────

@Test
fun `fan-out embedded in DAG reaches COMPLETED`() = runTest {
    val def = workflow {
        activity("scatter") {
            transition("sc.h")
            fanOut { transition("par.h"); retries(1) }
            next("join")
        }
        activity("join") { transition("j.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    val seqScatter = seqOf(def, "scatter")
    val scatterTask = taskRepo.findByWorkflowAndSequence(wfId, seqScatter)[0]
    gate.onTaskCompleted(scatterTask.id, wfId, seqScatter, TaskStatus.COMPLETED, """["item-a","item-b"]""")

    val seqParallel = buildSequenceMap(def).values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
    val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
    assertEquals(2, parTasks.size)

    for (t in parTasks) {
        gate.onTaskCompleted(t.id, wfId, seqParallel, TaskStatus.COMPLETED, null)
    }

    complete(wfId, def, "join")
    assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
}
```

- [ ] **Step 6: Spec items 44–45 — SKIPPED fan-out and multi-level cascade**

```kotlin
// ── Spec item 44 ─────────────────────────────────────────────────────

@Test
fun `fan-out on skipped branch skips scatter parallel and successors in DB`() = runTest {
    val def = workflow {
        activity("route") {
            transition("r.h")
            on("RUN") { next("scatter") }
            on("SKIP") { next("done") }
        }
        activity("scatter") {
            transition("sc.h")
            fanOut { transition("par.h") }
            next("done")
        }
        activity("done") { transition("d.h") }
    }
    val seqMap = buildSequenceMap(def)
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    val seqRoute = seqOf(def, "route")
    val routeTask = taskRepo.findByWorkflowAndSequence(wfId, seqRoute)[0]
    gate.onTaskCompleted(routeTask.id, wfId, seqRoute, TaskStatus.COMPLETED, """{"branch":"SKIP"}""")

    assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "scatter")))
    assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber))
    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "done")))
}

// ── Spec item 45 ─────────────────────────────────────────────────────

@Test
fun `multi-level skip cascade persisted correctly`() = runTest {
    val def = workflow {
        activity("a") {
            transition("a.h")
            on("GO") { next("b") }
            on("NO") { next("x") }
        }
        activity("b") { transition("b.h"); next("c") }
        activity("c") { transition("c.h"); next("d") }
        activity("d") { transition("d.h") }
        activity("x") { transition("x.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    val seqA = seqOf(def, "a")
    val aTask = taskRepo.findByWorkflowAndSequence(wfId, seqA)[0]
    gate.onTaskCompleted(aTask.id, wfId, seqA, TaskStatus.COMPLETED, """{"branch":"NO"}""")

    // b, c, d all SKIPPED; x PENDING
    assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "b")))
    assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "c")))
    assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "d")))
    assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "x")))
}
```

- [ ] **Step 7: Spec item 46 — CAS race**

```kotlin
// ── Spec item 46 ─────────────────────────────────────────────────────

@Test
fun `CAS race two workers completing fork branches simultaneously no duplicate join dispatch`() = runTest {
    val def = workflow {
        activity("start") { transition("s.h"); next("b1"); next("b2") }
        activity("b1")    { transition("b1.h"); next("join") }
        activity("b2")    { transition("b2.h"); next("join") }
        activity("join")  { transition("j.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId
    complete(wfId, def, "start")

    val seqB1 = seqOf(def, "b1")
    val seqB2 = seqOf(def, "b2")
    val b1Task = taskRepo.findByWorkflowAndSequence(wfId, seqB1)[0]
    val b2Task = taskRepo.findByWorkflowAndSequence(wfId, seqB2)[0]

    // Complete both branches concurrently
    awaitAll(
        async { gate.onTaskCompleted(b1Task.id, wfId, seqB1, TaskStatus.COMPLETED, null) },
        async { gate.onTaskCompleted(b2Task.id, wfId, seqB2, TaskStatus.COMPLETED, null) },
    )

    val seqJoin = seqOf(def, "join")
    val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
    assertEquals(1, joinTasks.size, "Exactly one join task must exist despite concurrent completions")
}
```

- [ ] **Step 8: Spec item 47 — Sweeper recovery**

```kotlin
// ── Spec item 47 ─────────────────────────────────────────────────────

@Test
fun `worker death after CAS before task insert sweeper re-dispatches`() = runTest {
    val def = workflow {
        activity("a") { transition("a.h"); next("b") }
        activity("b") { transition("b.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    // Simulate: complete task at seq 1 directly in DB without dispatching seq 2
    // (engine crashed between CAS and task insert)
    jdbi.useHandle<Exception> { h ->
        h.createUpdate(
            """UPDATE task SET status = 'COMPLETED', completed_at = SYSTIMESTAMP
               WHERE workflow_id = :wfId AND sequence_number = 1"""
        ).bind("wfId", wfId).execute()
        h.createUpdate(
            "UPDATE workflow SET version = version + 1, updated_at = :cutoff WHERE id = :wfId"
        ).bind("wfId", wfId)
            .bind("cutoff", java.time.LocalDateTime.now(java.time.ZoneOffset.UTC).minusMinutes(10))
            .execute()
    }

    // Sweeper triggers recovery
    gate.recoverStuckWorkflow(wfId)

    val seqB = seqOf(def, "b")
    await atMost java.time.Duration.ofSeconds(5) untilAsserted {
        val bTasks = runBlocking { taskRepo.findByWorkflowAndSequence(wfId, seqB) }
        assertEquals(1, bTasks.size)
        assertEquals(TaskStatus.PENDING, bTasks[0].status)
    }
}
```

- [ ] **Step 9: Spec item 48 — Dead-letter replay**

```kotlin
// ── Spec item 48 ─────────────────────────────────────────────────────

@Test
fun `replayWorkflow on failed DAG resumes from correct activity`() = runTest {
    val def = workflow {
        activity("step1") { transition("s1.h"); next("step2") }
        activity("step2") { transition("s2.h"); next("step3") }
        activity("step3") { transition("s3.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    // Complete step1 then FAIL step2
    complete(wfId, def, "step1")
    val seqS2 = seqOf(def, "step2")
    val s2Tasks = taskRepo.findByWorkflowAndSequence(wfId, seqS2)
    gate.onTaskCompleted(s2Tasks[0].id, wfId, seqS2, TaskStatus.FAILED, null)

    assertEquals(WorkflowStatus.FAILED, workflowRepo.findById(wfId)!!.status)

    // Replay — step2 task should go back to PENDING (not restart from step1)
    engine.replayWorkflow(wfId)

    assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)!!.status)
    val step2After = taskRepo.findByWorkflowAndSequence(wfId, seqS2)
    assertTrue(step2After.any { it.status == TaskStatus.PENDING }, "step2 must be PENDING after replay")
}
```

- [ ] **Step 10: Spec items 49–50 — Deadline and cancel**

```kotlin
// ── Spec item 49 ─────────────────────────────────────────────────────

@Test
fun `workflow deadline exceeded mid-DAG marks TIMED_OUT and cancels PENDING tasks`() = runTest {
    val def = workflow {
        deadline(java.time.Duration.ofMillis(1)) // expires immediately
        activity("step1") { transition("s1.h"); next("step2") }
        activity("step2") { transition("s2.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId

    Thread.sleep(50) // Let deadline expire

    // Watchdog expires the workflow
    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        val wf = workflowRepo.findByIdWithHandle(handle, wfId)!!
        workflowRepo.updateStatusWithHandle(handle, wfId, WorkflowStatus.TIMED_OUT, WorkflowStatus.RUNNING)
        taskRepo.cancelPendingTasksWithHandle(handle, wfId)
    }

    val wf = workflowRepo.findById(wfId)!!
    assertEquals(WorkflowStatus.TIMED_OUT, wf.status)

    val tasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
    assertTrue(tasks.all { it.status == TaskStatus.CANCELLED || it.status == TaskStatus.TIMED_OUT })
}

// ── Spec item 50 ─────────────────────────────────────────────────────

@Test
fun `cancel API mid-fork marks CANCELLED and cancels PENDING branch tasks`() = runTest {
    val def = workflow {
        activity("start") { transition("s.h"); next("b1"); next("b2") }
        activity("b1")    { transition("b1.h") }
        activity("b2")    { transition("b2.h") }
    }
    val result = engine.startWorkflow(def)
    val wfId = result.workflowId
    complete(wfId, def, "start")

    // Both b1 and b2 are now PENDING
    engine.cancelWorkflow(wfId)

    val wf = workflowRepo.findById(wfId)!!
    assertEquals(WorkflowStatus.CANCELLED, wf.status)

    val b1Tasks = taskRepo.findByWorkflowAndSequence(wfId, seqOf(def, "b1"))
    val b2Tasks = taskRepo.findByWorkflowAndSequence(wfId, seqOf(def, "b2"))
    assertTrue(b1Tasks.all { it.status == TaskStatus.CANCELLED })
    assertTrue(b2Tasks.all { it.status == TaskStatus.CANCELLED })
}
```

- [ ] **Step 11: Run all integration tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowIntegrationTest,DefaultPhaseGateTest,WorkflowEngineTest,WorkflowWatchdogTest,SchemaTest,RepositoryTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 12: Run coverage check**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn verify -pl WorkFlow`

Then: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`

Expected: All thresholds met

- [ ] **Step 13: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`

Expected: `BUILD SUCCESS` — all tests pass

- [ ] **Step 14: Commit**

```bash
git add src/test/kotlin/workflow/
git commit -m "test: add DAG integration tests spec items 37-50 (conditional, fork/join, fan-out, race, sweeper, replay)"
```

---

### Task 3: Update stress tests

**Files:**
- Modify: `src/test/kotlin/stress/StressTestBase.kt`
- Modify: stress test subclasses that reference old API

- [ ] **Step 1: Update `StressTestBase.kt`**

Remove `AdvancementStrategyRegistry`. Update `DefaultPhaseGate` construction. Update any workflow definitions using old DSL.

- [ ] **Step 2: Run stress tests (can use small scale)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="CorrectnessStressTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/stress/
git commit -m "fix: update stress tests for DAG model (no currentSequence, new DSL)"
```
