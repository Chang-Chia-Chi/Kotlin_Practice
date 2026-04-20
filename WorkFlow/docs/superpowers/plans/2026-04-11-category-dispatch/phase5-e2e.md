# Phase 5 — E2E Test Variant for Category Filtering

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove end-to-end that a cron trigger scoped to a single category actually narrows the dispatched configs. Extend `DispatchE2EHappyPathTest` with a variant that seeds configs across two categories and asserts only the scoped category reaches the join.

**Architecture:** Update the fixture JSON to tag `CFG-A` with `URGENT` and leave `CFG-B` / `CFG-C` as `NORMAL`. Add a new `@Test` method to `DispatchE2EHappyPathTest` that reuses the class-level fixture wiring and the `@InjectMock` repos, overrides the `findActiveConfigs` stub for the `setOf(URGENT)` matcher, starts the workflow with `initialItem = """{"categories":["URGENT"]}"""`, and polls Oracle via `await()` until the workflow is COMPLETED with exactly one simulation task.

**Tech Stack:** Quarkus test framework (`@QuarkusTest`, `@InjectMock`, `@QuarkusTestResource`), Awaitility, Mockito Kotlin, `DispatchE2EFixture`, JDBI for task-table introspection.

---

## Task 1 — Tag fixture configs with distinct categories

**Files:**
- Modify: `src/test/resources/fixtures/dispatch-e2e-fixture.json`

The fixture currently has three configs (`CFG-A`, `CFG-B`, `CFG-C`) all tagged `NORMAL` by Phase 1. Retag so a category filter produces a visibly different result set.

- [ ] **Step 1: Change `CFG-A` to `URGENT`; leave `CFG-B` and `CFG-C` as `NORMAL`**

Edit the JSON so `CFG-A`'s header reads:

```json
{
  "id": "CFG-A",
  "category": "URGENT",
  "mode": "QTY",
  ...
}
```

`CFG-B` and `CFG-C` stay `NORMAL`.

- [ ] **Step 2: Re-run the existing happy-path test to confirm the "no categories — all configs" path still dispatches all three**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchE2EHappyPathTest
```
Expected: BUILD SUCCESS. The existing `full dispatch pipeline completes with CSV and Parquet artifacts` test calls `engine.startWorkflow(dispatchWorkflow)` with no `initialItem`, so the scatter handler's cron branch falls through to `emptySet()` and fetches all active configs regardless of category.

---

## Task 2 — Add the URGENT-scoped variant test

**Files:**
- Modify: `src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt`

The existing test class injects `engine: WorkflowLifecycle`, `workflowRepo`, `objectMapper`, `jdbi`, and `@InjectMock` repos for config / candidate / baseline. It uses Awaitility (`await().atMost(...)`) to poll Oracle tables until tasks are COMPLETED. The new test reuses all of that.

- [ ] **Step 1: Add the necessary imports**

Add at the top of the file (alongside the existing imports):

```kotlin
import com.workflow.dispatch.model.DispatchCategory
import com.workflow.workflow.model.StartResult
import org.mockito.kotlin.eq
```

- [ ] **Step 2: Add the new test method**

Place this method inside the `DispatchE2EHappyPathTest` class, adjacent to the existing `full dispatch pipeline completes with CSV and Parquet artifacts` test:

```kotlin
@Test
fun `cron trigger scoped to URGENT category dispatches only URGENT configs`() {
    // Override the all-configs stub set up in setupMocks() with a URGENT-scoped stub.
    // Mockito uses the most recently declared matching whenever(), so this supersedes
    // the broader `any(), any()` stub for the URGENT matcher.
    val urgentConfigs = fixture.configs().filter { it.category == DispatchCategory.URGENT }
    assertEquals(1, urgentConfigs.size, "fixture must contain exactly one URGENT config (CFG-A)")
    runBlocking {
        whenever(
            configRepo.findActiveConfigs(any<LocalDateTime>(), eq(setOf(DispatchCategory.URGENT))),
        ).thenReturn(urgentConfigs)
    }

    // Drive the workflow with a URGENT-scoped payload — mirroring what DispatchScheduler.triggerUrgent() sends.
    val workflowId = runBlocking {
        val result = engine.startWorkflow(
            definition = dispatchWorkflow,
            idempotencyKey = "dispatch-URGENT-e2e-token",
            initialItem = """{"categories":["URGENT"]}""",
        )
        (result as StartResult.Created).workflowId
    }

    // Await simulation tasks — there should be exactly ONE (for CFG-A only).
    await().atMost(30, TimeUnit.SECONDS).untilAsserted {
        val tasks = findTasksByWorkflowId(workflowId)
        val simulationTasks = tasks.filter { it["HANDLER_KEY"] == "DispatchSimulationHandler" }
        assertEquals(
            1,
            simulationTasks.size,
            "URGENT-scoped dispatch should produce exactly one simulation task, got ${simulationTasks.size}",
        )
        assertTrue(
            simulationTasks.all { it["STATUS"] == "COMPLETED" },
            "Simulation task should be COMPLETED",
        )
    }

    // Await join + workflow terminal state.
    await().atMost(15, TimeUnit.SECONDS).untilAsserted {
        val tasks = findTasksByWorkflowId(workflowId)
        val joinTask = tasks.find { it["HANDLER_KEY"] == "DispatchJoinHandler" }
        assertEquals("COMPLETED", joinTask?.get("STATUS"), "Join task should be COMPLETED")
    }
    await().atMost(30, TimeUnit.SECONDS).untilAsserted {
        runBlocking {
            val wf = workflowRepo.findById(workflowId)
            assertEquals(WorkflowStatus.COMPLETED, wf?.status, "Workflow should be COMPLETED")
        }
    }
}
```

- [ ] **Step 3: Update `setupMocks()` to use the widened `findActiveConfigs` stub**

`setupMocks()` currently writes:

```kotlin
whenever(configRepo.findActiveConfigs(any<LocalDateTime>())).thenReturn(configs)
```

After Phase 3, the interface takes two arguments. If this wasn't already updated during Phase 3 Task 2 Step 4, update it now to:

```kotlin
whenever(configRepo.findActiveConfigs(any<LocalDateTime>(), any())).thenReturn(configs)
```

This broad stub remains the baseline for the existing happy-path test. The new URGENT-scoped test in Step 2 installs a more specific override.

- [ ] **Step 4: Run only this test class**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -q -Dtest=DispatchE2EHappyPathTest
```
Expected: PASS for both the original happy-path test and the new URGENT-scoped variant.

**Common failures to watch for:**

- **Matcher ordering.** If the existing broad stub (`any(), any()` returning all three configs) wins over the specific URGENT stub, the test will see three simulation tasks. In Mockito, the later-declared stub wins among matchers that both match, so declaring the URGENT stub inside the test body (after `@BeforeEach setupMocks()`) should give it precedence. If not, switch the test to explicitly reset and re-stub: `clearInvocations(configRepo)` then re-declare both matchers in the desired order.
- **Wrong number of simulation tasks.** If two or three tasks appear, either (a) the URGENT stub isn't being hit and the broad stub is returning all three configs, or (b) something upstream of the handler is ignoring the `categories` payload. Check with `verify(configRepo).findActiveConfigs(any(), eq(setOf(DispatchCategory.URGENT)))`.
- **Workflow never reaches COMPLETED.** The Oracle/Minio test containers must be healthy. Docker Desktop must be running (see CLAUDE.md). If `setupMocks()` didn't stub `queryCandidates` and `loadBaseline` for `CFG-A`, the simulation task will fail. The existing `setupMocks()` already stubs these for every config in the fixture, so `CFG-A` is covered.

---

## Task 3 — Coverage check

- [ ] **Step 1: Run the coverage script and verify thresholds**

Run:
```
python .claude/scripts/coverage.py
```
Expected: instruction coverage > 85%, branch coverage > 70%, consistent with the project's CLAUDE.md gates.

If coverage regresses on any file touched by this plan, add a targeted unit test to restore it and commit separately before moving on.

---

## Task 4 — Commit

- [ ] **Step 1: Stage**

```bash
git add src/test/resources/fixtures/dispatch-e2e-fixture.json
```
```bash
git add src/test/kotlin/dispatch/DispatchE2EHappyPathTest.kt
```

- [ ] **Step 2: Commit**

```bash
git commit -m "✅ test(dispatch): E2E variant asserts URGENT-scoped cron dispatches only URGENT configs

Tags CFG-A as URGENT in the E2E fixture and adds a new happy-path
variant that drives the workflow with initialItem={\"categories\":[\"URGENT\"]}.
Asserts exactly one simulation task is created and only CFG-A is
consulted — proving the end-to-end path from scheduler payload through
scatter handler through repository filter."
```

- [ ] **Step 3: Verify**

```bash
git status
```
Expected: working tree clean of Phase 5 changes.

---

## Plan completion checklist

After Phase 5 commits, the full plan is done. Final sanity pass:

- [ ] Five commits land on the branch, one per phase.
- [ ] `mvn test` passes end-to-end.
- [ ] `python .claude/scripts/coverage.py` meets the project gates.
- [ ] No `dispatch.cron` property reference survives anywhere in the codebase except the three per-category replacements.
- [ ] `DispatchConfig` constructions everywhere in `src/` use named arguments and include `category`.
- [ ] The spec file `docs/superpowers/specs/2026-04-11-category-dispatch-design.md` matches what shipped. If anything diverged during implementation, update the spec now and note the divergence in the final commit message.
