# Phase 4: Multi-Terminal DAG Integration Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add integration tests that verify workflow completion when a DAG has multiple independent terminal nodes at different depths, reached via conditional routing and skip cascades.

**Architecture:** Test-only. No production code changes.

**Tech Stack:** JUnit 5, kotlinx-coroutines, Oracle Testcontainer

**Spec:** `docs/superpowers/specs/2026-04-05-phase-gate-refactoring-design.md` — Phase 4

**Depends on:** Phase 2 (lock-based DefaultPhaseGate)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt` | Modify | Add `MultiTerminalDagCompletion` nested test class |

---

## Test Topology

```
          ┌──► fast (terminal, depth 1)
start ──┤
          └──► router ──(A)──► deep1 ──► deep2 (terminal, depth 3)
                       └──(B)──► alt (terminal, depth 2)
```

---

### Task 1: Add multi-terminal DAG tests

**Files:**
- Modify: `src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt`

- [ ] **Step 1: Add the test class**

Add this `@Nested` class inside `WorkflowIntegrationTest`, after the existing test classes (before the closing `}` of `WorkflowIntegrationTest`):

```kotlin
    // ═══════════════════════════════════════════════════════════════════════
    // Multi-terminal DAG: asymmetric depth + conditional routing
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class MultiTerminalDagCompletion {

        //          ┌──► fast (terminal, depth 1)
        // start ──┤
        //          └──► router ──(A)──► deep1 ──► deep2 (terminal, depth 3)
        //                       └──(B)──► alt (terminal, depth 2)
        private val multiTerminalDef = workflow {
            activity("start")  { transition("s.h"); next("fast"); next("router") }
            activity("fast")   { transition("f.h") }
            activity("router") {
                transition("r.h")
                on("A") { next("deep1") }
                on("B") { next("alt") }
            }
            activity("deep1")  { transition("d1.h"); next("deep2") }
            activity("deep2")  { transition("d2.h") }
            activity("alt")    { transition("a.h") }
        }

        @Test
        fun `branch A taken — terminals at depth 1 and depth 3`() = runBlocking {
            val wfId = engine.startWorkflow(multiTerminalDef).workflowId

            // Fork: start → fast PENDING, router PENDING
            complete(wfId, multiTerminalDef, "start")
            assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(multiTerminalDef, "fast")))
            assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(multiTerminalDef, "router")))

            // Route to A: deep1 PENDING, alt SKIPPED
            val seqRouter = seqOf(multiTerminalDef, "router")
            val routerTask = taskRepo.findByWorkflowAndSequence(wfId, seqRouter)[0]
            gate.onTaskCompleted(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"A"}""")

            assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(multiTerminalDef, "deep1")))
            assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(multiTerminalDef, "alt")))
            assertTrue(taskStatusAt(wfId, seqOf(multiTerminalDef, "deep2")).isEmpty())

            // Complete fast (terminal at depth 1) — workflow still RUNNING
            complete(wfId, multiTerminalDef, "fast")
            assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)!!.status)

            // Complete deep1 → deep2 PENDING
            complete(wfId, multiTerminalDef, "deep1")
            assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(multiTerminalDef, "deep2")))
            assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)!!.status)

            // Complete deep2 (terminal at depth 3) → workflow COMPLETED
            complete(wfId, multiTerminalDef, "deep2")
            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
        }

        @Test
        fun `branch B taken — terminals at depth 1 and 2, skip cascade to depth 3`() = runBlocking {
            val wfId = engine.startWorkflow(multiTerminalDef).workflowId

            complete(wfId, multiTerminalDef, "start")

            // Route to B: alt PENDING, deep1 SKIPPED, deep2 SKIPPED (cascade)
            val seqRouter = seqOf(multiTerminalDef, "router")
            val routerTask = taskRepo.findByWorkflowAndSequence(wfId, seqRouter)[0]
            gate.onTaskCompleted(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"B"}""")

            assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(multiTerminalDef, "alt")))
            assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(multiTerminalDef, "deep1")))
            assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(multiTerminalDef, "deep2")))

            // Complete alt — workflow RUNNING (fast still PENDING)
            complete(wfId, multiTerminalDef, "alt")
            assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)!!.status)

            // Complete fast → workflow COMPLETED (mix of COMPLETED and SKIPPED terminals)
            complete(wfId, multiTerminalDef, "fast")
            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
        }

        @Test
        fun `branch A taken, fast completes first — early terminal does not short-circuit`() = runBlocking {
            val wfId = engine.startWorkflow(multiTerminalDef).workflowId

            complete(wfId, multiTerminalDef, "start")

            // Fast completes before router — workflow still RUNNING
            complete(wfId, multiTerminalDef, "fast")
            assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)!!.status)

            // Route to A
            val seqRouter = seqOf(multiTerminalDef, "router")
            val routerTask = taskRepo.findByWorkflowAndSequence(wfId, seqRouter)[0]
            gate.onTaskCompleted(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"A"}""")

            assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(multiTerminalDef, "deep1")))
            assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(multiTerminalDef, "alt")))

            // Complete deep chain
            complete(wfId, multiTerminalDef, "deep1")
            assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)!!.status)

            complete(wfId, multiTerminalDef, "deep2")
            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
        }

        @Test
        fun `concurrent terminal completions produce exactly one COMPLETED transition`() = runBlocking {
            val wfId = engine.startWorkflow(multiTerminalDef).workflowId

            complete(wfId, multiTerminalDef, "start")

            // Route to B: alt and fast are the two independent terminals
            val seqRouter = seqOf(multiTerminalDef, "router")
            val routerTask = taskRepo.findByWorkflowAndSequence(wfId, seqRouter)[0]
            gate.onTaskCompleted(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"B"}""")

            // Both terminals ready: fast (seq for fast) and alt (seq for alt)
            val seqFast = seqOf(multiTerminalDef, "fast")
            val seqAlt = seqOf(multiTerminalDef, "alt")
            val fastTask = taskRepo.findByWorkflowAndSequence(wfId, seqFast)[0]
            val altTask = taskRepo.findByWorkflowAndSequence(wfId, seqAlt)[0]

            // Complete both concurrently
            awaitAll(
                async(Dispatchers.Default) {
                    gate.onTaskCompleted(fastTask.id, wfId, seqFast, TaskStatus.COMPLETED, null)
                },
                async(Dispatchers.Default) {
                    gate.onTaskCompleted(altTask.id, wfId, seqAlt, TaskStatus.COMPLETED, null)
                },
            )

            // Workflow must reach COMPLETED (not stuck in RUNNING)
            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)

            // No duplicate tasks at any sequence
            for (actName in listOf("fast", "router", "alt", "deep1", "deep2")) {
                val seq = seqOf(multiTerminalDef, actName)
                val count = countTasksDirect(wfId, seq)
                assertTrue(count <= 1, "Duplicate tasks at $actName (seq $seq): $count")
            }
        }
    }
```

- [ ] **Step 2: Run the new tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="WorkflowIntegrationTest$MultiTerminalDagCompletion" -pl .`
Expected: All 4 tests PASS.

- [ ] **Step 3: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl .`
Expected: All tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/workflow/adapter/persistent/WorkflowIntegrationTest.kt
git commit -m "test(workflow): add multi-terminal DAG integration tests with asymmetric depth"
```
