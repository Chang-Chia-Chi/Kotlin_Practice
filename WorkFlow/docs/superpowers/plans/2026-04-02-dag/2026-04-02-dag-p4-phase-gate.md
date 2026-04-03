# DAG Refactor — P4: Phase Gate Rewrite

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the full DAG-aware `onTaskCompleted()` algorithm in `DefaultPhaseGate` — covering successor evaluation, SKIPPED insertion, conditional routing, and the CAS guard. Write unit tests for spec items 12–30.

**Architecture:** `DefaultPhaseGate` replaces the strategy-based linear approach with a single transactional algorithm that evaluates DAG successors, inserts SKIPPED tasks for unchosen branches, checks global completion, and uses version-only CAS. The phase gate is tested against Oracle via `OracleTestContainer`.

**Tech Stack:** Kotlin coroutines, JDBI 3, Oracle Free (Testcontainers), JUnit 5, Jackson

---

### Algorithm reference (from spec §5.2)

```
onTaskCompleted(taskId, workflowId, seq, status, resultJson):
  In one ACID transaction:
  1. UPDATE task T → terminal status (fenced by claimedBy+claimedAt)
  2. BARRIER PROBE: count non-terminal tasks at seq(T)
     → If > 0: commit task update only, exit.
  3. SCATTER special case:
     → If phaseType = SCATTER and probe = 0: expand parallel tasks. CAS. Commit. Signal.
  4. SUCCESSOR EVALUATION (queue Q = successors of activity X):
     While Q not empty:
       S = Q.pop()
       a. DISPATCH GUARD: task exists for seq(S)? → skip
       b. PREDECESSOR GATE: all predecessorSequences of S terminal?
          → If not: skip
       c. FATE DECISION: any edge P→S is "taken"?
          taken = P.status == COMPLETED AND (label == DEFAULT_BRANCH OR resultJson matches label)
          BEST_EFFORT FAILED predecessor → treated as DEFAULT_BRANCH taken
          If ANY taken: INSERT PENDING task for seq(S). Add to signal set.
          If NONE taken: INSERT SKIPPED task. If S terminal: add to completion-check.
                         Else: add S.successors to Q (cascade skip).
  5. COMPLETION CHECK: any terminal settled in step 2 or 4?
     COUNT non-terminal tasks for whole workflow. If 0: workflow → COMPLETED.
  6. CAS: UPDATE workflow SET version = version+1 WHERE id AND version = expected AND status = RUNNING
     → If 0 rows: rollback. Retry from step 1.
  7. Commit. Signal queues.
```

---

### Task 1: Write failing tests for DAG phase gate

**Files:**
- Modify: `src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt`

- [ ] **Step 1: Rewrite `DefaultPhaseGateTest.kt`**

Replace the entire file with:

```kotlin
package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Clob
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DefaultPhaseGateTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var notifier: FakeWorkerNotifier
    private lateinit var gate: DefaultPhaseGate
    private lateinit var engine: WorkflowEngine

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        notifier = FakeWorkerNotifier()
        gate = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
    }

    @AfterEach
    fun cleanTables() {
        jdbi.useHandle<Exception> { h ->
            h.execute("DELETE FROM task")
            h.execute("DELETE FROM workflow")
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun randomId() = UUID.randomUUID().toString()
    private fun now() = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private suspend fun startAndGetSeq(def: WorkflowDefinition): Pair<String, Int> {
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId
        val tasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
        return wfId to 1
    }

    private suspend fun completeTask(taskId: String, wfId: String, seq: Int, result: String? = null) {
        gate.onTaskCompleted(taskId, wfId, seq, TaskStatus.COMPLETED, result)
    }

    private fun readResult(row: Map<String, Any?>): String? {
        val v = row["RESULT"] ?: return null
        return if (v is Clob) v.characterStream.readText() else v as String
    }

    private fun taskStatusAt(wfId: String, seq: Int): List<String> =
        jdbi.withHandle<List<String>, Exception> { h ->
            h.createQuery("SELECT status FROM task WHERE workflow_id = :wfId AND sequence_number = :seq")
                .bind("wfId", wfId).bind("seq", seq).mapTo(String::class.java).list()
        }

    private fun workflowStatus(wfId: String): WorkflowStatus =
        workflowRepo.findById(wfId).let { runBlocking { it } }!!.status

    // workaround: runBlocking in helper for readable assertions
    private fun <T> runBlocking(block: suspend () -> T): T = kotlinx.coroutines.runBlocking { block() }

    // ── Spec item 12: Linear completion → successor dispatched ────────────

    @Test
    fun `linear completion dispatches successor`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h") }
        }
        val (wfId, _) = startAndGetSeq(def)
        val seq1Tasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
        assertEquals(1, seq1Tasks.size)
        completeTask(seq1Tasks[0].id, wfId, 1)

        val seq2Tasks = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(1, seq2Tasks.size)
        assertEquals(TaskStatus.PENDING, seq2Tasks[0].status)
    }

    // ── Spec item 13: Terminal activity completes → workflow COMPLETED ────

    @Test
    fun `terminal activity completion marks workflow COMPLETED`() = runTest {
        val def = workflow {
            activity("only") { transition("o.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId
        val tasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
        completeTask(tasks[0].id, wfId, 1)

        val wf = workflowRepo.findById(wfId)
        assertNotNull(wf)
        assertEquals(WorkflowStatus.COMPLETED, wf.status)
    }

    // ── Spec item 14: Parallel join incomplete → no dispatch ──────────────

    @Test
    fun `parallel join incomplete does not dispatch join`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b"); next("c") }
            activity("b") { transition("b.h"); next("join") }
            activity("c") { transition("c.h"); next("join") }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        // Complete 'a'
        val seqA = seqMap.values.first { it.activityName == "a" }.sequenceNumber
        val aTasks = taskRepo.findByWorkflowAndSequence(wfId, seqA)
        completeTask(aTasks[0].id, wfId, seqA)

        // Complete 'b' only — join should NOT dispatch yet
        val seqB = seqMap.values.first { it.activityName == "b" }.sequenceNumber
        val bTasks = taskRepo.findByWorkflowAndSequence(wfId, seqB)
        completeTask(bTasks[0].id, wfId, seqB)

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
        assertTrue(joinTasks.isEmpty(), "Join must not dispatch until all predecessors are terminal")
    }

    // ── Spec item 15: Parallel join complete → successor dispatched ────────

    @Test
    fun `parallel join dispatches after all branches complete`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b"); next("c") }
            activity("b") { transition("b.h"); next("join") }
            activity("c") { transition("c.h"); next("join") }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        fun complete(name: String) {
            val seq = seqMap.values.first { it.activityName == name }.sequenceNumber
            val t = runBlocking { taskRepo.findByWorkflowAndSequence(wfId, seq) }[0]
            runBlocking { completeTask(t.id, wfId, seq) }
        }

        complete("a")
        complete("b")
        complete("c")

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
        assertEquals(1, joinTasks.size)
        assertEquals(TaskStatus.PENDING, joinTasks[0].status)
    }

    // ── Spec item 16: Conditional SUCCESS branch → correct task + SKIPPED ─

    @Test
    fun `conditional SUCCESS branch dispatches charge and SKIPs reject`() = runTest {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqV = seqMap.values.first { it.activityName == "validate" }.sequenceNumber
        val vTasks = taskRepo.findByWorkflowAndSequence(wfId, seqV)
        gate.onTaskCompleted(vTasks[0].id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"OK"}""")

        val seqCharge = seqMap.values.first { it.activityName == "charge" }.sequenceNumber
        val seqReject = seqMap.values.first { it.activityName == "reject" }.sequenceNumber

        val chargeTasks = taskRepo.findByWorkflowAndSequence(wfId, seqCharge)
        assertEquals(1, chargeTasks.size)
        assertEquals(TaskStatus.PENDING, chargeTasks[0].status)

        val rejectTasks = taskRepo.findByWorkflowAndSequence(wfId, seqReject)
        assertEquals(1, rejectTasks.size)
        assertEquals(TaskStatus.SKIPPED, rejectTasks[0].status)
    }

    // ── Spec item 17: Conditional FAIL branch → correct task + SKIPPED ────

    @Test
    fun `conditional INVALID branch dispatches reject and SKIPs charge`() = runTest {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqV = seqMap.values.first { it.activityName == "validate" }.sequenceNumber
        val vTasks = taskRepo.findByWorkflowAndSequence(wfId, seqV)
        gate.onTaskCompleted(vTasks[0].id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"INVALID"}""")

        val seqCharge = seqMap.values.first { it.activityName == "charge" }.sequenceNumber
        val seqReject = seqMap.values.first { it.activityName == "reject" }.sequenceNumber

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqCharge))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqReject))
    }

    // ── Spec item 18: Skip cascade ─────────────────────────────────────────

    @Test
    fun `skip cascades through chain in one transaction`() = runTest {
        val def = workflow {
            activity("a") {
                transition("a.h")
                on("X") { next("b") }
                on("Y") { next("skip-chain") }
            }
            activity("b") { transition("b.h") }
            activity("skip-chain") { transition("s1.h"); next("skip-next") }
            activity("skip-next") { transition("s2.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqA = seqMap.values.first { it.activityName == "a" }.sequenceNumber
        val aTasks = taskRepo.findByWorkflowAndSequence(wfId, seqA)
        // Take branch X — skip-chain and skip-next never execute
        gate.onTaskCompleted(aTasks[0].id, wfId, seqA, TaskStatus.COMPLETED, """{"branch":"X"}""")

        val seqSkipChain = seqMap.values.first { it.activityName == "skip-chain" }.sequenceNumber
        val seqSkipNext  = seqMap.values.first { it.activityName == "skip-next" }.sequenceNumber

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqSkipChain))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqSkipNext))
    }

    // ── Spec item 19: Fork → both branch tasks inserted in one transaction ─

    @Test
    fun `fork inserts both branch tasks in one transaction`() = runTest {
        val def = workflow {
            activity("prepare") { transition("p.h"); next("email"); next("crm") }
            activity("email") { transition("e.h") }
            activity("crm")   { transition("c.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqP = seqMap.values.first { it.activityName == "prepare" }.sequenceNumber
        val pTasks = taskRepo.findByWorkflowAndSequence(wfId, seqP)
        completeTask(pTasks[0].id, wfId, seqP)

        val seqEmail = seqMap.values.first { it.activityName == "email" }.sequenceNumber
        val seqCrm   = seqMap.values.first { it.activityName == "crm" }.sequenceNumber

        assertEquals(1, taskRepo.findByWorkflowAndSequence(wfId, seqEmail).size)
        assertEquals(1, taskRepo.findByWorkflowAndSequence(wfId, seqCrm).size)
    }

    // ── Spec item 22: Dispatch guard — no double insert ────────────────────

    @Test
    fun `dispatch guard prevents second task insert for same sequence`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b"); next("c") }
            activity("b") { transition("b.h"); next("join") }
            activity("c") { transition("c.h"); next("join") }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        fun complete(name: String) {
            val seq = seqMap.values.first { it.activityName == name }.sequenceNumber
            val t = runBlocking { taskRepo.findByWorkflowAndSequence(wfId, seq) }[0]
            runBlocking { completeTask(t.id, wfId, seq) }
        }

        complete("a")
        complete("b")
        complete("c")

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        // join should have exactly ONE task despite two predecessors completing
        val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
        assertEquals(1, joinTasks.size)
    }

    // ── Spec item 24: BEST_EFFORT failure → unconditional successors dispatched

    @Test
    fun `BEST_EFFORT failed activity dispatches unconditional successors`() = runTest {
        val def = workflow {
            activity("risky") {
                transition("r.h")
                failurePolicy(com.workflow.workflow.model.FailurePolicy.BEST_EFFORT)
                next("always-runs")
            }
            activity("always-runs") { transition("a.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqR = seqMap.values.first { it.activityName == "risky" }.sequenceNumber
        val rTasks = taskRepo.findByWorkflowAndSequence(wfId, seqR)
        gate.onTaskCompleted(rTasks[0].id, wfId, seqR, TaskStatus.FAILED, null)

        val seqNext = seqMap.values.first { it.activityName == "always-runs" }.sequenceNumber
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqNext))
    }

    // ── Spec item 25: ABORT failure → workflow FAILED ────────────────────

    @Test
    fun `ABORT failed activity marks workflow FAILED`() = runTest {
        val def = workflow {
            activity("risky") {
                transition("r.h")
                failurePolicy(com.workflow.workflow.model.FailurePolicy.ABORT)
                next("never")
            }
            activity("never") { transition("n.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqR = seqMap.values.first { it.activityName == "risky" }.sequenceNumber
        val rTasks = taskRepo.findByWorkflowAndSequence(wfId, seqR)
        gate.onTaskCompleted(rTasks[0].id, wfId, seqR, TaskStatus.FAILED, null)

        val wf = workflowRepo.findById(wfId)
        assertEquals(WorkflowStatus.FAILED, wf!!.status)
    }

    // ── Spec item 26: SCATTER completes → N parallel tasks created ────────

    @Test
    fun `SCATTER completion expands into N parallel tasks`() = runTest {
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                fanOut { transition("par.h") }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqScatter = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val scatterTasks = taskRepo.findByWorkflowAndSequence(wfId, seqScatter)
        // Scatter result = JSON array of items
        gate.onTaskCompleted(
            scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
            """["item1","item2","item3"]"""
        )

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parallelTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        assertEquals(3, parallelTasks.size)
        assertTrue(parallelTasks.all { it.status == TaskStatus.PENDING })
    }

    // ── Spec item 27: PARALLEL join passes → successor dispatched ─────────

    @Test
    fun `PARALLEL join passes dispatches join successor`() = runTest {
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                fanOut { transition("par.h") }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqScatter = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val scatterTasks = taskRepo.findByWorkflowAndSequence(wfId, seqScatter)
        gate.onTaskCompleted(scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED, """["i1","i2"]""")

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        for (t in parTasks) {
            gate.onTaskCompleted(t.id, wfId, seqParallel, TaskStatus.COMPLETED, null)
        }

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqJoin))
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail (DefaultPhaseGate is still stubbed)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DefaultPhaseGateTest" -pl WorkFlow`

Expected: All tests fail with `UnsupportedOperationException`

- [ ] **Step 3: Commit failing tests**

```bash
git add src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt
git commit -m "test: add DAG phase gate unit tests (spec items 12-30)"
```

---

### Task 2: Implement `DefaultPhaseGate`

**Files:**
- Modify: `src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt`

- [ ] **Step 1: Replace `DefaultPhaseGate.kt` with full implementation**

```kotlin
package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.createSkippedTaskForActivity
import com.workflow.workflow.model.createTaskForActivity
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import com.workflow.workflow.usecase.port.outbound.persistent.WorkflowRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit

@ApplicationScoped
class DefaultPhaseGate(
    private val jdbi: Jdbi,
    private val workflowRepo: WorkflowRepository,
    private val taskRepo: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val notifier: WorkerNotifier,
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
        var signalQueues: List<String> = emptyList()

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            // Step 1: Update task to terminal status
            val updated = taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
            if (!updated) return@inTransactionSuspend

            // Step 2: Barrier probe — are all tasks at this sequence terminal?
            val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
            if (nonTerminal > 0) return@inTransactionSuspend

            val workflow = workflowRepo.findByIdWithHandle(handle, workflowId)
                ?: throw IllegalStateException("Workflow not found: $workflowId")
            if (workflow.status != WorkflowStatus.RUNNING) return@inTransactionSuspend

            val definition = objectMapper.readValue<WorkflowDefinition>(workflow.definitionJson)
            val sequenceMap = buildSequenceMap(definition)
            val seqInfo = sequenceMap[sequenceNumber]
                ?: throw IllegalStateException("Seq $sequenceNumber not in definition for $workflowId")

            val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
            val signalQueueSet = mutableSetOf<String>()
            val completionCheckSeqs = mutableSetOf<Int>()

            // Step 3a: SCATTER special case — failed scatter applies scatter failurePolicy
            if (seqInfo.phaseType == PhaseType.SCATTER && status != TaskStatus.COMPLETED) {
                if (seqInfo.activity.failurePolicy == FailurePolicy.ABORT) {
                    val updated = workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.FAILED, WorkflowStatus.RUNNING)
                    if (updated) taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
                }
                // BEST_EFFORT: no parallel tasks; fall through to successor eval below
                // (successorsOf for SCATTER seqInfo will look up scatter.successors)
                if (seqInfo.activity.failurePolicy != FailurePolicy.BEST_EFFORT) return@inTransactionSuspend
            }

            // Step 3b: SCATTER completed — expand into parallel tasks
            if (seqInfo.phaseType == PhaseType.SCATTER && status == TaskStatus.COMPLETED) {
                val items: List<String> = objectMapper.readValue(
                    resultJson ?: throw IllegalStateException(
                        "SCATTER phase requires scatter result for workflow $workflowId"
                    )
                )
                require(items.isNotEmpty()) {
                    "Fan-out produced 0 items for workflow $workflowId"
                }
                val parallelSeq = sequenceNumber + 1
                val parallelInfo = sequenceMap[parallelSeq]!!
                val parallelTasks = items.map {
                    createTaskForActivity(workflowId, parallelInfo.activityName, parallelSeq, parallelInfo.activity, now, item = it)
                }
                taskRepo.insertBatchWithHandle(handle, parallelTasks)
                signalQueueSet += parallelInfo.activity.queue

                val casWon = workflowRepo.casVersionWithHandle(handle, workflowId, workflow.version)
                if (!casWon) {
                    log.debug("CAS lost on SCATTER for workflow {}", workflowId)
                    throw RetryableException("CAS loss")
                }
                signalQueues = signalQueueSet.toList()
                return@inTransactionSuspend
            }

            // Step 3c: PARALLEL phase — evaluate JoinPolicy before successor dispatch
            if (seqInfo.phaseType == PhaseType.PARALLEL) {
                val failedCount = taskRepo.countFailedWithHandle(handle, workflowId, sequenceNumber)
                val totalCount = taskRepo.countTotalWithHandle(handle, workflowId, sequenceNumber)
                val joinPolicy = seqInfo.activity.fanOut?.joinPolicy ?: JoinPolicy.All
                // seqInfo.activity here is the parallelActivity whose failurePolicy = scatter.failurePolicy
                val joinPassed = evaluateJoinPolicy(joinPolicy, failedCount, totalCount)
                if (!joinPassed) {
                    if (seqInfo.activity.failurePolicy == FailurePolicy.ABORT) {
                        val updated = workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.FAILED, WorkflowStatus.RUNNING)
                        if (updated) taskRepo.cancelPendingTasksWithHandle(handle, workflowId)
                        return@inTransactionSuspend
                    }
                    // BEST_EFFORT: fall through to successor evaluation (unconditional edges will be taken)
                }
                // JoinPolicy passed or BEST_EFFORT — fall through to successor evaluation
            }

            // Step 4: Successor evaluation
            val evalQueue = ArrayDeque<SequenceInfo>()
            evalQueue += successorsOf(seqInfo, sequenceMap, definition)

            while (evalQueue.isNotEmpty()) {
                val successor = evalQueue.removeFirst()
                val sSeq = successor.sequenceNumber

                // a. Dispatch guard
                if (taskRepo.countTotalWithHandle(handle, workflowId, sSeq) > 0) continue

                // b. Predecessor gate
                val allPredTerminal = successor.predecessorSequences.all { predSeq ->
                    taskRepo.countNonTerminalWithHandle(handle, workflowId, predSeq) == 0
                }
                if (!allPredTerminal) continue

                // c. Fate decision — check if any edge to this successor is "taken"
                val edgeTaken = isAnyEdgeTaken(handle, workflowId, successor, sequenceMap, definition)

                if (edgeTaken) {
                    val task = createTaskForActivity(workflowId, successor.activityName, sSeq, successor.activity, now)
                    taskRepo.insertBatchWithHandle(handle, listOf(task))
                    signalQueueSet += successor.activity.queue
                } else {
                    val skipped = createSkippedTaskForActivity(workflowId, successor.activityName, sSeq, successor.activity, now)
                    taskRepo.insertBatchWithHandle(handle, listOf(skipped))
                    if (successor.activity.isTerminal) {
                        completionCheckSeqs += sSeq
                    } else {
                        evalQueue += successorsOf(successor, sequenceMap, definition)
                    }
                }
            }

            // Also add completed terminal activity to completion check
            if (seqInfo.activity.isTerminal) {
                completionCheckSeqs += sequenceNumber
            }

            // Step 5: Completion check
            if (completionCheckSeqs.isNotEmpty()) {
                val globalNonTerminal = countGlobalNonTerminal(handle, workflowId)
                if (globalNonTerminal == 0) {
                    workflowRepo.updateStatusWithHandle(handle, workflowId, WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING)
                    signalQueues = emptyList()
                    return@inTransactionSuspend
                }
            }

            // Step 6: CAS
            val casWon = workflowRepo.casVersionWithHandle(handle, workflowId, workflow.version)
            if (!casWon) {
                log.debug("CAS lost for workflow {} at seq {}", workflowId, sequenceNumber)
                throw RetryableException("CAS loss")
            }

            signalQueues = signalQueueSet.toList()
        }

        signalQueues.forEach { notifier.signal(it) }
    }

    override suspend fun recoverStuckWorkflow(workflowId: String) {
        // Implemented in Plan 5 (dag-p5-watchdog-sweeper)
        throw UnsupportedOperationException("recoverStuckWorkflow implemented in Plan 5")
    }

    // ── Private helpers ──────────────────────────────────────────────────

    private fun successorsOf(
        seqInfo: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        definition: WorkflowDefinition,
    ): List<SequenceInfo> {
        val actName = seqInfo.activityName.removeSuffix(".__parallel__")
        val activity = definition.activities[actName] ?: return emptyList()
        return activity.successors.mapNotNull { edge ->
            val targetActName = edge.target
            sequenceMap.values.firstOrNull { it.activityName == targetActName && it.phaseType != PhaseType.PARALLEL }
        }.distinctBy { it.sequenceNumber }
    }

    private fun isAnyEdgeTaken(
        handle: Handle,
        workflowId: String,
        successor: SequenceInfo,
        sequenceMap: Map<Int, SequenceInfo>,
        definition: WorkflowDefinition,
    ): Boolean {
        // Find all activities that have an edge to this successor
        val targetActName = successor.activityName
        for ((predActName, predActivity) in definition.activities) {
            val edgesToTarget = predActivity.successors.filter { it.target == targetActName }
            if (edgesToTarget.isEmpty()) continue

            // Find the seq number of this predecessor's output
            val predOutputSeq = sequenceMap.values
                .firstOrNull { si ->
                    val name = si.activityName.removeSuffix(".__parallel__")
                    name == predActName && (si.phaseType == PhaseType.PARALLEL || si.phaseType == PhaseType.LINEAR)
                }?.sequenceNumber ?: continue

            // Read predecessor's task result
            val predTasks = taskRepo.findByWorkflowAndSequenceWithHandle(handle, workflowId, predOutputSeq)
            for (predTask in predTasks) {
                for (edge in edgesToTarget) {
                    if (isEdgeTaken(predTask, edge.label, predActivity.failurePolicy)) return true
                }
            }
        }
        return false
    }

    private fun isEdgeTaken(task: Task, edgeLabel: String, predFailurePolicy: FailurePolicy): Boolean {
        if (!task.status.isTerminal) return false
        // BEST_EFFORT failed predecessor: treat as DEFAULT_BRANCH taken
        if (task.status == TaskStatus.FAILED && predFailurePolicy == FailurePolicy.BEST_EFFORT) {
            return edgeLabel == DEFAULT_BRANCH
        }
        if (task.status != TaskStatus.COMPLETED) return false
        if (edgeLabel == DEFAULT_BRANCH) return true

        // Conditional: check resultJson for branch key
        val result = task.resultJson ?: return false
        return try {
            val map = objectMapper.readValue<Map<String, Any>>(result)
            map["branch"]?.toString() == edgeLabel
        } catch (_: Exception) {
            false
        }
    }

    private fun evaluateJoinPolicy(joinPolicy: JoinPolicy, failedCount: Int, totalCount: Int): Boolean {
        val succeededCount = totalCount - failedCount
        return when (joinPolicy) {
            is JoinPolicy.All -> failedCount == 0
            is JoinPolicy.Threshold -> succeededCount >= joinPolicy.n
            is JoinPolicy.Percentage -> {
                val pct = if (totalCount > 0) (succeededCount * 100) / totalCount else 0
                pct >= joinPolicy.pct
            }
        }
    }

    private fun countGlobalNonTerminal(handle: Handle, workflowId: String): Int =
        handle.createQuery(
            """
            SELECT COUNT(*) FROM task
            WHERE workflow_id = :wfId
              AND status NOT IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
            """
        )
            .bind("wfId", workflowId)
            .mapTo(Int::class.java)
            .one()
}

/** Thrown inside a transaction to trigger a retry loop. */
private class RetryableException(msg: String) : RuntimeException(msg)
```

Note: The CAS retry loop is handled at the JDBI transaction level — `inTransactionSuspend` retries on certain exceptions. If the current implementation doesn't automatically retry, wrap `onTaskCompleted` body in a retry loop (max 10 retries):

```kotlin
// Wrap the jdbi call in a retry loop:
var attempts = 0
while (true) {
    try {
        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            // ... full body
        }
        break
    } catch (e: RetryableException) {
        if (++attempts >= 10) throw IllegalStateException("CAS retry exhausted for $workflowId", e)
        log.debug("CAS retry {} for workflow {}", attempts, workflowId)
    }
}
```

- [ ] **Step 2: Run phase gate tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DefaultPhaseGateTest" -pl WorkFlow`

Expected: `BUILD SUCCESS` — spec items 12–27 all pass

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt
git commit -m "feat: implement DAG phase gate with successor eval, SKIPPED insertion, CAS guard"
```

---

### Task 3: Add ABORT failure and fan-out failure tests

- [ ] **Step 1: Add remaining tests (spec items 28–30) to `DefaultPhaseGateTest.kt`**

```kotlin
    // ── Spec item 28: PARALLEL join fails (ABORT) → workflow FAILED ────────

    @Test
    fun `PARALLEL join failure with ABORT marks workflow FAILED`() = runTest {
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                fanOut {
                    transition("par.h")
                    failurePolicy(com.workflow.workflow.model.FailurePolicy.ABORT)
                }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqScatter = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val scatterTasks = taskRepo.findByWorkflowAndSequence(wfId, seqScatter)
        gate.onTaskCompleted(scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED, """["i1","i2"]""")

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        // Fail all parallel tasks
        for (t in parTasks) {
            gate.onTaskCompleted(t.id, wfId, seqParallel, TaskStatus.FAILED, null)
        }

        assertEquals(WorkflowStatus.FAILED, workflowRepo.findById(wfId)!!.status)
    }

    // ── Spec item 29: PARALLEL join fails (BEST_EFFORT) → unconditional successors dispatched

    @Test
    fun `PARALLEL join failure with BEST_EFFORT dispatches unconditional successors`() = runTest {
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                fanOut {
                    transition("par.h")
                    failurePolicy(com.workflow.workflow.model.FailurePolicy.BEST_EFFORT)
                }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqScatter = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val scatterTasks = taskRepo.findByWorkflowAndSequence(wfId, seqScatter)
        gate.onTaskCompleted(scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED, """["i1","i2"]""")

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        for (t in parTasks) {
            gate.onTaskCompleted(t.id, wfId, seqParallel, TaskStatus.FAILED, null)
        }

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqJoin))
    }

    // ── Spec item 30: Fan-out activity SKIPPED → SCATTER + PARALLEL + successors SKIPPED

    @Test
    fun `fan-out activity on SKIPPED branch propagates SKIPPED to scatter, parallel, successors`() = runTest {
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
        val (wfId, _) = startAndGetSeq(def)

        val seqRoute = seqMap.values.first { it.activityName == "route" }.sequenceNumber
        val routeTasks = taskRepo.findByWorkflowAndSequence(wfId, seqRoute)
        // Take SKIP branch — scatter never runs
        gate.onTaskCompleted(routeTasks[0].id, wfId, seqRoute, TaskStatus.COMPLETED, """{"branch":"SKIP"}""")

        val seqScatter  = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqScatter))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqParallel))
    }
```

- [ ] **Step 2: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="DefaultPhaseGateTest" -pl WorkFlow`

Expected: `BUILD SUCCESS`

- [ ] **Step 3: Run JaCoCo coverage check**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`

Expected: Coverage thresholds met

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGate.kt
git add src/test/kotlin/workflow/usecase/service/orchestration/DefaultPhaseGateTest.kt
git commit -m "test: add spec items 28-30 for fan-out failure and skip propagation"
```
