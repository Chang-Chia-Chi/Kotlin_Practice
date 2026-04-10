package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.TaskCompletionEvent
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.createTaskForActivity
import com.workflow.workflow.model.workflowId
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.time.temporal.ChronoUnit
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

    // -- Helpers ----------------------------------------------------------------

    private suspend fun startAndGetSeq(def: WorkflowDefinition): Pair<String, Int> {
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId
        return wfId to 1
    }

    private suspend fun completeTask(taskId: String, wfId: String, seq: Int, result: String? = null) {
        gate.onTaskCompleted(TaskCompletionEvent(taskId, wfId, seq, TaskStatus.COMPLETED, result))
    }

    private fun taskStatusAt(wfId: String, seq: Int): List<String> =
        jdbi.withHandle<List<String>, Exception> { h ->
            h.createQuery("SELECT status FROM task WHERE workflow_id = :wfId AND sequence_number = :seq")
                .bind("wfId", wfId).bind("seq", seq).mapTo(String::class.java).list()
        }

    private fun workflowStatus(wfId: String): WorkflowStatus =
        kotlinx.coroutines.runBlocking { workflowRepo.findById(wfId) }!!.status

    // -- Spec item 12: Linear completion -> successor dispatched ----------------

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

    // -- Spec item 13: Terminal activity completes -> workflow COMPLETED --------

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

    // -- Spec item 14: Parallel join incomplete -> no dispatch ------------------

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

        // Complete 'b' only -- join should NOT dispatch yet
        val seqB = seqMap.values.first { it.activityName == "b" }.sequenceNumber
        val bTasks = taskRepo.findByWorkflowAndSequence(wfId, seqB)
        completeTask(bTasks[0].id, wfId, seqB)

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
        assertTrue(joinTasks.isEmpty(), "Join must not dispatch until all predecessors are terminal")
    }

    // -- Spec item 15: Parallel join complete -> successor dispatched -----------

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

        fun completeByName(name: String) {
            val seq = seqMap.values.first { it.activityName == name }.sequenceNumber
            val t = kotlinx.coroutines.runBlocking { taskRepo.findByWorkflowAndSequence(wfId, seq) }[0]
            kotlinx.coroutines.runBlocking { completeTask(t.id, wfId, seq) }
        }

        completeByName("a")
        completeByName("b")
        completeByName("c")

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
        assertEquals(1, joinTasks.size)
        assertEquals(TaskStatus.PENDING, joinTasks[0].status)
    }

    // -- Spec item 16: Conditional SUCCESS branch -> correct task + SKIPPED -----

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
        gate.onTaskCompleted(TaskCompletionEvent(vTasks[0].id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"OK"}"""))

        val seqCharge = seqMap.values.first { it.activityName == "charge" }.sequenceNumber
        val seqReject = seqMap.values.first { it.activityName == "reject" }.sequenceNumber

        val chargeTasks = taskRepo.findByWorkflowAndSequence(wfId, seqCharge)
        assertEquals(1, chargeTasks.size)
        assertEquals(TaskStatus.PENDING, chargeTasks[0].status)

        val rejectTasks = taskRepo.findByWorkflowAndSequence(wfId, seqReject)
        assertEquals(1, rejectTasks.size)
        assertEquals(TaskStatus.SKIPPED, rejectTasks[0].status)
    }

    // -- Spec item 17: Conditional FAIL branch -> correct task + SKIPPED --------

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
        gate.onTaskCompleted(TaskCompletionEvent(vTasks[0].id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"INVALID"}"""))

        val seqCharge = seqMap.values.first { it.activityName == "charge" }.sequenceNumber
        val seqReject = seqMap.values.first { it.activityName == "reject" }.sequenceNumber

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqCharge))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqReject))
    }

    // -- Spec item 18: Skip cascade ---------------------------------------------

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
        // Take branch X -- skip-chain and skip-next never execute
        gate.onTaskCompleted(TaskCompletionEvent(aTasks[0].id, wfId, seqA, TaskStatus.COMPLETED, """{"branch":"X"}"""))

        val seqSkipChain = seqMap.values.first { it.activityName == "skip-chain" }.sequenceNumber
        val seqSkipNext = seqMap.values.first { it.activityName == "skip-next" }.sequenceNumber

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqSkipChain))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqSkipNext))
    }

    // -- Spec item 19: Fork -> both branch tasks inserted in one transaction ----

    @Test
    fun `fork inserts both branch tasks in one transaction`() = runTest {
        val def = workflow {
            activity("prepare") { transition("p.h"); next("email"); next("crm") }
            activity("email") { transition("e.h") }
            activity("crm") { transition("c.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqP = seqMap.values.first { it.activityName == "prepare" }.sequenceNumber
        val pTasks = taskRepo.findByWorkflowAndSequence(wfId, seqP)
        completeTask(pTasks[0].id, wfId, seqP)

        val seqEmail = seqMap.values.first { it.activityName == "email" }.sequenceNumber
        val seqCrm = seqMap.values.first { it.activityName == "crm" }.sequenceNumber

        assertEquals(1, taskRepo.findByWorkflowAndSequence(wfId, seqEmail).size)
        assertEquals(1, taskRepo.findByWorkflowAndSequence(wfId, seqCrm).size)
    }

    // -- Spec item 22: Dispatch guard -- no double insert -----------------------

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

        fun completeByName(name: String) {
            val seq = seqMap.values.first { it.activityName == name }.sequenceNumber
            val t = kotlinx.coroutines.runBlocking { taskRepo.findByWorkflowAndSequence(wfId, seq) }[0]
            kotlinx.coroutines.runBlocking { completeTask(t.id, wfId, seq) }
        }

        completeByName("a")
        completeByName("b")
        completeByName("c")

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        // join should have exactly ONE task despite two predecessors completing
        val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
        assertEquals(1, joinTasks.size)
    }

    // -- Spec item 24: ABORT failure -> workflow FAILED -------------------------

    @Test
    fun `ABORT failed activity marks workflow FAILED`() = runTest {
        val def = workflow {
            activity("risky") {
                transition("r.h")
                next("never")
            }
            activity("never") { transition("n.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqR = seqMap.values.first { it.activityName == "risky" }.sequenceNumber
        val rTasks = taskRepo.findByWorkflowAndSequence(wfId, seqR)
        gate.onTaskCompleted(TaskCompletionEvent(rTasks[0].id, wfId, seqR, TaskStatus.FAILED, null))

        val wf = workflowRepo.findById(wfId)
        assertEquals(WorkflowStatus.FAILED, wf!!.status)
    }

    // -- Spec item 26: SCATTER completes -> N parallel tasks created ------------

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
        gate.onTaskCompleted(TaskCompletionEvent(
            scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
            resultJson = null, itemsJson = """["item1","item2","item3"]""",
        ))

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parallelTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        assertEquals(3, parallelTasks.size)
        assertTrue(parallelTasks.all { it.status == TaskStatus.PENDING })
    }

    // -- Spec item 27: PARALLEL join passes -> successor dispatched -------------

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
        gate.onTaskCompleted(TaskCompletionEvent(scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED, resultJson = null, itemsJson = """["i1","i2"]"""))

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        for (t in parTasks) {
            gate.onTaskCompleted(TaskCompletionEvent(t.id, wfId, seqParallel, TaskStatus.COMPLETED, null))
        }

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqJoin))
    }

    // -- Spec item 28: PARALLEL join fails (ABORT) -> workflow FAILED -----------

    @Test
    fun `PARALLEL join failure with ABORT marks workflow FAILED`() = runTest {
        // parallel join failure aborts the workflow
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
        gate.onTaskCompleted(TaskCompletionEvent(scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED, resultJson = null, itemsJson = """["i1","i2"]"""))

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        // Fail all parallel tasks
        for (t in parTasks) {
            gate.onTaskCompleted(TaskCompletionEvent(t.id, wfId, seqParallel, TaskStatus.FAILED, null))
        }

        assertEquals(WorkflowStatus.FAILED, workflowStatus(wfId))
    }

    // -- Spec item 29: Fan-out activity SKIPPED -> SCATTER + PARALLEL + successors SKIPPED

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
        // Take SKIP branch -- scatter never runs
        gate.onTaskCompleted(TaskCompletionEvent(routeTasks[0].id, wfId, seqRoute, TaskStatus.COMPLETED, """{"branch":"SKIP"}"""))

        val seqScatter = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqScatter))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqParallel))
    }

    // -- Coverage gap 1: SCATTER completes with null itemsJson throws ---------------

    @Test
    fun `SCATTER completion with null itemsJson throws IllegalStateException`() = runTest {
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

        org.junit.jupiter.api.assertThrows<IllegalStateException> {
            gate.onTaskCompleted(TaskCompletionEvent(
                scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
                resultJson = null, itemsJson = null,
            ))
        }
    }

    // -- Coverage gap 2: Idempotent completion (updateStatus returns false) -------

    @Test
    fun `idempotent completion is a no-op and does not create duplicate successors`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h") }
        }
        val (wfId, _) = startAndGetSeq(def)

        val seq1Tasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
        assertEquals(1, seq1Tasks.size)
        val taskId = seq1Tasks[0].id

        // First completion: should dispatch successor "b"
        gate.onTaskCompleted(TaskCompletionEvent(taskId, wfId, 1, TaskStatus.COMPLETED, null))
        val seq2Tasks = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(1, seq2Tasks.size)
        assertEquals(TaskStatus.PENDING, seq2Tasks[0].status)

        // Second completion of the same task: updateStatusWithHandle returns false
        // because task is already in terminal status. Should be a complete no-op.
        gate.onTaskCompleted(TaskCompletionEvent(taskId, wfId, 1, TaskStatus.COMPLETED, null))

        // Assert: still exactly 1 task for "b" (no duplicate successor created)
        val seq2TasksAfter = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(1, seq2TasksAfter.size)
    }

    // -- Bug 2: signal exception must not propagate to caller or corrupt state ----

    @Test
    fun `signal exception does not propagate out of onTaskCompleted and tx2 is still committed`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h") }
        }
        val (wfId, _) = startAndGetSeq(def)
        val task = taskRepo.findByWorkflowAndSequence(wfId, 1).first()

        // Use a local gate wired to a notifier that throws on every signal
        val failingNotifier = FakeWorkerNotifier().apply { failQueues = setOf("default") }
        val localGate = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, failingNotifier)

        // Must NOT throw despite notifier.signal throwing
        localGate.onTaskCompleted(TaskCompletionEvent(task.id, wfId, 1, TaskStatus.COMPLETED, null))

        // TX2 committed: successor task "b" was inserted
        val seq2Tasks = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(1, seq2Tasks.size, "Successor task must exist — TX2 must have committed before signal")
        assertEquals(TaskStatus.PENDING, seq2Tasks[0].status)
    }

    // -- G2: Workflow already FAILED between TX1 and TX2 — no routing occurs ----

    @Test
    fun `G2 workflow FAILED between TX1 and TX2 results in no routing`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqA = seqMap.values.first { it.activityName == "a" }.sequenceNumber
        val aTasks = taskRepo.findByWorkflowAndSequence(wfId, seqA)

        // Simulate watchdog marking workflow FAILED before gate TX2 runs
        jdbi.useHandle<Exception> { h ->
            h.execute("UPDATE workflow SET status = 'FAILED' WHERE id = ?", wfId)
        }

        gate.onTaskCompleted(TaskCompletionEvent(aTasks[0].id, wfId, seqA, TaskStatus.COMPLETED, null))

        // No successor dispatched
        val seqB = seqMap.values.first { it.activityName == "b" }.sequenceNumber
        val bTasks = taskRepo.findByWorkflowAndSequence(wfId, seqB)
        assertTrue(bTasks.isEmpty(), "No successor must be dispatched when workflow is already FAILED")

        // Workflow status remains FAILED
        assertEquals(WorkflowStatus.FAILED, workflowStatus(wfId))
    }

    // -- G5: SCATTER with non-array itemsJson throws ----------------------------

    @Test
    fun `G5 SCATTER with non-array itemsJson throws IllegalStateException`() = runTest {
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

        org.junit.jupiter.api.assertThrows<IllegalStateException> {
            gate.onTaskCompleted(TaskCompletionEvent(
                scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
                resultJson = null, itemsJson = """{"not":"array"}""",
            ))
        }
    }

    // -- G6: SCATTER with empty items array aborts workflow --------------------
    // Empty items list is treated as PhaseDecision.Abort: workflow transitions to FAILED.

    @Test
    fun `G6 SCATTER with empty items array aborts workflow to FAILED`() = runTest {
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

        gate.onTaskCompleted(TaskCompletionEvent(
            scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
            resultJson = null, itemsJson = """[]""",
        ))

        val wf = workflowRepo.findById(wfId)
        assertEquals(WorkflowStatus.FAILED, wf?.status)
    }

    // -- G7a: assembleChildItem merges scatter resultJson with item, item fields win on collision

    @Test
    fun `G7a assembleChildItem merges scatter result with item — item fields win on collision`() = runTest {
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

        gate.onTaskCompleted(TaskCompletionEvent(
            scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
            resultJson = """{"shared":"ctx","key":"from-scatter"}""",
            itemsJson = """[{"key":"from-item","extra":"data"}]""",
        ))

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parallelTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        assertEquals(1, parallelTasks.size)

        val itemMap = objectMapper.readValue<Map<String, Any>>(parallelTasks[0].item!!)
        assertEquals("ctx", itemMap["shared"], "shared field must come from scatter result")
        assertEquals("from-item", itemMap["key"], "item field must win over scatter on collision")
        assertEquals("data", itemMap["extra"], "extra field must come from item")
    }

    // -- G7b: assembleChildItem with plain string item returns it unchanged -----

    @Test
    fun `G7b assembleChildItem with plain string item returns it unchanged`() = runTest {
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

        gate.onTaskCompleted(TaskCompletionEvent(
            scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
            resultJson = """{"shared":"context"}""",
            itemsJson = """["plain-string"]""",
        ))

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parallelTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        assertEquals(1, parallelTasks.size)
        assertEquals("plain-string", parallelTasks[0].item, "Non-object rawItem must be returned as-is")
    }

    // -- G11: TIMED_OUT task with ABORT policy marks workflow FAILED ------------

    @Test
    fun `G11 TIMED_OUT task with ABORT policy marks workflow FAILED`() = runTest {
        val def = workflow {
            activity("step") {
                transition("s.h")            }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqStep = seqMap.values.first { it.activityName == "step" }.sequenceNumber
        val tasks = taskRepo.findByWorkflowAndSequence(wfId, seqStep)

        gate.onTaskCompleted(TaskCompletionEvent(tasks[0].id, wfId, seqStep, TaskStatus.TIMED_OUT, null))

        assertEquals(WorkflowStatus.FAILED, workflowStatus(wfId))
    }

    // -- G4: ScatterExpand idempotency guard (existingParallelCount > 0) --------

    @Test
    fun `G4 ScatterExpand idempotency guard prevents double-insertion of PARALLEL tasks`() = runTest {
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
        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parallelActivity = seqMap.values.first { it.activityName == "scatter.__parallel__" }.activity
        val scatterTasks = taskRepo.findByWorkflowAndSequence(wfId, seqScatter)

        // Pre-insert 2 PARALLEL task rows — simulates recoverStuckWorkflow having already committed them
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val preInserted = listOf("pre-item-1", "pre-item-2").map { item ->
            createTaskForActivity(wfId, "scatter.__parallel__", seqParallel, parallelActivity, now, item = item)
        }
        jdbi.useHandle<Exception> { h ->
            taskRepo.insertBatchWithHandle(h, preInserted)
        }

        // Call gate with itemsJson containing 2 items — guard must detect existing rows and short-circuit
        gate.onTaskCompleted(TaskCompletionEvent(
            scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
            resultJson = null, itemsJson = """["item1","item2"]""",
        ))

        // Assert: exactly the 2 pre-inserted tasks — no additional tasks created
        val allParallel = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        assertEquals(2, allParallel.size, "ScatterExpand guard must not double-insert PARALLEL tasks")
    }

    // -- Spec 4: Definition cache correctness across multiple completions -----

    @Test
    fun `definition cache serves repeated buildSnapshot calls for same workflowId`() = runTest {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h"); next("c") }
            activity("c") { transition("c.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        // Complete a -> dispatches b
        val seqA = seqMap.values.first { it.activityName == "a" }.sequenceNumber
        val aTasks = taskRepo.findByWorkflowAndSequence(wfId, seqA)
        completeTask(aTasks[0].id, wfId, seqA)

        // Complete b -> dispatches c (buildSnapshot called again, should use cached definition)
        val seqB = seqMap.values.first { it.activityName == "b" }.sequenceNumber
        val bTasks = taskRepo.findByWorkflowAndSequence(wfId, seqB)
        completeTask(bTasks[0].id, wfId, seqB)

        // Complete c -> workflow COMPLETED (cache eviction triggered)
        val seqC = seqMap.values.first { it.activityName == "c" }.sequenceNumber
        val cTasks = taskRepo.findByWorkflowAndSequence(wfId, seqC)
        completeTask(cTasks[0].id, wfId, seqC)

        assertEquals(WorkflowStatus.COMPLETED, workflowStatus(wfId))
    }

    @Test
    fun `cache eviction on terminal status allows fresh cache for new workflow with same definition`() = runTest {
        val def = workflow {
            activity("only") { transition("o.h") }
        }

        // Workflow 1: start and complete
        val result1 = engine.startWorkflow(def)
        val wfId1 = result1.workflowId
        val tasks1 = taskRepo.findByWorkflowAndSequence(wfId1, 1)
        completeTask(tasks1[0].id, wfId1, 1)
        assertEquals(WorkflowStatus.COMPLETED, workflowStatus(wfId1))

        // Workflow 2: start and complete (exercises fresh cache entry after eviction)
        val result2 = engine.startWorkflow(def)
        val wfId2 = result2.workflowId
        val tasks2 = taskRepo.findByWorkflowAndSequence(wfId2, 1)
        completeTask(tasks2[0].id, wfId2, 1)
        assertEquals(WorkflowStatus.COMPLETED, workflowStatus(wfId2))
    }
}
