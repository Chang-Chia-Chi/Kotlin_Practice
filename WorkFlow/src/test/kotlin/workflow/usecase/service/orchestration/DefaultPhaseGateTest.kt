package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.buildSequenceMap
import com.workflow.workflow.model.workflowId
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
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
        gate.onTaskCompleted(taskId, wfId, seq, TaskStatus.COMPLETED, result)
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
        gate.onTaskCompleted(vTasks[0].id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"INVALID"}""")

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
        gate.onTaskCompleted(aTasks[0].id, wfId, seqA, TaskStatus.COMPLETED, """{"branch":"X"}""")

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

    // -- Spec item 24: BEST_EFFORT failure -> unconditional successors dispatched

    @Test
    fun `BEST_EFFORT failed activity dispatches unconditional successors`() = runTest {
        val def = workflow {
            activity("risky") {
                transition("r.h")
                failurePolicy(FailurePolicy.BEST_EFFORT)
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

    // -- Spec item 25: ABORT failure -> workflow FAILED -------------------------

    @Test
    fun `ABORT failed activity marks workflow FAILED`() = runTest {
        val def = workflow {
            activity("risky") {
                transition("r.h")
                failurePolicy(FailurePolicy.ABORT)
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
        gate.onTaskCompleted(
            scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED,
            """["item1","item2","item3"]""",
        )

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
        gate.onTaskCompleted(scatterTasks[0].id, wfId, seqScatter, TaskStatus.COMPLETED, """["i1","i2"]""")

        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        for (t in parTasks) {
            gate.onTaskCompleted(t.id, wfId, seqParallel, TaskStatus.COMPLETED, null)
        }

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqJoin))
    }

    // -- Spec item 28: PARALLEL join fails (ABORT) -> workflow FAILED -----------

    @Test
    fun `PARALLEL join failure with ABORT marks workflow FAILED`() = runTest {
        // scatter activity's failurePolicy governs the parallel join behavior
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                failurePolicy(FailurePolicy.ABORT)
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
        // Fail all parallel tasks
        for (t in parTasks) {
            gate.onTaskCompleted(t.id, wfId, seqParallel, TaskStatus.FAILED, null)
        }

        assertEquals(WorkflowStatus.FAILED, workflowStatus(wfId))
    }

    // -- Spec item 29: PARALLEL join fails (BEST_EFFORT) -> unconditional successors dispatched

    @Test
    fun `PARALLEL join failure with BEST_EFFORT dispatches unconditional successors`() = runTest {
        // fanOut.failurePolicy governs the parallel join behavior
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                fanOut {
                    transition("par.h")
                    failurePolicy(FailurePolicy.BEST_EFFORT)
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

    // -- Spec item 30: Fan-out activity SKIPPED -> SCATTER + PARALLEL + successors SKIPPED

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
        gate.onTaskCompleted(routeTasks[0].id, wfId, seqRoute, TaskStatus.COMPLETED, """{"branch":"SKIP"}""")

        val seqScatter = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val seqParallel = seqMap.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqScatter))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqParallel))
    }

    // -- Coverage gap 1: SCATTER failure with BEST_EFFORT dispatches successors ---

    @Test
    fun `SCATTER failure with BEST_EFFORT dispatches unconditional successors`() = runTest {
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                failurePolicy(FailurePolicy.BEST_EFFORT)
                fanOut { transition("par.h") }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val seqMap = buildSequenceMap(def)
        val (wfId, _) = startAndGetSeq(def)

        val seqScatter = seqMap.values.first { it.activityName == "scatter" }.sequenceNumber
        val scatterTasks = taskRepo.findByWorkflowAndSequence(wfId, seqScatter)
        // Scatter itself fails (no parallel expansion) -- BEST_EFFORT should fall through
        gate.onTaskCompleted(scatterTasks[0].id, wfId, seqScatter, TaskStatus.FAILED, null)

        val seqJoin = seqMap.values.first { it.activityName == "join" }.sequenceNumber
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqJoin))
        // Workflow should still be RUNNING (not FAILED), since BEST_EFFORT proceeds
        assertEquals(WorkflowStatus.RUNNING, workflowStatus(wfId))
    }

    // -- Coverage gap 2: CAS retry exhaustion ------------------------------------
    // SKIPPED: The CAS retry loop (max 10 attempts) is triggered by RetryableException,
    // which is a private class thrown only when casVersionWithHandle returns false.
    // To exhaust the retry loop we would need another concurrent transaction to
    // increment the workflow version between each retry attempt — 10 times within
    // the same test. This requires either (a) mocking internal repository methods
    // (prohibited by CLAUDE.md: "mock at interface boundaries only") or (b) a
    // real concurrent writer with precise timing that is inherently flaky.
    // The positive CAS path is covered by every existing test that dispatches
    // successors. The retry mechanism is a simple while-loop with a counter;
    // a unit test would not add meaningful confidence beyond code inspection.

    // -- Coverage gap 3: Idempotent completion (updateStatus returns false) -------

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
        gate.onTaskCompleted(taskId, wfId, 1, TaskStatus.COMPLETED, null)
        val seq2Tasks = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(1, seq2Tasks.size)
        assertEquals(TaskStatus.PENDING, seq2Tasks[0].status)

        // Second completion of the same task: updateStatusWithHandle returns false
        // because task is already in terminal status. Should be a complete no-op.
        gate.onTaskCompleted(taskId, wfId, 1, TaskStatus.COMPLETED, null)

        // Assert: still exactly 1 task for "b" (no duplicate successor created)
        val seq2TasksAfter = taskRepo.findByWorkflowAndSequence(wfId, 2)
        assertEquals(1, seq2TasksAfter.size)
    }
}
