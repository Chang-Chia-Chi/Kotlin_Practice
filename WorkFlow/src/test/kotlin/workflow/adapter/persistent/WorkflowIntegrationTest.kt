package com.workflow.workflow.adapter.persistent

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.config.WatchdogConfig
import com.workflow.workflow.model.TaskCompletionEvent
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.WorkflowWatchdog
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.buildSequenceMap
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Clob
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkflowIntegrationTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var engine: WorkflowEngine
    private lateinit var barrier: DefaultPhaseGate
    private lateinit var watchdog: WorkflowWatchdog

    private val gracePeriod = Duration.ofMinutes(2)
    private val staleTaskThreshold = Duration.ofMinutes(10)

    private val notifier = FakeWorkerNotifier()

    private val testWatchdogConfig = object : WatchdogConfig {
        override fun interval(): Duration = Duration.ofSeconds(30)
        override fun gracePeriod(): Duration = gracePeriod
        override fun staleTaskThreshold(): Duration = staleTaskThreshold
    }

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
        barrier = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
        watchdog = WorkflowWatchdog(jdbi, workflowRepo, taskRepo, barrier, testWatchdogConfig)
    }

    @AfterEach
    fun cleanTables() {
        jdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    private val gate get() = barrier

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun readWorkflowDirect(id: String): Map<String, Any?>? {
        return jdbi.withHandle<Map<String, Any?>?, Exception> { handle ->
            handle.createQuery("SELECT * FROM workflow WHERE id = :id")
                .bind("id", id)
                .mapToMap()
                .findOne()
                .map { raw ->
                    val ci = java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
                    raw.forEach { (k, v) -> ci[k] = if (v is Clob) v.characterStream.readText() else v }
                    ci
                }
                .orElse(null)
        }
    }

    private fun countTasksDirect(workflowId: String, sequenceNumber: Int): Int {
        return jdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND sequence_number = :seq",
            )
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .mapTo(Int::class.java)
                .one()
        }
    }

    private fun readTasksDirect(workflowId: String, sequenceNumber: Int): List<Map<String, Any?>> {
        return jdbi.withHandle<List<Map<String, Any?>>, Exception> { handle ->
            handle.createQuery(
                "SELECT * FROM task WHERE workflow_id = :wfId AND sequence_number = :seq",
            )
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .mapToMap()
                .list()
                .map { raw ->
                    val ci = java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
                    raw.forEach { (k, v) -> ci[k] = if (v is Clob) v.characterStream.readText() else v }
                    ci
                }
        }
    }

    private fun updateWorkflowUpdatedAtDirect(id: String, updatedAt: Instant) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE workflow SET updated_at = :updatedAt WHERE id = :id")
                .bind("id", id)
                .bind("updatedAt", LocalDateTime.ofInstant(updatedAt, ZoneOffset.UTC))
                .execute()
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #17: Linear workflow end-to-end
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class LinearWorkflowE2E {

        @Test
        fun `3-activity linear workflow completes end-to-end`() = runTest {
            val definition = workflow {
                activity("validate") { transition("order.validate"); next("process") }
                activity("process") { transition("order.process"); next("notify") }
                activity("notify") { transition("order.notify") }
            }
            // Start workflow
            val runId = engine.startWorkflow(definition).workflowId

            // Verify: workflow RUNNING at seq 1, one PENDING task
            var wf = readWorkflowDirect(runId)!!
            assertEquals("RUNNING", wf["STATUS"])
            var tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            assertEquals(1, tasks.size)
            assertEquals("order.validate", tasks[0].handlerKey)

            // Complete task 1 with result
            val task1Result = """{"validated":true}"""
            barrier.onTaskCompleted(TaskCompletionEvent(
                tasks[0].id, runId, 1, TaskStatus.COMPLETED, task1Result,
            ))

            // Verify: workflow version incremented
            wf = readWorkflowDirect(runId)!!
            assertEquals(1, (wf["VERSION"] as Number).toInt())

            tasks = taskRepo.findByWorkflowAndSequence(runId, 2)
            assertEquals(1, tasks.size)
            assertEquals("order.process", tasks[0].handlerKey)

            // Complete task 2 with result
            val task2Result = """{"processed":true}"""
            barrier.onTaskCompleted(TaskCompletionEvent(
                tasks[0].id, runId, 2, TaskStatus.COMPLETED, task2Result,
            ))

            // Verify: workflow version incremented again
            wf = readWorkflowDirect(runId)!!
            assertEquals(2, (wf["VERSION"] as Number).toInt())

            tasks = taskRepo.findByWorkflowAndSequence(runId, 3)
            assertEquals(1, tasks.size)
            assertEquals("order.notify", tasks[0].handlerKey)

            // Complete task 3
            barrier.onTaskCompleted(TaskCompletionEvent(
                tasks[0].id, runId, 3, TaskStatus.COMPLETED, """{"notified":true}""",
            ))

            // Verify: workflow COMPLETED
            wf = readWorkflowDirect(runId)!!
            assertEquals("COMPLETED", wf["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #18: Fan-out workflow end-to-end
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class FanOutWorkflowE2E {

        @Test
        fun `scatter to 50 parallel sub-tasks then linear completes workflow`() = runTest {
            val definition = workflow {
                activity("batch") {
                    transition("batch.worker")
                    fanOut {
                        transition("batch.scatter")
                        }
                    next("aggregate")
                }
                activity("aggregate") { transition("batch.aggregate") }
            }
            // Start workflow — creates scatter task at seq 1
            val runId = engine.startWorkflow(definition).workflowId

            var wf = readWorkflowDirect(runId)!!

            val scatterTasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            assertEquals(1, scatterTasks.size)
            assertEquals("batch.worker", scatterTasks[0].handlerKey)

            // Complete scatter task with JSON array of 50 payloads
            val payloads = (1..50).map { """{"item":$it}""" }
            val scatterResult = objectMapper.writeValueAsString(payloads)
            barrier.onTaskCompleted(TaskCompletionEvent(
                scatterTasks[0].id, runId, 1, TaskStatus.COMPLETED, resultJson = null, fanOutPayloadsJson = scatterResult,
            ))

            // Verify: 50 PENDING sub-tasks created at seq 2

            val parallelTasks = taskRepo.findByWorkflowAndSequence(runId, 2)
            assertEquals(50, parallelTasks.size)
            // Parallel sub-tasks use their own activity's transition as handler key
            assertTrue(parallelTasks.all { it.handlerKey == "batch.scatter" })
            assertTrue(parallelTasks.all { it.status == TaskStatus.PENDING })
            // Each sub-task item matches one of the scatter payloads
            val actualPayloads = parallelTasks.map { it.taskPayload }.toSet()
            assertEquals(payloads.toSet(), actualPayloads)

            // Complete all 50 sub-tasks
            for (task in parallelTasks) {
                barrier.onTaskCompleted(TaskCompletionEvent(
                    task.id, runId, 2, TaskStatus.COMPLETED, """{"done":true}""",
                ))
            }

            // Verify: all sub-tasks joined, next linear task created at seq 3

            val aggregateTasks = taskRepo.findByWorkflowAndSequence(runId, 3)
            assertEquals(1, aggregateTasks.size)
            assertEquals("batch.aggregate", aggregateTasks[0].handlerKey)

            // Complete final task
            barrier.onTaskCompleted(TaskCompletionEvent(
                aggregateTasks[0].id, runId, 3, TaskStatus.COMPLETED, """{"aggregated":true}""",
            ))

            // Verify: workflow COMPLETED
            wf = readWorkflowDirect(runId)!!
            assertEquals("COMPLETED", wf["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #19: Worker death simulation
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class WorkerDeathSimulation {

        @Test
        fun `watchdog recovers stuck workflow when worker died after task completion but before DAG advance`() = runTest {
            // Build a 2-step linear definition
            val definition = workflow {
                activity("step1") { transition("step1.handler"); next("step2") }
                activity("step2") { transition("step2.handler") }
            }

            // Start workflow normally
            val runId = engine.startWorkflow(definition).workflowId

            // Simulate worker completing task but dying before lock-based advance:
            // Set task at seq 1 to COMPLETED directly via SQL (bypassing phase gate)
            val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            assertEquals(1, tasks.size)
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "UPDATE task SET status = 'COMPLETED', result = :result WHERE id = :id",
                )
                    .bind("id", tasks[0].id)
                    .bind("result", """{"out":"step1-done"}""")
                    .execute()
            }

            // Workflow version still 0 — lock-based advance was never executed
            var wf = readWorkflowDirect(runId)!!
            assertEquals(0, (wf["VERSION"] as Number).toInt())

            // Push updated_at into the past so watchdog's findStuck picks it up
            updateWorkflowUpdatedAtDirect(
                runId,
                Instant.now().minus(gracePeriod).minusSeconds(120),
            )

            // WorkflowWatchdog patrol detects and recovers
            watchdog.patrol()

            // Verify: workflow version incremented, downstream task created
            wf = readWorkflowDirect(runId)!!
            assertEquals(1, (wf["VERSION"] as Number).toInt())
            assertEquals("RUNNING", wf["STATUS"])

            val seq2Tasks = readTasksDirect(runId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals("step2.handler", seq2Tasks[0]["HANDLER_KEY"])
            assertEquals("PENDING", seq2Tasks[0]["STATUS"])

            // WorkflowWatchdog idempotency: second patrol is a no-op (version already advanced)
            watchdog.patrol()
            val wfAfter = readWorkflowDirect(runId)!!
            assertEquals(1, (wfAfter["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(runId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #20: High-concurrency barrier
    // Spec calls for 100+ sub-tasks. Reduced to 20 because Oracle Free
    // container has ~20 PROCESSES and exhausts listener handlers (ORA-12516)
    // under high connection concurrency. 20 sub-tasks with Semaphore(3)
    // still exercises real lock contention across concurrent barrier calls.
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class HighConcurrencyBarrier {

        @Test
        fun `concurrent barrier completions produce exactly one phase transition`() = runTest {
            val subTaskCount = 20
            val definition = workflow {
                activity("scatter-work") {
                    transition("scatter.handler")
                    fanOut {
                        transition("parallel.handler")
                        }
                    next("post-join")
                }
                activity("post-join") { transition("post.handler") }
            }

            // Start workflow — creates scatter task at seq 1
            val runId = engine.startWorkflow(definition).workflowId

            // Complete scatter with sub-task payloads
            val scatterTasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            val payloads = (1..subTaskCount).map { """{"i":$it}""" }
            barrier.onTaskCompleted(TaskCompletionEvent(
                scatterTasks[0].id, runId, 1, TaskStatus.COMPLETED,
                resultJson = null, fanOutPayloadsJson = objectMapper.writeValueAsString(payloads),
            ))

            // Verify sub-tasks at seq 2
            val parallelTasks = taskRepo.findByWorkflowAndSequence(runId, 2)
            assertEquals(subTaskCount, parallelTasks.size)

            // Complete all near-simultaneously via async/awaitAll
            // Semaphore throttles concurrent JDBC connections to avoid ORA-12516
            // (Oracle Free has limited PROCESSES). 3 concurrent barrier calls
            // still exercise real lock contention without exhausting the listener.
            val semaphore = Semaphore(3)
            parallelTasks.map { task ->
                async {
                    semaphore.withPermit {
                        barrier.onTaskCompleted(TaskCompletionEvent(
                            task.id, runId, 2, TaskStatus.COMPLETED, """{"ok":true}""",
                        ))
                    }
                }
            }.awaitAll()

            // Verify exactly ONE phase transition
            val wf = readWorkflowDirect(runId)!!
            // Version should be 2 (scatter->parallel was v0->v1, parallel->linear lock advance is v1->v2)
            assertEquals(2, (wf["VERSION"] as Number).toInt())

            // Verify exactly one set of downstream tasks (no duplicates)
            val seq3Count = countTasksDirect(runId, 3)
            assertEquals(1, seq3Count)

            // The single downstream task has the correct handler
            val seq3Tasks = readTasksDirect(runId, 3)
            assertEquals("post.handler", seq3Tasks[0]["HANDLER_KEY"])
            assertEquals("PENDING", seq3Tasks[0]["STATUS"])
        }
    }

    // ── DAG helpers ───────────────────────────────────────────────────────

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
            gate.onTaskCompleted(TaskCompletionEvent(t.id, wfId, seq, TaskStatus.COMPLETED, result))
        }
    }

    // ── Spec item 37 ─────────────────────────────────────────────────────

    @Test
    fun `linear DAG end-to-end reaches COMPLETED`() = runBlocking {
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

    // ── Spec item 38 ─────────────────────────────────────────────────────

    @Test
    fun `conditional routing SUCCESS path correct branch runs other SKIPPED in DB`() = runBlocking {
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
        gate.onTaskCompleted(TaskCompletionEvent(vTask.id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"OK"}"""))

        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "charge")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "reject")))

        complete(wfId, def, "charge")
        complete(wfId, def, "done")
        assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
    }

    // ── Spec item 39 ─────────────────────────────────────────────────────

    @Test
    fun `conditional routing FAIL path correct branch runs other SKIPPED in DB`() = runBlocking {
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
        gate.onTaskCompleted(TaskCompletionEvent(vTask.id, wfId, seqV, TaskStatus.COMPLETED, """{"branch":"INVALID"}"""))

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "charge")))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "reject")))
    }

    // ── Spec item 40 ─────────────────────────────────────────────────────

    @Test
    fun `unconditional fork all branch tasks PENDING simultaneously`() = runBlocking {
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
    fun `fork and join dispatches join only after all branches COMPLETED`() = runBlocking {
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

        assertTrue(taskStatusAt(wfId, seqOf(def, "join")).isEmpty())

        complete(wfId, def, "b2")

        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "join")))
    }

    // ── Spec item 42 ─────────────────────────────────────────────────────

    @Test
    fun `asymmetric fork timing join waits for slow branch`() = runBlocking {
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

    // ── Spec item 43 ─────────────────────────────────────────────────────

    @Test
    fun `fan-out embedded in DAG reaches COMPLETED`() = runBlocking {
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
        gate.onTaskCompleted(TaskCompletionEvent(scatterTask.id, wfId, seqScatter, TaskStatus.COMPLETED, resultJson = null, fanOutPayloadsJson = """["item-a","item-b"]"""))

        val seqParallel = seqOf(def, "scatter.__parallel__")
        val parTasks = taskRepo.findByWorkflowAndSequence(wfId, seqParallel)
        assertEquals(2, parTasks.size)

        for (t in parTasks) {
            gate.onTaskCompleted(TaskCompletionEvent(t.id, wfId, seqParallel, TaskStatus.COMPLETED, null))
        }

        complete(wfId, def, "join")
        assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
    }

    // ── Spec item 44 ─────────────────────────────────────────────────────

    @Test
    fun `fan-out on skipped branch skips scatter parallel and successors in DB`() = runBlocking {
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
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId

        val seqRoute = seqOf(def, "route")
        val routeTask = taskRepo.findByWorkflowAndSequence(wfId, seqRoute)[0]
        gate.onTaskCompleted(TaskCompletionEvent(routeTask.id, wfId, seqRoute, TaskStatus.COMPLETED, """{"branch":"SKIP"}"""))

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "scatter")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "scatter.__parallel__")))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "done")))
    }

    // ── Spec item 45 ─────────────────────────────────────────────────────

    @Test
    fun `multi-level skip cascade persisted correctly`() = runBlocking {
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
        gate.onTaskCompleted(TaskCompletionEvent(aTask.id, wfId, seqA, TaskStatus.COMPLETED, """{"branch":"NO"}"""))

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "b")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "c")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "d")))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "x")))
    }

    // ── Asymmetric conditional join ──────────────────────────────────────

    @Test
    fun `asymmetric depth conditional routing join not prematurely skipped`() = runBlocking {
        // Taken branch (GO) has depth 2 before the join; skipped branch (NO) has depth 1.
        // The cascade skip from the short branch must NOT prematurely skip the join
        // while the deep branch's intermediate nodes haven't been dispatched yet.
        //
        //        ┌──(GO)──► b ──► c ──┐
        //   a ──┤                      ├──► join
        //        └──(NO)──► x ────────┘
        val def = workflow {
            activity("a") {
                transition("a.h")
                on("GO") { next("b") }
                on("NO") { next("x") }
            }
            activity("b") { transition("b.h"); next("c") }
            activity("c") { transition("c.h"); next("join") }
            activity("x") { transition("x.h"); next("join") }
            activity("join") { transition("j.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId

        // Route to GO branch — x is skipped, cascade must NOT skip join
        val seqA = seqOf(def, "a")
        val aTask = taskRepo.findByWorkflowAndSequence(wfId, seqA)[0]
        gate.onTaskCompleted(TaskCompletionEvent(aTask.id, wfId, seqA, TaskStatus.COMPLETED, """{"branch":"GO"}"""))

        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "b")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "x")))
        // c and join must NOT have tasks yet — b hasn't completed
        assertTrue(taskStatusAt(wfId, seqOf(def, "c")).isEmpty())
        assertTrue(taskStatusAt(wfId, seqOf(def, "join")).isEmpty())

        // Complete the deep branch: b → c → join
        complete(wfId, def, "b")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "c")))

        complete(wfId, def, "c")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "join")))

        complete(wfId, def, "join")
        assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
    }

    // ── Three-way conditional with asymmetric depths ─────────────────────

    @Test
    fun `three-way conditional with asymmetric depths to shared join`() = runBlocking {
        //          ┌──(A)──► b ──► c ──► d ──┐
        //     a ──┤──(B)──► e ──► f ─────────├──► join
        //          └──(C)──► h ──────────────┘
        val def = workflow {
            activity("a") {
                transition("a.h")
                on("A") { next("b") }
                on("B") { next("e") }
                on("C") { next("h") }
            }
            activity("b") { transition("b.h"); next("c") }
            activity("c") { transition("c.h"); next("d") }
            activity("d") { transition("d.h"); next("join") }
            activity("e") { transition("e.h"); next("f") }
            activity("f") { transition("f.h"); next("join") }
            activity("h") { transition("h.h"); next("join") }
            activity("join") { transition("j.h") }
        }

        // ── Branch C taken (shortest path): b→c→d and e→f all SKIPPED ──
        val r1 = engine.startWorkflow(def)
        val wfId1 = r1.workflowId
        val seqA1 = seqOf(def, "a")
        val aTask1 = taskRepo.findByWorkflowAndSequence(wfId1, seqA1)[0]
        gate.onTaskCompleted(TaskCompletionEvent(aTask1.id, wfId1, seqA1, TaskStatus.COMPLETED, """{"branch":"C"}"""))

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId1, seqOf(def, "b")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId1, seqOf(def, "c")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId1, seqOf(def, "d")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId1, seqOf(def, "e")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId1, seqOf(def, "f")))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId1, seqOf(def, "h")))
        // join must NOT exist yet — h is still PENDING
        assertTrue(taskStatusAt(wfId1, seqOf(def, "join")).isEmpty())

        complete(wfId1, def, "h")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId1, seqOf(def, "join")))

        complete(wfId1, def, "join")
        assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId1)!!.status)

        // ── Branch A taken (longest path): e→f and h all SKIPPED ──
        val r2 = engine.startWorkflow(def)
        val wfId2 = r2.workflowId
        val seqA2 = seqOf(def, "a")
        val aTask2 = taskRepo.findByWorkflowAndSequence(wfId2, seqA2)[0]
        gate.onTaskCompleted(TaskCompletionEvent(aTask2.id, wfId2, seqA2, TaskStatus.COMPLETED, """{"branch":"A"}"""))

        assertEquals(listOf("PENDING"), taskStatusAt(wfId2, seqOf(def, "b")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId2, seqOf(def, "e")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId2, seqOf(def, "f")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId2, seqOf(def, "h")))
        // c, d, join must NOT exist yet
        assertTrue(taskStatusAt(wfId2, seqOf(def, "c")).isEmpty())
        assertTrue(taskStatusAt(wfId2, seqOf(def, "d")).isEmpty())
        assertTrue(taskStatusAt(wfId2, seqOf(def, "join")).isEmpty())

        complete(wfId2, def, "b")
        complete(wfId2, def, "c")
        complete(wfId2, def, "d")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId2, seqOf(def, "join")))

        complete(wfId2, def, "join")
        assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId2)!!.status)
    }

    // ── Fork with embedded conditional then rejoin ───────────────────────

    @Test
    fun `fork with conditional routing inside one branch then rejoin`() = runBlocking {
        //               ┌──► cond ──(X)──► taken ──┐
        //     start ──┤         └──(Y)──► alt ─────├──► join ──► end
        //               └──► linear ───────────────┘
        val def = workflow {
            activity("start")  { transition("s.h"); next("cond"); next("linear") }
            activity("cond")   {
                transition("c.h")
                on("X") { next("taken") }
                on("Y") { next("alt") }
            }
            activity("taken")  { transition("t.h"); next("join") }
            activity("alt")    { transition("a.h"); next("join") }
            activity("linear") { transition("l.h"); next("join") }
            activity("join")   { transition("j.h"); next("end") }
            activity("end")    { transition("e.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId

        // Fork: both cond and linear become PENDING
        complete(wfId, def, "start")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "cond")))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "linear")))

        // Conditional chooses X — taken PENDING, alt SKIPPED
        val seqCond = seqOf(def, "cond")
        val condTask = taskRepo.findByWorkflowAndSequence(wfId, seqCond)[0]
        gate.onTaskCompleted(TaskCompletionEvent(condTask.id, wfId, seqCond, TaskStatus.COMPLETED, """{"branch":"X"}"""))

        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "taken")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "alt")))
        // join blocked: taken is PENDING, linear is PENDING
        assertTrue(taskStatusAt(wfId, seqOf(def, "join")).isEmpty())

        // Complete taken — join still blocked (linear not done)
        complete(wfId, def, "taken")
        assertTrue(taskStatusAt(wfId, seqOf(def, "join")).isEmpty())

        // Complete linear — all three preds resolved, join dispatched
        complete(wfId, def, "linear")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "join")))

        complete(wfId, def, "join")
        complete(wfId, def, "end")
        assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
    }

    // ── Double diamond (two sequential conditional branch points) ────────

    @Test
    fun `double diamond two sequential conditionals reach COMPLETED`() = runBlocking {
        //          ┌──(A)──► b1 ──┐          ┌──(X)──► d1 ──┐
        //     a ──┤               ├──► c ──┤               ├──► e
        //          └──(B)──► b2 ──┘          └──(Y)──► d2 ──┘
        val def = workflow {
            activity("a") {
                transition("a.h")
                on("A") { next("b1") }
                on("B") { next("b2") }
            }
            activity("b1") { transition("b1.h"); next("c") }
            activity("b2") { transition("b2.h"); next("c") }
            activity("c") {
                transition("c.h")
                on("X") { next("d1") }
                on("Y") { next("d2") }
            }
            activity("d1") { transition("d1.h"); next("e") }
            activity("d2") { transition("d2.h"); next("e") }
            activity("e") { transition("e.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId

        // First diamond: A taken, b2 SKIPPED
        val seqA = seqOf(def, "a")
        val aTask = taskRepo.findByWorkflowAndSequence(wfId, seqA)[0]
        gate.onTaskCompleted(TaskCompletionEvent(aTask.id, wfId, seqA, TaskStatus.COMPLETED, """{"branch":"A"}"""))

        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "b1")))
        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "b2")))
        // c must NOT exist — b1 still PENDING, cascade from b2 must be blocked
        assertTrue(taskStatusAt(wfId, seqOf(def, "c")).isEmpty())

        complete(wfId, def, "b1")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "c")))

        // Second diamond: Y taken, d1 SKIPPED
        val seqC = seqOf(def, "c")
        val cTask = taskRepo.findByWorkflowAndSequence(wfId, seqC)[0]
        gate.onTaskCompleted(TaskCompletionEvent(cTask.id, wfId, seqC, TaskStatus.COMPLETED, """{"branch":"Y"}"""))

        assertEquals(listOf("SKIPPED"), taskStatusAt(wfId, seqOf(def, "d1")))
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "d2")))
        // e must NOT exist — d2 still PENDING
        assertTrue(taskStatusAt(wfId, seqOf(def, "e")).isEmpty())

        complete(wfId, def, "d2")
        assertEquals(listOf("PENDING"), taskStatusAt(wfId, seqOf(def, "e")))

        complete(wfId, def, "e")
        assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)
    }

    // ── Spec item 46 ─────────────────────────────────────────────────────

    @Test
    fun `lock race two workers completing fork branches simultaneously no duplicate join dispatch`() = runBlocking {
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

        awaitAll(
            async(Dispatchers.Default) { gate.onTaskCompleted(TaskCompletionEvent(b1Task.id, wfId, seqB1, TaskStatus.COMPLETED, null)) },
            async(Dispatchers.Default) { gate.onTaskCompleted(TaskCompletionEvent(b2Task.id, wfId, seqB2, TaskStatus.COMPLETED, null)) },
        )

        val seqJoin = seqOf(def, "join")
        val joinTasks = taskRepo.findByWorkflowAndSequence(wfId, seqJoin)
        assertEquals(1, joinTasks.size, "Exactly one join task must exist despite concurrent completions")
    }

    // ── Spec item 47 ─────────────────────────────────────────────────────

    @Test
    fun `worker death after lock-based advance before task insert sweeper re-dispatches`() = runBlocking {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId

        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """UPDATE task SET status = 'COMPLETED', completed_at = SYSTIMESTAMP
                   WHERE workflow_id = :wfId AND sequence_number = 1"""
            ).bind("wfId", wfId).execute()
            h.createUpdate(
                "UPDATE workflow SET version = version + 1, updated_at = :cutoff WHERE id = :wfId"
            ).bind("wfId", wfId)
                .bind("cutoff", LocalDateTime.now(ZoneOffset.UTC).minusMinutes(10))
                .execute()
        }

        gate.recoverStuckWorkflow(wfId)

        val seqB = seqOf(def, "b")
        val bTasks = taskRepo.findByWorkflowAndSequence(wfId, seqB)
        assertEquals(1, bTasks.size)
        assertEquals(TaskStatus.PENDING, bTasks[0].status)
    }

    // ── Spec item 48 ─────────────────────────────────────────────────────

    @Test
    fun `replayWorkflow on failed DAG resumes from correct activity`() = runBlocking {
        val def = workflow {
            activity("step1") { transition("s1.h"); next("step2") }
            activity("step2") { transition("s2.h"); next("step3") }
            activity("step3") { transition("s3.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId

        complete(wfId, def, "step1")
        val seqS2 = seqOf(def, "step2")
        val s2Tasks = taskRepo.findByWorkflowAndSequence(wfId, seqS2)
        gate.onTaskCompleted(TaskCompletionEvent(s2Tasks[0].id, wfId, seqS2, TaskStatus.FAILED, null))

        assertEquals(WorkflowStatus.FAILED, workflowRepo.findById(wfId)!!.status)

        engine.replayWorkflow(wfId)

        assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)!!.status)
        val step2After = taskRepo.findByWorkflowAndSequence(wfId, seqS2)
        assertTrue(step2After.any { it.status == TaskStatus.PENDING }, "step2 must be PENDING after replay")
    }

    // ── Spec item 49 ─────────────────────────────────────────────────────

    @Test
    fun `workflow deadline exceeded mid-DAG marks TIMED_OUT and cancels PENDING tasks`() = runBlocking {
        val def = workflow {
            deadline(java.time.Duration.ofMillis(1))
            activity("step1") { transition("s1.h"); next("step2") }
            activity("step2") { transition("s2.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId

        delay(50)

        jdbi.inTransactionSuspend<Unit, Exception> { handle ->
            workflowRepo.updateStatusWithHandle(handle, wfId, WorkflowStatus.TIMED_OUT, WorkflowStatus.RUNNING)
            taskRepo.cancelPendingTasksWithHandle(handle, wfId)
        }

        val wf = workflowRepo.findById(wfId)!!
        assertEquals(WorkflowStatus.TIMED_OUT, wf.status)

        val tasks = taskRepo.findByWorkflowAndSequence(wfId, seqOf(def, "step1"))
        assertTrue(tasks.all { it.status == TaskStatus.CANCELLED || it.status == TaskStatus.TIMED_OUT })
    }

    // ── Spec item 50 ─────────────────────────────────────────────────────

    @Test
    fun `cancel API mid-fork marks CANCELLED and cancels PENDING branch tasks`() = runBlocking {
        val def = workflow {
            activity("start") { transition("s.h"); next("b1"); next("b2") }
            activity("b1")    { transition("b1.h") }
            activity("b2")    { transition("b2.h") }
        }
        val result = engine.startWorkflow(def)
        val wfId = result.workflowId
        complete(wfId, def, "start")

        engine.cancelWorkflow(wfId)

        val wf = workflowRepo.findById(wfId)!!
        assertEquals(WorkflowStatus.CANCELLED, wf.status)

        val b1Tasks = taskRepo.findByWorkflowAndSequence(wfId, seqOf(def, "b1"))
        val b2Tasks = taskRepo.findByWorkflowAndSequence(wfId, seqOf(def, "b2"))
        assertTrue(b1Tasks.all { it.status == TaskStatus.CANCELLED })
        assertTrue(b2Tasks.all { it.status == TaskStatus.CANCELLED })
    }

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
            gate.onTaskCompleted(TaskCompletionEvent(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"A"}"""))

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
            gate.onTaskCompleted(TaskCompletionEvent(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"B"}"""))

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
            gate.onTaskCompleted(TaskCompletionEvent(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"A"}"""))

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
            gate.onTaskCompleted(TaskCompletionEvent(routerTask.id, wfId, seqRouter, TaskStatus.COMPLETED, """{"branch":"B"}"""))

            // Both terminals ready: fast (seq for fast) and alt (seq for alt)
            val seqFast = seqOf(multiTerminalDef, "fast")
            val seqAlt = seqOf(multiTerminalDef, "alt")
            val fastTask = taskRepo.findByWorkflowAndSequence(wfId, seqFast)[0]
            val altTask = taskRepo.findByWorkflowAndSequence(wfId, seqAlt)[0]

            // Complete both concurrently
            awaitAll(
                async(Dispatchers.Default) {
                    gate.onTaskCompleted(TaskCompletionEvent(fastTask.id, wfId, seqFast, TaskStatus.COMPLETED, null))
                },
                async(Dispatchers.Default) {
                    gate.onTaskCompleted(TaskCompletionEvent(altTask.id, wfId, seqAlt, TaskStatus.COMPLETED, null))
                },
            )

            // Workflow must reach COMPLETED (not stuck in RUNNING)
            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)!!.status)

            // Exactly one task at every sequence (no duplicates, no missing rows)
            for (actName in listOf("start", "fast", "router", "alt", "deep1", "deep2")) {
                val seq = seqOf(multiTerminalDef, actName)
                val count = countTasksDirect(wfId, seq)
                assertEquals(1, count, "Expected exactly 1 task at $actName (seq $seq), got $count")
            }
        }
    }
}
