package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.config.WatchdogConfig
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.WorkflowWatchdog
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine

import com.workflow.workflow.model.buildSequenceMap
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import com.workflow.infrastructure.persistence.OracleTestContainer
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
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkflowWatchdogTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
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

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun randomId(): String = UUID.randomUUID().toString()

    private fun now(): Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    private fun makeWorkflow(
        id: String = randomId(),
        definition: WorkflowDefinition,
        version: Int = 0,
        status: WorkflowStatus = WorkflowStatus.RUNNING,
        createdAt: Instant = now(),
        updatedAt: Instant = now(),
        deadlineAt: Instant = now().plus(Duration.ofHours(1)),
    ): WorkflowRun = WorkflowRun(
        id = id,
        definitionJson = objectMapper.writeValueAsString(definition),
        version = version,
        status = status,
        createdAt = createdAt,
        updatedAt = updatedAt,
        deadlineAt = deadlineAt,
    )

    private fun makeTask(
        id: String = randomId(),
        workflowId: String,
        activityName: String = "test-activity",
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
        notBefore: Instant? = null,
        backoffBase: Int = 1,
        backoffCap: Int = 300,
    ): Task = Task(
        id = id,
        workflowId = workflowId,
        activityName = activityName,
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
        notBefore = notBefore,
        backoffBase = backoffBase,
        backoffCap = backoffCap,
    )

    /** Insert a workflow directly via SQL (independent of repo under test). */
    private fun insertWorkflowDirect(run: WorkflowRun) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at)
                   VALUES (:id, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)""",
            )
                .bind("id", run.id)
                .bind("definition", run.definitionJson)
                .bind("version", run.version)
                .bind("status", run.status.name)
                .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
                .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
                .bind("deadlineAt", LocalDateTime.ofInstant(run.deadlineAt, ZoneOffset.UTC))
                .execute()
        }
    }

    /** Insert a task directly via SQL (independent of repo under test). */
    private fun insertTaskDirect(task: Task) {
        jdbi.useHandle<Exception> { handle ->
            val stmt = handle.createUpdate(
                """INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, item, result,
                   claimed_by, claimed_at, completed_at, retry_count, max_retries, deadline_at, not_before,
                   backoff_base, backoff_cap)
                   VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey, :item, :result,
                   :claimedBy, :claimedAt, :completedAt, :retryCount, :maxRetries, :deadlineAt, :notBefore,
                   :backoffBase, :backoffCap)""",
            )
                .bind("id", task.id)
                .bind("workflowId", task.workflowId)
                .bind("sequenceNumber", task.sequenceNumber)
                .bind("status", task.status.name)
                .bind("handlerKey", task.handlerKey)
                .bind("retryCount", task.retryCount)
                .bind("maxRetries", task.maxRetries)

            fun bindStringOrNull(name: String, value: String?) =
                if (value != null) stmt.bind(name, value) else stmt.bindNull(name, java.sql.Types.VARCHAR)

            fun bindTimestampOrNull(name: String, value: Instant?) =
                if (value != null) stmt.bind(name, LocalDateTime.ofInstant(value, ZoneOffset.UTC))
                else stmt.bindNull(name, java.sql.Types.TIMESTAMP)

            bindStringOrNull("item", task.item)
            bindStringOrNull("result", task.resultJson)
            bindStringOrNull("claimedBy", task.claimedBy)
            bindTimestampOrNull("claimedAt", task.claimedAt)
            bindTimestampOrNull("completedAt", task.completedAt)
            bindTimestampOrNull("deadlineAt", task.deadlineAt)
            bindTimestampOrNull("notBefore", task.notBefore)
            stmt.bind("backoffBase", task.backoffBase)
            stmt.bind("backoffCap", task.backoffCap)

            stmt.execute()
        }
    }

    /** Read a workflow row directly via SQL for assertion. */
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

    /** Count tasks at a given workflow + sequence directly via SQL. */
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

    /** Count tasks at a given workflow + sequence + status directly via SQL. */
    private fun countTasksWithStatusDirect(workflowId: String, sequenceNumber: Int, status: TaskStatus): Int {
        return jdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND sequence_number = :seq AND status = :status",
            )
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .bind("status", status.name)
                .mapTo(Int::class.java)
                .one()
        }
    }

    /** Read a single task by ID directly via SQL for assertion. */
    private fun readTaskDirect(taskId: String): Map<String, Any?>? {
        return jdbi.withHandle<Map<String, Any?>?, Exception> { handle ->
            handle.createQuery("SELECT * FROM task WHERE id = :id")
                .bind("id", taskId)
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

    /** Read all tasks at a given workflow + sequence directly via SQL. */
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

    /** Update workflow updated_at directly via SQL. */
    private fun updateWorkflowUpdatedAtDirect(id: String, updatedAt: Instant) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE workflow SET updated_at = :updatedAt WHERE id = :id")
                .bind("id", id)
                .bind("updatedAt", LocalDateTime.ofInstant(updatedAt, ZoneOffset.UTC))
                .execute()
        }
    }

    /** Update workflow version and updated_at directly via SQL (for simulating concurrent advance). */
    private fun advanceWorkflowDirect(id: String, newVersion: Int) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """UPDATE workflow SET version = :ver, updated_at = :now
                   WHERE id = :id""",
            )
                .bind("id", id)
                .bind("ver", newVersion)
                .bind("now", LocalDateTime.now(ZoneOffset.UTC))
                .execute()
        }
    }

    // ── Workflow Definition Builders ─────────────────────────────────────

    /** Two linear activities: seq 1 -> seq 2. */
    private fun twoStepLinearDef() = workflow {
        activity("step1") { transition("step1.handler"); next("step2") }
        activity("step2") { transition("step2.handler") }
    }

    /** Single activity: seq 1 only. */
    private fun singleStepDef() = workflow {
        activity("only") { transition("only.handler") }
    }

    /** Two linear with BEST_EFFORT on step1. */
    private fun twoStepBestEffortDef() = workflow {
        activity("step1") {
            transition("step1.handler")
            failurePolicy(FailurePolicy.BEST_EFFORT)
            next("step2")
        }
        activity("step2") { transition("step2.handler") }
    }

    /** Diamond DAG: A→B, A→C, B→D, C→D. Topo: A,C,B,D → Seq: A=1, C=2, B=3, D=4. */
    private fun diamondDagDef() = workflow {
        activity("A") { transition("a.handler"); next("B"); next("C") }
        activity("B") { transition("b.handler"); next("D") }
        activity("C") { transition("c.handler"); next("D") }
        activity("D") { transition("d.handler") }
    }

    /** Fan-out then linear: seq 1 (SCATTER) -> seq 2 (PARALLEL) -> seq 3 (LINEAR). */
    private fun fanOutThenLinearDef(joinPolicy: JoinPolicy = JoinPolicy.All) = workflow {
        activity("scatter-activity") {
            transition("scatter.handler")
            fanOut {
                transition("parallel.handler")
                joinPolicy(joinPolicy)
            }
            next("final-step")
        }
        activity("final-step") { transition("final.handler") }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 1: Stuck workflow detected after grace period -> watchdog recovers
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class StuckWorkflowRecoveredAfterGracePeriod {

        @Test
        fun `stuck workflow with all tasks COMPLETED past grace period - patrol advances to next sequence`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(
                id = wfId,
                definition = def,
                
                version = 0,
                updatedAt = pastGrace,
            )
            insertWorkflowDirect(wf)

            // Task at seq 1 already COMPLETED (terminal) — the barrier missed the advance
            insertTaskDirect(
                makeTask(
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.COMPLETED,
                    handlerKey = "step1.handler",
                    resultJson = """{"out":"value"}""",
                ),
            )

            watchdog.patrol()

            // Workflow version incremented, still RUNNING
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(1, (updatedWf["VERSION"] as Number).toInt())
            assertEquals("RUNNING", updatedWf["STATUS"])

            // One new PENDING task created at sequence 2 with step2's handler key
            val seq2Tasks = readTasksDirect(wfId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals("step2.handler", seq2Tasks[0]["HANDLER_KEY"])
            assertEquals("PENDING", seq2Tasks[0]["STATUS"])
        }

        @Test
        fun `last sequence stuck - watchdog marks workflow COMPLETED`() = runTest {
            val def = singleStepDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.COMPLETED, handlerKey = "only.handler",
                ),
            )

            watchdog.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("COMPLETED", row["STATUS"])
        }

        @Test
        fun `all tasks FAILED with ABORT policy - watchdog marks workflow FAILED`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.FAILED, handlerKey = "step1.handler",
                ),
            )

            watchdog.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("FAILED", row["STATUS"])
        }

        @Test
        fun `all tasks FAILED with BEST_EFFORT - watchdog advances to next sequence`() = runTest {
            val def = twoStepBestEffortDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.FAILED, handlerKey = "step1.handler",
                ),
            )

            watchdog.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("RUNNING", row["STATUS"])
            assertEquals(1, countTasksDirect(wfId, 2))
        }

        @Test
        fun `completed task triggers next sequence creation`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.COMPLETED, handlerKey = "step1.handler",
                    resultJson = """{"pipeline":"data"}""",
                ),
            )

            watchdog.patrol()

            val nextTasks = readTasksDirect(wfId, 2)
            assertEquals(1, nextTasks.size)
            assertEquals("step2.handler", nextTasks[0]["HANDLER_KEY"])
        }
    }

    @Nested
    inner class AlreadyTerminalWorkflow {

        @Test
        fun `COMPLETED workflow is not recovered`() = runTest {
            val def = singleStepDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(
                id = wfId, definition = def,
                status = WorkflowStatus.COMPLETED, updatedAt = pastGrace,
            )
            insertWorkflowDirect(wf)

            watchdog.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("COMPLETED", row["STATUS"])
            assertEquals(0, (row["VERSION"] as Number).toInt())
        }

        @Test
        fun `FAILED workflow is not recovered`() = runTest {
            val def = singleStepDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(
                id = wfId, definition = def,
                status = WorkflowStatus.FAILED, updatedAt = pastGrace,
            )
            insertWorkflowDirect(wf)

            watchdog.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("FAILED", row["STATUS"])
            assertEquals(0, (row["VERSION"] as Number).toInt())
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 2: Within grace period -> watchdog skips
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class WithinGracePeriodSkipped {

        @Test
        fun `workflow within grace period - findStuck returns empty, no recovery`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            // updated_at is recent (within grace period)
            val recentUpdate = Instant.now().minusSeconds(30)
            val wf = makeWorkflow(
                id = wfId,
                definition = def,
                
                version = 0,
                updatedAt = recentUpdate,
            )
            insertWorkflowDirect(wf)

            // Task at seq 1 already COMPLETED — same stuck shape, but updated_at is recent
            insertTaskDirect(
                makeTask(
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.COMPLETED,
                    handlerKey = "step1.handler",
                ),
            )

            watchdog.patrol()

            // Workflow NOT advanced — same version
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(0, (updatedWf["VERSION"] as Number).toInt())

            // No tasks created at sequence 2
            assertEquals(0, countTasksDirect(wfId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 3: WorkflowWatchdog CAS loses to worker -> no duplicate downstream tasks
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class WorkflowWatchdogCasLosesToWorker {

        @Test
        fun `worker advances workflow before watchdog patrol - CAS fails, no duplicate tasks`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(
                id = wfId,
                definition = def,
                
                version = 0,
                updatedAt = pastGrace,
            )
            insertWorkflowDirect(wf)

            // Task at seq 1 COMPLETED
            insertTaskDirect(
                makeTask(
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.COMPLETED,
                    handlerKey = "step1.handler",
                ),
            )

            // Simulate worker advancing the workflow before watchdog runs:
            // version=1, updated_at=NOW
            advanceWorkflowDirect(wfId, newVersion = 1)

            // Insert a PENDING task at seq 2 (as the worker-driven advance would have done)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId,
                    sequenceNumber = 2,
                    status = TaskStatus.PENDING,
                    handlerKey = "step2.handler",
                ),
            )

            watchdog.patrol()

            // Workflow not double-advanced — version unchanged from worker advance (watchdog CAS failed)
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(1, (updatedWf["VERSION"] as Number).toInt())

            // Still exactly 1 task at seq 2 — no duplicates
            assertEquals(1, countTasksDirect(wfId, 2))
            assertEquals(1, countTasksWithStatusDirect(wfId, 2, TaskStatus.PENDING))
        }

        @Test
        fun `two concurrent recoverStuckWorkflow calls - exactly one CAS wins`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.COMPLETED, handlerKey = "step1.handler",
                ),
            )

            val results = listOf(
                async { barrier.recoverStuckWorkflow(wfId) },
                async { barrier.recoverStuckWorkflow(wfId) },
            ).awaitAll()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals(1, (row["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(wfId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 4: WorkflowWatchdog fires twice on same stuck workflow -> second is no-op
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class WorkflowWatchdogIdempotency {

        @Test
        fun `patrol twice on same stuck workflow - first recovers, second is no-op`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(
                id = wfId,
                definition = def,
                
                version = 0,
                updatedAt = pastGrace,
            )
            insertWorkflowDirect(wf)

            // Task at seq 1 COMPLETED
            insertTaskDirect(
                makeTask(
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.COMPLETED,
                    handlerKey = "step1.handler",
                ),
            )

            // First patrol — recovers the workflow
            watchdog.patrol()

            // Verify first recovery succeeded
            val afterFirst = readWorkflowDirect(wfId)
            assertNotNull(afterFirst)
            assertEquals(1, (afterFirst["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(wfId, 2))
            assertEquals(1, countTasksWithStatusDirect(wfId, 2, TaskStatus.PENDING))

            // Re-set updated_at to past grace period (make it eligible again by time)
            val pastGraceAgain = Instant.now().minus(gracePeriod).minusSeconds(60)
            updateWorkflowUpdatedAtDirect(wfId, pastGraceAgain)

            // Second patrol — findStuck should NOT return this workflow because
            // seq 2 has a PENDING (non-terminal) task, so NOT EXISTS condition fails
            watchdog.patrol()

            // Verify no change: version still 1, exactly 1 task at seq 2
            val afterSecond = readWorkflowDirect(wfId)
            assertNotNull(afterSecond)
            assertEquals(1, (afterSecond["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(wfId, 2))

            // No tasks created at sequence 3
            assertEquals(0, countTasksDirect(wfId, 3))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 5: expireOverdueTasks — expired PROCESSING tasks marked TIMED_OUT
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class FailExpiredTasks {

        @Test
        fun `expired PROCESSING task past deadline - patrol marks TIMED_OUT and triggers barrier`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val pastGrace = now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)

            // Single PROCESSING task at seq 1 with deadline 30 min in the past
            insertTaskDirect(
                makeTask(
                    id = taskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "dead-worker",
                    claimedAt = now().minus(Duration.ofMinutes(45)),
                    deadlineAt = now().minus(Duration.ofMinutes(30)),
                ),
            )

            watchdog.patrol()

            // Task should be TIMED_OUT
            val task = readTaskDirect(taskId)
            assertNotNull(task)
            assertEquals("TIMED_OUT", task["STATUS"])

            // Only task at seq 1 with ABORT policy -> workflow FAILED
            val wfRow = readWorkflowDirect(wfId)
            assertNotNull(wfRow)
            assertEquals("FAILED", wfRow["STATUS"])
        }

        @Test
        fun `non-expired PROCESSING task - not touched by patrol`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = now())
            insertWorkflowDirect(wf)

            // PROCESSING task with deadline 30 min in the future, claimed just now
            insertTaskDirect(
                makeTask(
                    id = taskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "active-worker",
                    claimedAt = now(),
                    deadlineAt = now().plus(Duration.ofMinutes(30)),
                ),
            )

            watchdog.patrol()

            // Task still PROCESSING, workflow version unchanged
            val task = readTaskDirect(taskId)
            assertNotNull(task)
            assertEquals("PROCESSING", task["STATUS"])

            val wfRow = readWorkflowDirect(wfId)
            assertNotNull(wfRow)
            assertEquals(0, (wfRow["VERSION"] as Number).toInt())
        }

        @Test
        fun `expired task among multiple tasks - only expired ones fail`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val expiredTaskId = randomId()
            val healthyTaskId = randomId()
            val pastGrace = now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)

            // Expired PROCESSING task
            insertTaskDirect(
                makeTask(
                    id = expiredTaskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "dead-worker",
                    claimedAt = now().minus(Duration.ofMinutes(45)),
                    deadlineAt = now().minus(Duration.ofMinutes(30)),
                ),
            )

            // Non-expired PROCESSING task
            insertTaskDirect(
                makeTask(
                    id = healthyTaskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "active-worker",
                    claimedAt = now(),
                    deadlineAt = now().plus(Duration.ofMinutes(30)),
                ),
            )

            watchdog.patrol()

            // Expired task -> TIMED_OUT
            val expiredTask = readTaskDirect(expiredTaskId)
            assertNotNull(expiredTask)
            assertEquals("TIMED_OUT", expiredTask["STATUS"])

            // Healthy task -> still PROCESSING (barrier sees non-terminal, doesn't advance)
            val healthyTask = readTaskDirect(healthyTaskId)
            assertNotNull(healthyTask)
            assertEquals("PROCESSING", healthyTask["STATUS"])

            // Workflow unchanged (still has a non-terminal task)
            val wfRow = readWorkflowDirect(wfId)
            assertNotNull(wfRow)
            assertEquals("RUNNING", wfRow["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 6: reclaimStaleTasks — stale PROCESSING tasks reclaimed or failed
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class ReclaimStaleTasks {

        @Test
        fun `stale PROCESSING task with retries remaining - reclaimed to PENDING`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = now())
            insertWorkflowDirect(wf)

            // PROCESSING task claimed 15 min ago (> 10 min threshold), not expired, retries remaining
            insertTaskDirect(
                makeTask(
                    id = taskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "stale-worker",
                    claimedAt = now().minus(Duration.ofMinutes(15)),
                    retryCount = 0,
                    maxRetries = 3,
                    deadlineAt = now().plus(Duration.ofMinutes(30)),
                ),
            )

            watchdog.patrol()

            // Task should be reset to PENDING with incremented retry count
            val task = readTaskDirect(taskId)
            assertNotNull(task)
            assertEquals("PENDING", task["STATUS"])
            assertEquals(1, (task["RETRY_COUNT"] as Number).toInt())
            // claimed_by and claimed_at should be null after reset
            assertEquals(null, task["CLAIMED_BY"])
            assertEquals(null, task["CLAIMED_AT"])
        }

        @Test
        fun `stale PROCESSING task with retries exhausted - marked FAILED`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val pastGrace = now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)

            // PROCESSING task claimed 15 min ago, retries exhausted (3/3)
            insertTaskDirect(
                makeTask(
                    id = taskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "stale-worker",
                    claimedAt = now().minus(Duration.ofMinutes(15)),
                    retryCount = 3,
                    maxRetries = 3,
                    deadlineAt = now().plus(Duration.ofMinutes(30)),
                ),
            )

            watchdog.patrol()

            // Task should be DEAD_LETTER (retries exhausted)
            val task = readTaskDirect(taskId)
            assertNotNull(task)
            assertEquals("DEAD_LETTER", task["STATUS"])

            // Only task at seq 1 with ABORT policy -> workflow FAILED
            val wfRow = readWorkflowDirect(wfId)
            assertNotNull(wfRow)
            assertEquals("FAILED", wfRow["STATUS"])
        }

        @Test
        fun `non-stale PROCESSING task - not touched`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = now())
            insertWorkflowDirect(wf)

            // PROCESSING task claimed 5 min ago (< 10 min threshold), not expired
            insertTaskDirect(
                makeTask(
                    id = taskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "active-worker",
                    claimedAt = now().minus(Duration.ofMinutes(5)),
                    retryCount = 0,
                    maxRetries = 3,
                    deadlineAt = now().plus(Duration.ofMinutes(30)),
                ),
            )

            watchdog.patrol()

            // Task still PROCESSING, unchanged
            val task = readTaskDirect(taskId)
            assertNotNull(task)
            assertEquals("PROCESSING", task["STATUS"])
            assertEquals("active-worker", task["CLAIMED_BY"])
            assertEquals(0, (task["RETRY_COUNT"] as Number).toInt())
        }

        @Test
        fun `expired AND stale task - failExpiredTasks handles it first, reclaimStaleTasks skips`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val pastGrace = now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)

            // Task is both expired (deadline in the past) AND stale (claimed 15 min ago)
            // with retries remaining — failExpiredTasks should handle it first
            insertTaskDirect(
                makeTask(
                    id = taskId,
                    workflowId = wfId,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "dead-worker",
                    claimedAt = now().minus(Duration.ofMinutes(15)),
                    retryCount = 0,
                    maxRetries = 3,
                    deadlineAt = now().minus(Duration.ofMinutes(5)),
                ),
            )

            watchdog.patrol()

            // Task should be TIMED_OUT (not PENDING) — expireOverdueTasks ran first
            val task = readTaskDirect(taskId)
            assertNotNull(task)
            assertEquals("TIMED_OUT", task["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 7: Patrol ordering — all three phases execute in sequence
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class PatrolOrdering {

        @Test
        fun `full patrol sequence - expired fails, stale reclaims, stuck workflows recover`() = runTest {
            // ── Workflow A: expired task -> should be FAILED ──
            val defA = twoStepLinearDef()
            val wfIdA = randomId()
            val expiredTaskId = randomId()
            val pastGraceA = now().minus(gracePeriod).minusSeconds(60)
            insertWorkflowDirect(makeWorkflow(id = wfIdA, definition = defA, updatedAt = pastGraceA))
            insertTaskDirect(
                makeTask(
                    id = expiredTaskId,
                    workflowId = wfIdA,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "dead-worker",
                    claimedAt = now().minus(Duration.ofMinutes(45)),
                    deadlineAt = now().minus(Duration.ofMinutes(30)),
                ),
            )

            // ── Workflow B: stale task with retries -> should be reclaimed to PENDING ──
            val defB = twoStepLinearDef()
            val wfIdB = randomId()
            val staleTaskId = randomId()
            insertWorkflowDirect(makeWorkflow(id = wfIdB, definition = defB, updatedAt = now()))
            insertTaskDirect(
                makeTask(
                    id = staleTaskId,
                    workflowId = wfIdB,
                    sequenceNumber = 1,
                    status = TaskStatus.PROCESSING,
                    handlerKey = "step1.handler",
                    claimedBy = "stale-worker",
                    claimedAt = now().minus(Duration.ofMinutes(15)),
                    retryCount = 0,
                    maxRetries = 3,
                    deadlineAt = now().plus(Duration.ofMinutes(30)),
                ),
            )

            // ── Workflow C: all tasks COMPLETED at seq 1, past grace -> should advance to seq 2 ──
            val defC = twoStepLinearDef()
            val wfIdC = randomId()
            val pastGraceC = now().minus(gracePeriod).minusSeconds(60)
            insertWorkflowDirect(makeWorkflow(id = wfIdC, definition = defC, updatedAt = pastGraceC))
            insertTaskDirect(
                makeTask(
                    workflowId = wfIdC,
                    sequenceNumber = 1,
                    status = TaskStatus.COMPLETED,
                    handlerKey = "step1.handler",
                    resultJson = """{"out":"value"}""",
                ),
            )

            // Single patrol() call triggers all three phases
            watchdog.patrol()

            // Workflow A: expired task -> TIMED_OUT, workflow FAILED (ABORT, single task)
            val taskA = readTaskDirect(expiredTaskId)
            assertNotNull(taskA)
            assertEquals("TIMED_OUT", taskA["STATUS"])
            val wfRowA = readWorkflowDirect(wfIdA)
            assertNotNull(wfRowA)
            assertEquals("FAILED", wfRowA["STATUS"])

            // Workflow B: stale task -> PENDING, retry_count incremented
            val taskB = readTaskDirect(staleTaskId)
            assertNotNull(taskB)
            assertEquals("PENDING", taskB["STATUS"])
            assertEquals(1, (taskB["RETRY_COUNT"] as Number).toInt())

            // Workflow C: stuck -> advanced to seq 2
            val wfRowC = readWorkflowDirect(wfIdC)
            assertNotNull(wfRowC)
            assertEquals(1, (wfRowC["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(wfIdC, 2))
            assertEquals(1, countTasksWithStatusDirect(wfIdC, 2, TaskStatus.PENDING))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Backoff, Barrier Guard, and Replay Tests
    // ═══════════════════════════════════════════════════════════════════════

    /** Read a nullable Oracle timestamp from raw row data for assertions. */
    private fun readNullableTimestampDirect(value: Any?): Instant? = when (value) {
        null -> null
        is java.sql.Timestamp -> value.toLocalDateTime().toInstant(ZoneOffset.UTC)
        else -> {
            val method = value::class.java.getMethod("timestampValue")
            (method.invoke(value) as java.sql.Timestamp).toLocalDateTime().toInstant(ZoneOffset.UTC)
        }
    }

    @Nested
    inner class SweepBackoffAndReplay {

        @Test
        fun `reclaimStaleTasks sets not_before with backoff`() = runTest {
            val wf = makeWorkflow(
                definition = workflow {
                    activity("step1") {
                        transition("test.handler"); retries(5)
                        deadline(Duration.ofHours(1)); failurePolicy(FailurePolicy.ABORT)
                    }
                },
            )
            insertWorkflowDirect(wf)

            val staleTime = Instant.now().minus(Duration.ofMinutes(15))
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "dead-worker",
                claimedAt = staleTime,
                retryCount = 2,
                maxRetries = 5,
            )
            insertTaskDirect(task)

            val threshold = Instant.now().minus(Duration.ofMinutes(10))
            val beforeReclaim = Instant.now()
            val reclaimed = taskRepo.resetStaleTasks(threshold)

            assertEquals(1, reclaimed)
            val row = readTaskDirect(task.id)!!
            assertEquals("PENDING", row["STATUS"])
            // retry_count incremented from 2 to 3, so backoff = 1*2^3 = 8s
            val notBefore = readNullableTimestampDirect(row["NOT_BEFORE"])
            assertNotNull(notBefore, "not_before should be set after stale reclaim")
            // retry_count incremented from 2 to 3, so backoff = 1*2^3 = 8s
            assertTrue(
                notBefore.isAfter(beforeReclaim.plusSeconds(6)),
                "not_before ($notBefore) should be at least ~8s after reclaim ($beforeReclaim)",
            )
            assertTrue(
                notBefore.isBefore(beforeReclaim.plusSeconds(10)),
                "not_before ($notBefore) should be at most ~8s+slack after reclaim ($beforeReclaim)",
            )
        }

        @Test
        fun `onTaskCompleted does not advance FAILED workflow`() = runTest {
            val definition = workflow {
                activity("step1") {
                    transition("test.handler"); retries(0)
                    deadline(Duration.ofHours(1)); failurePolicy(FailurePolicy.ABORT)
                }
            }
            val wf = makeWorkflow(
                definition = definition,
                
                version = 0,
                status = WorkflowStatus.FAILED,
            )
            insertWorkflowDirect(wf)
            val task = makeTask(
                workflowId = wf.id,
                sequenceNumber = 1,
                status = TaskStatus.PROCESSING,
                claimedBy = "worker-1",
                claimedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            )
            insertTaskDirect(task)

            barrier.onTaskCompleted(
                taskId = task.id,
                workflowId = wf.id,
                sequenceNumber = 1,
                status = TaskStatus.COMPLETED,
                resultJson = """{"ok":true}""",
                claimedBy = task.claimedBy,
                claimedAt = task.claimedAt,
            )

            // Task should be updated to COMPLETED
            val taskRow = readTaskDirect(task.id)!!
            assertEquals("COMPLETED", taskRow["STATUS"])

            // BUT workflow should still be FAILED — not advanced
            val wfRow = readWorkflowDirect(wf.id)!!
            assertEquals("FAILED", wfRow["STATUS"])
            assertEquals(0, (wfRow["VERSION"] as Number).toInt())
        }

        @Test
        fun `replayWorkflow resets FAILED workflow and replays dead-lettered tasks`() = runTest {
            val wf = makeWorkflow(
                definition = workflow {
                    activity("step1") {
                        transition("test.handler"); retries(3)
                        deadline(Duration.ofHours(1)); failurePolicy(FailurePolicy.ABORT)
                    }
                },
                status = WorkflowStatus.FAILED,
            )
            insertWorkflowDirect(wf)

            val dl1 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
            val dl2 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
            val completed = makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED)
            insertTaskDirect(dl1)
            insertTaskDirect(dl2)
            insertTaskDirect(completed)

            val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
            val result = engine.replayWorkflow(wf.id)

            assertTrue(result)
            val wfRow = readWorkflowDirect(wf.id)!!
            assertEquals("RUNNING", wfRow["STATUS"])
            assertEquals("PENDING", readTaskDirect(dl1.id)!!["STATUS"])
            assertEquals("PENDING", readTaskDirect(dl2.id)!!["STATUS"])
            assertEquals("COMPLETED", readTaskDirect(completed.id)!!["STATUS"])
        }

        @Test
        fun `replayWorkflow returns false for RUNNING workflow`() = runTest {
            val wf = makeWorkflow(
                definition = workflow {
                    activity("step1") {
                        transition("test.handler"); retries(0)
                        deadline(Duration.ofHours(1)); failurePolicy(FailurePolicy.ABORT)
                    }
                },
                status = WorkflowStatus.RUNNING,
            )
            insertWorkflowDirect(wf)

            val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
            val result = engine.replayWorkflow(wf.id)

            assertFalse(result)
        }

        @Test
        fun `replayWorkflow returns false for COMPLETED workflow`() = runTest {
            val wf = makeWorkflow(
                definition = workflow {
                    activity("step1") {
                        transition("test.handler"); retries(0)
                        deadline(Duration.ofHours(1)); failurePolicy(FailurePolicy.ABORT)
                    }
                },
                status = WorkflowStatus.COMPLETED,
            )
            insertWorkflowDirect(wf)

            val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
            val result = engine.replayWorkflow(wf.id)

            assertFalse(result)
        }

        @Test
        fun `replayWorkflow returns false for non-existent workflow`() = runTest {
            val engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
            val result = engine.replayWorkflow(randomId())

            assertFalse(result)
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 8: expireOverdueWorkflows — workflows past deadline
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class ExpireOverdueWorkflows {

        @Test
        fun `timed-out workflow transitions to TIMED_OUT and cancels pending tasks`() = runTest {
            val definition = workflow {
                activity("step1") { transition("handler1"); next("step2") }
                activity("step2") { transition("handler2") }
            }
            val wfId = randomId()
            val pastDeadline = now().minus(Duration.ofMinutes(5))
            val wf = makeWorkflow(
                id = wfId,
                definition = definition,
                updatedAt = now().minus(Duration.ofHours(1)),
                deadlineAt = pastDeadline,
            )
            workflowRepo.insert(wf)

            val task = Task(
                id = randomId(), workflowId = wfId, activityName = "step1", sequenceNumber = 1,
                status = TaskStatus.PENDING, handlerKey = "handler1",
                item = null, resultJson = null,
                claimedBy = null, claimedAt = null, completedAt = null,
                retryCount = 0, maxRetries = 3, deadlineAt = null,
            )
            taskRepo.insertBatch(listOf(task))

            watchdog.patrol()

            val updatedWf = workflowRepo.findById(wfId)
            assertNotNull(updatedWf)
            assertEquals(WorkflowStatus.TIMED_OUT, updatedWf.status)

            val updatedTasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
            assertTrue(updatedTasks.all { it.status == TaskStatus.CANCELLED })
        }

        @Test
        fun `workflow within deadline is not expired`() = runTest {
            val definition = workflow {
                activity("step1") { transition("handler1") }
            }
            val wfId = randomId()
            val futureDeadline = now().plus(Duration.ofHours(1))
            val wf = makeWorkflow(
                id = wfId,
                definition = definition,
                deadlineAt = futureDeadline,
            )
            workflowRepo.insert(wf)

            watchdog.patrol()

            val updatedWf = workflowRepo.findById(wfId)
            assertNotNull(updatedWf)
            assertEquals(WorkflowStatus.RUNNING, updatedWf.status)
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 9: Diamond DAG recovery — iterate-all-sequences dispatches gap
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class DiamondDagRecovery {

        @Test
        fun `diamond DAG crash after B dispatched but C missed - recovery dispatches C`() = runTest {
            val def = diamondDagDef()
            val seqMap = buildSequenceMap(def)
            val seqA = seqMap.entries.first { it.value.activityName == "A" }.key
            val seqB = seqMap.entries.first { it.value.activityName == "B" }.key
            val seqC = seqMap.entries.first { it.value.activityName == "C" }.key
            val seqD = seqMap.entries.first { it.value.activityName == "D" }.key

            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)

            // A completed at seqA
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = seqA,
                    status = TaskStatus.COMPLETED, handlerKey = "a.handler",
                ),
            )
            // B completed at seqB (was dispatched before crash)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = seqB,
                    status = TaskStatus.COMPLETED, handlerKey = "b.handler",
                ),
            )
            // C at seqC: NO task (the gap — crash happened before C was dispatched)
            // D at seqD: NO task

            watchdog.patrol()

            // C should now have a PENDING task at seqC
            assertEquals(1, countTasksDirect(wfId, seqC))
            assertEquals(1, countTasksWithStatusDirect(wfId, seqC, TaskStatus.PENDING))

            // D should NOT have a task yet (C is non-terminal, predecessor gate blocks)
            assertEquals(0, countTasksDirect(wfId, seqD))

            // Workflow still RUNNING with CAS bumped
            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("RUNNING", row["STATUS"])
            assertEquals(1, (row["VERSION"] as Number).toInt())
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 10: findStuck EXISTS guard — zero-task workflow not returned
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class FindStuckExistsGuard {

        @Test
        fun `RUNNING workflow past grace with zero tasks - not returned by findStuck`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(id = wfId, definition = def, updatedAt = pastGrace)
            insertWorkflowDirect(wf)
            // No tasks inserted — brand new workflow that hasn't started dispatching

            watchdog.patrol()

            // Workflow should be untouched: still RUNNING, version 0
            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("RUNNING", row["STATUS"])
            assertEquals(0, (row["VERSION"] as Number).toInt())

            // No tasks created at any sequence
            assertEquals(0, countTasksDirect(wfId, 1))
            assertEquals(0, countTasksDirect(wfId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 11: recoverStuckWorkflow direct-call edge cases
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class RecoverStuckWorkflowDirectCalls {

        @Test
        fun `recoverStuckWorkflow with non-existent workflow ID - no-op no error`() = runTest {
            barrier.recoverStuckWorkflow(randomId())
            // No exception thrown, no side effects
        }

        @Test
        fun `recoverStuckWorkflow on COMPLETED workflow - no-op`() = runTest {
            val def = singleStepDef()
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, status = WorkflowStatus.COMPLETED)
            insertWorkflowDirect(wf)
            insertTaskDirect(makeTask(workflowId = wfId, sequenceNumber = 1, status = TaskStatus.COMPLETED, handlerKey = "only.handler"))

            barrier.recoverStuckWorkflow(wfId)

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("COMPLETED", row["STATUS"])
            assertEquals(0, (row["VERSION"] as Number).toInt())
        }

        @Test
        fun `recoverStuckWorkflow on FAILED workflow - no-op`() = runTest {
            val def = singleStepDef()
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, status = WorkflowStatus.FAILED)
            insertWorkflowDirect(wf)
            insertTaskDirect(makeTask(workflowId = wfId, sequenceNumber = 1, status = TaskStatus.FAILED, handlerKey = "only.handler"))

            barrier.recoverStuckWorkflow(wfId)

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("FAILED", row["STATUS"])
            assertEquals(0, (row["VERSION"] as Number).toInt())
        }
    }
}
