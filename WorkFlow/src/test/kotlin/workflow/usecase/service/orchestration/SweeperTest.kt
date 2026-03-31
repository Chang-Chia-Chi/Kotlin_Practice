package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.config.SweeperConfig
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.service.orchestration.BarrierService
import com.workflow.workflow.usecase.service.orchestration.Sweeper
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import com.workflow.workflow.usecase.service.phase.PhaseStrategyRegistry
import com.workflow.worker.adapter.http.FakeDispatchNotifier
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
class SweeperTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var barrier: BarrierService
    private lateinit var sweeper: Sweeper

    private val gracePeriod = Duration.ofMinutes(2)
    private val staleTaskThreshold = Duration.ofMinutes(10)

    private val notifier = FakeDispatchNotifier()

    private val testSweeperConfig = object : SweeperConfig {
        override fun interval(): Duration = Duration.ofSeconds(30)
        override fun gracePeriod(): Duration = gracePeriod
        override fun staleTaskThreshold(): Duration = staleTaskThreshold
    }

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        barrier = BarrierService(jdbi, workflowRepo, taskRepo, objectMapper, PhaseStrategyRegistry(), notifier)
        sweeper = Sweeper(jdbi, workflowRepo, taskRepo, barrier, testSweeperConfig)
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
        currentSequence: Int = 1,
        version: Int = 0,
        status: WorkflowStatus = WorkflowStatus.RUNNING,
        createdAt: Instant = now(),
        updatedAt: Instant = now(),
        deadlineAt: Instant = now().plus(Duration.ofHours(1)),
    ): WorkflowRun = WorkflowRun(
        id = id,
        definitionJson = objectMapper.writeValueAsString(definition),
        currentSequence = currentSequence,
        version = version,
        status = status,
        createdAt = createdAt,
        updatedAt = updatedAt,
        deadlineAt = deadlineAt,
    )

    private fun makeTask(
        id: String = randomId(),
        workflowId: String,
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
                """INSERT INTO workflow (id, definition, current_sequence, version, status, created_at, updated_at, deadline_at)
                   VALUES (:id, :definition, :currentSequence, :version, :status, :createdAt, :updatedAt, :deadlineAt)""",
            )
                .bind("id", run.id)
                .bind("definition", run.definitionJson)
                .bind("currentSequence", run.currentSequence)
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

    /** Update workflow directly via SQL (for simulating concurrent advance). */
    private fun advanceWorkflowDirect(id: String, newSequence: Int, newVersion: Int) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """UPDATE workflow SET current_sequence = :seq, version = :ver, updated_at = :now
                   WHERE id = :id""",
            )
                .bind("id", id)
                .bind("seq", newSequence)
                .bind("ver", newVersion)
                .bind("now", LocalDateTime.now(ZoneOffset.UTC))
                .execute()
        }
    }

    // ── Workflow Definition Builders ─────────────────────────────────────

    /** Two linear activities: seq 1 -> seq 2. */
    private fun twoStepLinearDef() = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(name = "step1", transition = "step1.handler"),
            ActivityDefinition(name = "step2", transition = "step2.handler"),
        ),
    )

    /** Single activity: seq 1 only. */
    private fun singleStepDef() = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(name = "only", transition = "only.handler"),
        ),
    )

    /** Two linear with BEST_EFFORT on step1. */
    private fun twoStepBestEffortDef() = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(
                name = "step1", transition = "step1.handler",
                failurePolicy = FailurePolicy.BEST_EFFORT,
            ),
            ActivityDefinition(name = "step2", transition = "step2.handler"),
        ),
    )

    /** Fan-out then linear: seq 1 (LINEAR scatter) -> seq 2 (PARALLEL) -> seq 3 (LINEAR). */
    private fun fanOutThenLinearDef(joinPolicy: JoinPolicy = JoinPolicy.All) = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(
                name = "scatter-activity", transition = "scatter.handler",
                fanOut = "parallel-activity",
            ),
            ActivityDefinition(
                name = "parallel-activity", transition = "parallel.handler",
                joinPolicy = joinPolicy,
            ),
            ActivityDefinition(name = "final-step", transition = "final.handler"),
        ),
    )

    // ═══════════════════════════════════════════════════════════════════════
    // Test 1: Stuck workflow detected after grace period -> sweeper recovers
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
                currentSequence = 1,
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

            sweeper.patrol()

            // Workflow should have advanced to sequence 2, version incremented
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (updatedWf["VERSION"] as Number).toInt())
            assertEquals("RUNNING", updatedWf["STATUS"])

            // One new PENDING task created at sequence 2 with step2's handler key
            val seq2Tasks = readTasksDirect(wfId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals("step2.handler", seq2Tasks[0]["HANDLER_KEY"])
            assertEquals("PENDING", seq2Tasks[0]["STATUS"])
        }

        @Test
        fun `last sequence stuck - sweeper marks workflow COMPLETED`() = runTest {
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

            sweeper.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("COMPLETED", row["STATUS"])
        }

        @Test
        fun `all tasks FAILED with ABORT policy - sweeper marks workflow FAILED`() = runTest {
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

            sweeper.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("FAILED", row["STATUS"])
            assertEquals(0, countTasksDirect(wfId, 2))
        }

        @Test
        fun `all tasks FAILED with BEST_EFFORT - sweeper advances to next sequence`() = runTest {
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

            sweeper.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals(2, (row["CURRENT_SEQUENCE"] as Number).toInt())
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

            sweeper.patrol()

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

            sweeper.patrol()

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

            sweeper.patrol()

            val row = readWorkflowDirect(wfId)
            assertNotNull(row)
            assertEquals("FAILED", row["STATUS"])
            assertEquals(0, (row["VERSION"] as Number).toInt())
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 2: Within grace period -> sweeper skips
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
                currentSequence = 1,
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

            sweeper.patrol()

            // Workflow NOT advanced — still at sequence 1, same version
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(1, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(0, (updatedWf["VERSION"] as Number).toInt())

            // No tasks created at sequence 2
            assertEquals(0, countTasksDirect(wfId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 3: Sweeper CAS loses to worker -> no duplicate downstream tasks
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class SweeperCasLosesToWorker {

        @Test
        fun `worker advances workflow before sweeper patrol - CAS fails, no duplicate tasks`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(
                id = wfId,
                definition = def,
                currentSequence = 1,
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

            // Simulate worker advancing the workflow before sweeper runs:
            // advance to seq 2, version=1, updated_at=NOW
            advanceWorkflowDirect(wfId, newSequence = 2, newVersion = 1)

            // Insert a PENDING task at seq 2 (as the worker-driven advance would have done)
            insertTaskDirect(
                makeTask(
                    workflowId = wfId,
                    sequenceNumber = 2,
                    status = TaskStatus.PENDING,
                    handlerKey = "step2.handler",
                ),
            )

            sweeper.patrol()

            // Workflow still at sequence 2 (not double-advanced)
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            // Version unchanged from the worker advance (sweeper CAS failed)
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
            assertEquals(2, (row["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (row["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(wfId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 4: Sweeper fires twice on same stuck workflow -> second is no-op
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class SweeperIdempotency {

        @Test
        fun `patrol twice on same stuck workflow - first recovers, second is no-op`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val pastGrace = Instant.now().minus(gracePeriod).minusSeconds(60)
            val wf = makeWorkflow(
                id = wfId,
                definition = def,
                currentSequence = 1,
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
            sweeper.patrol()

            // Verify first recovery succeeded
            val afterFirst = readWorkflowDirect(wfId)
            assertNotNull(afterFirst)
            assertEquals(2, (afterFirst["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (afterFirst["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(wfId, 2))
            assertEquals(1, countTasksWithStatusDirect(wfId, 2, TaskStatus.PENDING))

            // Re-set updated_at to past grace period (make it eligible again by time)
            val pastGraceAgain = Instant.now().minus(gracePeriod).minusSeconds(60)
            updateWorkflowUpdatedAtDirect(wfId, pastGraceAgain)

            // Second patrol — findStuck should NOT return this workflow because
            // seq 2 has a PENDING (non-terminal) task, so NOT EXISTS condition fails
            sweeper.patrol()

            // Verify no change: still at seq 2, version 1, exactly 1 task at seq 2
            val afterSecond = readWorkflowDirect(wfId)
            assertNotNull(afterSecond)
            assertEquals(2, (afterSecond["CURRENT_SEQUENCE"] as Number).toInt())
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

            sweeper.patrol()

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

            sweeper.patrol()

            // Task still PROCESSING, workflow still at seq 1 version 0
            val task = readTaskDirect(taskId)
            assertNotNull(task)
            assertEquals("PROCESSING", task["STATUS"])

            val wfRow = readWorkflowDirect(wfId)
            assertNotNull(wfRow)
            assertEquals(1, (wfRow["CURRENT_SEQUENCE"] as Number).toInt())
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

            sweeper.patrol()

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
            assertEquals(1, (wfRow["CURRENT_SEQUENCE"] as Number).toInt())
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

            sweeper.patrol()

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

            sweeper.patrol()

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

            sweeper.patrol()

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

            sweeper.patrol()

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
            sweeper.patrol()

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
            assertEquals(2, (wfRowC["CURRENT_SEQUENCE"] as Number).toInt())
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
                definition = WorkflowDefinition(
                    activities = listOf(ActivityDefinition(
                        name = "step1", transition = "test.handler", retries = 5,
                        deadline = Duration.ofHours(1), failurePolicy = FailurePolicy.ABORT,
                    )),
                ),
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
            val definition = WorkflowDefinition(
                activities = listOf(
                    ActivityDefinition(
                        name = "step1",
                        transition = "test.handler",
                        retries = 0,
                        deadline = Duration.ofHours(1),
                        failurePolicy = FailurePolicy.ABORT,
                    ),
                ),
            )
            val wf = makeWorkflow(
                definition = definition,
                currentSequence = 1,
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
            assertEquals(1, (wfRow["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(0, (wfRow["VERSION"] as Number).toInt())
        }

        @Test
        fun `replayWorkflow resets FAILED workflow and replays dead-lettered tasks`() = runTest {
            val wf = makeWorkflow(
                definition = WorkflowDefinition(
                    activities = listOf(ActivityDefinition(
                        name = "step1", transition = "test.handler", retries = 3,
                        deadline = Duration.ofHours(1), failurePolicy = FailurePolicy.ABORT,
                    )),
                ),
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
                definition = WorkflowDefinition(
                    activities = listOf(ActivityDefinition(
                        name = "step1", transition = "test.handler", retries = 0,
                        deadline = Duration.ofHours(1), failurePolicy = FailurePolicy.ABORT,
                    )),
                ),
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
                definition = WorkflowDefinition(
                    activities = listOf(ActivityDefinition(
                        name = "step1", transition = "test.handler", retries = 0,
                        deadline = Duration.ofHours(1), failurePolicy = FailurePolicy.ABORT,
                    )),
                ),
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
            val definition = WorkflowDefinition(
                activities = listOf(
                    ActivityDefinition(name = "step1", transition = "handler1"),
                    ActivityDefinition(name = "step2", transition = "handler2"),
                ),
            )
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
                id = randomId(), workflowId = wfId, sequenceNumber = 1,
                status = TaskStatus.PENDING, handlerKey = "handler1",
                item = null, resultJson = null,
                claimedBy = null, claimedAt = null, completedAt = null,
                retryCount = 0, maxRetries = 3, deadlineAt = null,
            )
            taskRepo.insertBatch(listOf(task))

            sweeper.patrol()

            val updatedWf = workflowRepo.findById(wfId)
            assertNotNull(updatedWf)
            assertEquals(WorkflowStatus.TIMED_OUT, updatedWf.status)

            val updatedTasks = taskRepo.findByWorkflowAndSequence(wfId, 1)
            assertTrue(updatedTasks.all { it.status == TaskStatus.CANCELLED })
        }

        @Test
        fun `workflow within deadline is not expired`() = runTest {
            val definition = WorkflowDefinition(
                activities = listOf(
                    ActivityDefinition(name = "step1", transition = "handler1"),
                ),
            )
            val wfId = randomId()
            val futureDeadline = now().plus(Duration.ofHours(1))
            val wf = makeWorkflow(
                id = wfId,
                definition = definition,
                deadlineAt = futureDeadline,
            )
            workflowRepo.insert(wf)

            sweeper.patrol()

            val updatedWf = workflowRepo.findById(wfId)
            assertNotNull(updatedWf)
            assertEquals(WorkflowStatus.RUNNING, updatedWf.status)
        }
    }
}
