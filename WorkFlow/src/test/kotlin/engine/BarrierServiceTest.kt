package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FailurePolicy
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
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
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BarrierServiceTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: WorkflowRepository
    private lateinit var taskRepo: TaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var barrier: BarrierService

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = WorkflowRepository(jdbi)
        taskRepo = TaskRepository(jdbi)
        barrier = BarrierService(jdbi, workflowRepo, taskRepo, objectMapper)
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
    ): WorkflowRun = WorkflowRun(
        id = id,
        definitionJson = objectMapper.writeValueAsString(definition),
        currentSequence = currentSequence,
        version = version,
        status = status,
        createdAt = createdAt,
        updatedAt = updatedAt,
    )

    private fun makeTask(
        id: String = randomId(),
        workflowId: String,
        sequenceNumber: Int = 1,
        status: TaskStatus = TaskStatus.PENDING,
        handlerKey: String = "test.handler",
        payloadJson: String? = null,
        resultJson: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        completedAt: Instant? = null,
        retryCount: Int = 0,
        maxRetries: Int = 0,
        deadlineAt: Instant? = null,
    ): Task = Task(
        id = id,
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = status,
        handlerKey = handlerKey,
        payloadJson = payloadJson,
        resultJson = resultJson,
        claimedBy = claimedBy,
        claimedAt = claimedAt,
        completedAt = completedAt,
        retryCount = retryCount,
        maxRetries = maxRetries,
        deadlineAt = deadlineAt,
    )

    /** Insert a workflow directly via SQL (independent of repo under test). */
    private fun insertWorkflowDirect(run: WorkflowRun) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO workflow (id, definition, current_sequence, version, status, created_at, updated_at)
                   VALUES (:id, :definition, :currentSequence, :version, :status, :createdAt, :updatedAt)""",
            )
                .bind("id", run.id)
                .bind("definition", run.definitionJson)
                .bind("currentSequence", run.currentSequence)
                .bind("version", run.version)
                .bind("status", run.status.name)
                .bind("createdAt", LocalDateTime.ofInstant(run.createdAt, ZoneOffset.UTC))
                .bind("updatedAt", LocalDateTime.ofInstant(run.updatedAt, ZoneOffset.UTC))
                .execute()
        }
    }

    /** Insert a task directly via SQL (independent of repo under test). */
    private fun insertTaskDirect(task: Task) {
        jdbi.useHandle<Exception> { handle ->
            val stmt = handle.createUpdate(
                """INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, payload, result,
                   claimed_by, claimed_at, completed_at, retry_count, max_retries, deadline_at)
                   VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey, :payload, :result,
                   :claimedBy, :claimedAt, :completedAt, :retryCount, :maxRetries, :deadlineAt)""",
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

            bindStringOrNull("payload", task.payloadJson)
            bindStringOrNull("result", task.resultJson)
            bindStringOrNull("claimedBy", task.claimedBy)
            bindTimestampOrNull("claimedAt", task.claimedAt)
            bindTimestampOrNull("completedAt", task.completedAt)
            bindTimestampOrNull("deadlineAt", task.deadlineAt)

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

    // ── Workflow Definition Builders ─────────────────────────────────────

    /** Two linear activities: seq 1 → seq 2. */
    private fun twoStepLinearDef() = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(name = "step1", transition = "step1.handler"),
            ActivityDefinition(name = "step2", transition = "step2.handler"),
        ),
    )

    /**
     * Single fan-out activity: seq 1 (SCATTER) → seq 2 (PARALLEL).
     *
     * Handler key mapping per locked contract:
     * - SCATTER phase uses `activity.transition` → "parallel.handler"
     * - PARALLEL phase uses `fanOut.transition` → "scatter.handler"
     */
    private fun fanOutDef(
        joinPolicy: JoinPolicy = JoinPolicy.All,
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
        fanOutFailurePolicy: FailurePolicy = FailurePolicy.ABORT,
    ) = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(
                name = "scatter-activity",
                transition = "parallel.handler",
                failurePolicy = failurePolicy,
                fanOut = FanOutDefinition(
                    transition = "scatter.handler",
                    failurePolicy = fanOutFailurePolicy,
                    joinPolicy = joinPolicy,
                ),
            ),
        ),
    )

    /**
     * Fan-out followed by a linear step: seq 1 (SCATTER) → seq 2 (PARALLEL) → seq 3 (LINEAR).
     * Used to verify that after parallel phase completes, the next linear task is inserted.
     *
     * Handler key mapping per locked contract:
     * - SCATTER phase uses `activity.transition` → "parallel.handler"
     * - PARALLEL phase uses `fanOut.transition` → "scatter.handler"
     */
    private fun fanOutThenLinearDef(
        joinPolicy: JoinPolicy = JoinPolicy.All,
        fanOutFailurePolicy: FailurePolicy = FailurePolicy.ABORT,
    ) = WorkflowDefinition(
        activities = listOf(
            ActivityDefinition(
                name = "scatter-activity",
                transition = "parallel.handler",
                fanOut = FanOutDefinition(
                    transition = "scatter.handler",
                    failurePolicy = fanOutFailurePolicy,
                    joinPolicy = joinPolicy,
                ),
            ),
            ActivityDefinition(name = "final-step", transition = "final.handler"),
        ),
    )

    // ═══════════════════════════════════════════════════════════════════════
    // Test 1: Single task completes (linear)
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class SingleLinearTaskCompletes {

        @Test
        fun `linear task completes - probe is 0, CAS wins, next sequence tasks inserted`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    id = taskId, workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.PROCESSING, handlerKey = "step1.handler",
                ),
            )

            barrier.onTaskCompleted(taskId, wfId, 1, TaskStatus.COMPLETED, null)

            // Workflow advanced to sequence 2, version incremented
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (updatedWf["VERSION"] as Number).toInt())
            assertEquals("RUNNING", updatedWf["STATUS"])

            // One task created at sequence 2 with step2's handler key
            val seq2Tasks = readTasksDirect(wfId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals("step2.handler", seq2Tasks[0]["HANDLER_KEY"])
            assertEquals("PENDING", seq2Tasks[0]["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 2: Last-of-many completes (parallel phase)
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class LastOfManyParallelCompletes {

        @Test
        fun `last parallel task completes - probe is 0, CAS wins, exactly one phase transition`() = runTest {
            val def = fanOutThenLinearDef()
            val wfId = randomId()
            // Workflow already at PARALLEL phase (sequence 2), scatter done
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 2, version = 1)
            insertWorkflowDirect(wf)

            // 3 parallel sub-tasks: 2 already COMPLETED, 1 still PROCESSING (the last one)
            val lastTaskId = randomId()
            insertTaskDirect(
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED, handlerKey = "parallel.handler"),
            )
            insertTaskDirect(
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED, handlerKey = "parallel.handler"),
            )
            insertTaskDirect(
                makeTask(
                    id = lastTaskId, workflowId = wfId, sequenceNumber = 2,
                    status = TaskStatus.PROCESSING, handlerKey = "parallel.handler",
                ),
            )

            barrier.onTaskCompleted(lastTaskId, wfId, 2, TaskStatus.COMPLETED, null)

            // Workflow advanced to sequence 3
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(3, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(2, (updatedWf["VERSION"] as Number).toInt())

            // One task at sequence 3 (the linear final-step)
            assertEquals(1, countTasksDirect(wfId, 3))
            val seq3Tasks = readTasksDirect(wfId, 3)
            assertEquals("final.handler", seq3Tasks[0]["HANDLER_KEY"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 3: Not-last task (probe > 0, no CAS)
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class NotLastTask {

        @Test
        fun `not-last task completes - probe greater than 0, task updated, no phase transition`() = runTest {
            val def = fanOutThenLinearDef()
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 2, version = 1)
            insertWorkflowDirect(wf)

            val completingTaskId = randomId()
            // 3 parallel sub-tasks: 1 COMPLETED, 1 PROCESSING (completing now), 1 still PENDING
            insertTaskDirect(
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED, handlerKey = "parallel.handler"),
            )
            insertTaskDirect(
                makeTask(
                    id = completingTaskId, workflowId = wfId, sequenceNumber = 2,
                    status = TaskStatus.PROCESSING, handlerKey = "parallel.handler",
                ),
            )
            insertTaskDirect(
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.PENDING, handlerKey = "parallel.handler"),
            )

            barrier.onTaskCompleted(completingTaskId, wfId, 2, TaskStatus.COMPLETED, null)

            // Workflow NOT advanced — still at sequence 2, same version
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (updatedWf["VERSION"] as Number).toInt())

            // No tasks created at sequence 3
            assertEquals(0, countTasksDirect(wfId, 3))

            // The completing task was updated to COMPLETED
            assertEquals(2, countTasksWithStatusDirect(wfId, 2, TaskStatus.COMPLETED))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 4: CAS race — two concurrent recovery attempts
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class CasRace {

        @Test
        fun `two concurrent recovery attempts on stuck workflow - exactly one CAS wins, one set of downstream tasks`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)

            // Both tasks pre-committed as COMPLETED (simulates stuck workflow
            // where all tasks are terminal but workflow wasn't advanced).
            // recoverStuckWorkflow skips self-update, so both calls see
            // nonTerminal=0 and both attempt CAS — exactly one wins.
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.COMPLETED, handlerKey = "step1.handler",
                ),
            )
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.COMPLETED, handlerKey = "step1.handler",
                ),
            )

            // Two concurrent recovery attempts — both see nonTerminal=0, both attempt CAS
            val d1 = async { barrier.recoverStuckWorkflow(wfId) }
            val d2 = async { barrier.recoverStuckWorkflow(wfId) }
            awaitAll(d1, d2)

            // Workflow advanced exactly once — version must be 1
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (updatedWf["VERSION"] as Number).toInt())

            // Exactly one set of downstream tasks at sequence 2
            assertEquals(1, countTasksDirect(wfId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 5: JoinPolicy ALL with 1 failure → failure
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class JoinPolicyAllWithFailure {

        @Test
        fun `JoinPolicy ALL with 1 failed task - workflow marked FAILED`() = runTest {
            val def = fanOutDef(joinPolicy = JoinPolicy.All)
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 2, version = 1)
            insertWorkflowDirect(wf)

            // 3 parallel tasks: 1 FAILED, 1 COMPLETED, 1 PROCESSING (completing now as COMPLETED)
            val lastTaskId = randomId()
            insertTaskDirect(
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.FAILED, handlerKey = "parallel.handler"),
            )
            insertTaskDirect(
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED, handlerKey = "parallel.handler"),
            )
            insertTaskDirect(
                makeTask(
                    id = lastTaskId, workflowId = wfId, sequenceNumber = 2,
                    status = TaskStatus.PROCESSING, handlerKey = "parallel.handler",
                ),
            )

            barrier.onTaskCompleted(lastTaskId, wfId, 2, TaskStatus.COMPLETED, null)

            // JoinPolicy.All requires zero failures — workflow should be FAILED
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals("FAILED", updatedWf["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 6: JoinPolicy PERCENTAGE(95) with 3/100 failed → success
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class JoinPolicyPercentage95Success {

        @Test
        fun `PERCENTAGE 95 with 3 of 100 failed - outcome is success, workflow advances`() = runTest {
            val def = fanOutThenLinearDef(joinPolicy = JoinPolicy.Percentage(95))
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 2, version = 1)
            insertWorkflowDirect(wf)

            // 100 tasks: 96 COMPLETED, 3 FAILED, 1 PROCESSING (completing now)
            val lastTaskId = randomId()
            repeat(96) {
                insertTaskDirect(
                    makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED, handlerKey = "parallel.handler"),
                )
            }
            repeat(3) {
                insertTaskDirect(
                    makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.FAILED, handlerKey = "parallel.handler"),
                )
            }
            insertTaskDirect(
                makeTask(
                    id = lastTaskId, workflowId = wfId, sequenceNumber = 2,
                    status = TaskStatus.PROCESSING, handlerKey = "parallel.handler",
                ),
            )

            barrier.onTaskCompleted(lastTaskId, wfId, 2, TaskStatus.COMPLETED, null)

            // 97/100 succeeded = 97% >= 95% → success → workflow advances to seq 3
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(3, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals("RUNNING", updatedWf["STATUS"])
            assertEquals(1, countTasksDirect(wfId, 3))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 7: JoinPolicy PERCENTAGE(95) with 10/100 failed → failure
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class JoinPolicyPercentage95Failure {

        @Test
        fun `PERCENTAGE 95 with 10 of 100 failed - outcome is failure, workflow FAILED`() = runTest {
            val def = fanOutDef(joinPolicy = JoinPolicy.Percentage(95))
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 2, version = 1)
            insertWorkflowDirect(wf)

            // 100 tasks: 89 COMPLETED, 10 FAILED, 1 PROCESSING (completing now)
            val lastTaskId = randomId()
            repeat(89) {
                insertTaskDirect(
                    makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED, handlerKey = "parallel.handler"),
                )
            }
            repeat(10) {
                insertTaskDirect(
                    makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.FAILED, handlerKey = "parallel.handler"),
                )
            }
            insertTaskDirect(
                makeTask(
                    id = lastTaskId, workflowId = wfId, sequenceNumber = 2,
                    status = TaskStatus.PROCESSING, handlerKey = "parallel.handler",
                ),
            )

            barrier.onTaskCompleted(lastTaskId, wfId, 2, TaskStatus.COMPLETED, null)

            // 90/100 succeeded = 90% < 95% → failure → workflow FAILED
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals("FAILED", updatedWf["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 8: JoinPolicy THRESHOLD(40) with 45/50 succeeded → success
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class JoinPolicyThreshold40Success {

        @Test
        fun `THRESHOLD 40 with 45 of 50 succeeded - outcome is success, workflow advances`() = runTest {
            val def = fanOutThenLinearDef(joinPolicy = JoinPolicy.Threshold(40))
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 2, version = 1)
            insertWorkflowDirect(wf)

            // 50 tasks: 44 COMPLETED, 5 FAILED, 1 PROCESSING (completing now)
            val lastTaskId = randomId()
            repeat(44) {
                insertTaskDirect(
                    makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED, handlerKey = "parallel.handler"),
                )
            }
            repeat(5) {
                insertTaskDirect(
                    makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.FAILED, handlerKey = "parallel.handler"),
                )
            }
            insertTaskDirect(
                makeTask(
                    id = lastTaskId, workflowId = wfId, sequenceNumber = 2,
                    status = TaskStatus.PROCESSING, handlerKey = "parallel.handler",
                ),
            )

            barrier.onTaskCompleted(lastTaskId, wfId, 2, TaskStatus.COMPLETED, null)

            // 45 succeeded >= threshold(40) → success → workflow advances to seq 3
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(3, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals("RUNNING", updatedWf["STATUS"])
            assertEquals(1, countTasksDirect(wfId, 3))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 9: FailurePolicy BEST_EFFORT on failed phase → workflow advances
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class FailurePolicyBestEffort {

        @Test
        fun `BEST_EFFORT on linear task failure - workflow advances to next sequence`() = runTest {
            val def = WorkflowDefinition(
                activities = listOf(
                    ActivityDefinition(
                        name = "step1", transition = "step1.handler",
                        failurePolicy = FailurePolicy.BEST_EFFORT,
                    ),
                    ActivityDefinition(name = "step2", transition = "step2.handler"),
                ),
            )
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    id = taskId, workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.PROCESSING, handlerKey = "step1.handler",
                ),
            )

            // Task fails, but FailurePolicy is BEST_EFFORT
            barrier.onTaskCompleted(taskId, wfId, 1, TaskStatus.FAILED, null)

            // Workflow should advance to sequence 2 despite failure
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals("RUNNING", updatedWf["STATUS"])
            assertEquals(1, countTasksDirect(wfId, 2))
        }

        @Test
        fun `BEST_EFFORT with non-null resultJson - failed task result propagates as next task payload`() = runTest {
            val def = WorkflowDefinition(
                activities = listOf(
                    ActivityDefinition(
                        name = "step1", transition = "step1.handler",
                        failurePolicy = FailurePolicy.BEST_EFFORT,
                    ),
                    ActivityDefinition(name = "step2", transition = "step2.handler"),
                ),
            )
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    id = taskId, workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.PROCESSING, handlerKey = "step1.handler",
                ),
            )

            val errorResult = """{"error":"timeout","partial":{"count":5}}"""
            barrier.onTaskCompleted(taskId, wfId, 1, TaskStatus.FAILED, errorResult)

            // Workflow advances despite failure
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())

            // Failed task's resultJson propagates as next task's payload
            val seq2Tasks = readTasksDirect(wfId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals(errorResult, seq2Tasks[0]["PAYLOAD"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 10: PARALLEL→LINEAR payload propagation (next task payload is null)
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class ParallelToLinearPayload {

        @Test
        fun `parallel phase completes - next linear task has null payload`() = runTest {
            val def = fanOutThenLinearDef(joinPolicy = JoinPolicy.All)
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 2, version = 1)
            insertWorkflowDirect(wf)

            val lastTaskId = randomId()
            insertTaskDirect(
                makeTask(
                    workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED,
                    handlerKey = "parallel.handler", resultJson = """{"r":"one"}""",
                ),
            )
            insertTaskDirect(
                makeTask(
                    id = lastTaskId, workflowId = wfId, sequenceNumber = 2,
                    status = TaskStatus.PROCESSING, handlerKey = "parallel.handler",
                ),
            )

            barrier.onTaskCompleted(lastTaskId, wfId, 2, TaskStatus.COMPLETED, """{"r":"two"}""")

            // Workflow advances to seq 3
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(3, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals("RUNNING", updatedWf["STATUS"])

            // Next linear task has null payload (multiple parallel results, no single payload to propagate)
            val seq3Tasks = readTasksDirect(wfId, 3)
            assertEquals(1, seq3Tasks.size)
            assertEquals("final.handler", seq3Tasks[0]["HANDLER_KEY"])
            assertTrue(seq3Tasks[0]["PAYLOAD"] == null,
                "PARALLEL→LINEAR: next task payload should be null")
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 11: Payload propagation — LINEAR→LINEAR
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class LinearToLinearPayload {

        @Test
        fun `linear task completes with resultJson - next task receives it as payloadJson`() = runTest {
            val def = twoStepLinearDef()
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    id = taskId, workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.PROCESSING, handlerKey = "step1.handler",
                ),
            )

            val resultPayload = """{"output":"step1-result","count":42}"""
            barrier.onTaskCompleted(taskId, wfId, 1, TaskStatus.COMPLETED, resultPayload)

            // Workflow advanced to sequence 2
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())

            // Next task's payload should be the previous task's resultJson
            val seq2Tasks = readTasksDirect(wfId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals(resultPayload, seq2Tasks[0]["PAYLOAD"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 11b: Payload propagation — LINEAR→SCATTER
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class LinearToScatterPayload {

        @Test
        fun `linear task completes with resultJson - scatter task receives it as payloadJson`() = runTest {
            // LINEAR step → fan-out (SCATTER → PARALLEL)
            val def = WorkflowDefinition(
                activities = listOf(
                    ActivityDefinition(name = "step1", transition = "step1.handler"),
                    ActivityDefinition(
                        name = "scatter-activity",
                        transition = "parallel.handler",
                        fanOut = FanOutDefinition(
                            transition = "scatter.handler",
                            joinPolicy = JoinPolicy.All,
                        ),
                    ),
                ),
            )
            val wfId = randomId()
            val taskId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)
            insertTaskDirect(
                makeTask(
                    id = taskId, workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.PROCESSING, handlerKey = "step1.handler",
                ),
            )

            val resultPayload = """{"users":["alice","bob"]}"""
            barrier.onTaskCompleted(taskId, wfId, 1, TaskStatus.COMPLETED, resultPayload)

            // Workflow advanced to sequence 2 (SCATTER phase)
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())

            // Scatter task's payload should be the previous task's resultJson
            // Scatter tasks use activity.transition as handler key
            val seq2Tasks = readTasksDirect(wfId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals("parallel.handler", seq2Tasks[0]["HANDLER_KEY"])
            assertEquals(resultPayload, seq2Tasks[0]["PAYLOAD"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Test 12: Scatter → parallel handoff
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class ScatterToParallelHandoff {

        @Test
        fun `scatter task completes with payloads - CAS winner reads result, inserts N sub-tasks at next sequence`() = runTest {
            val def = fanOutDef(joinPolicy = JoinPolicy.All)
            val wfId = randomId()
            val wf = makeWorkflow(id = wfId, definition = def, currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)

            // Scatter task at sequence 1 with payloads in result
            val scatterTaskId = randomId()
            val scatterPayloads = listOf(
                """{"item":"A"}""",
                """{"item":"B"}""",
                """{"item":"C"}""",
            )
            val scatterResultJson = objectMapper.writeValueAsString(scatterPayloads)

            insertTaskDirect(
                makeTask(
                    id = scatterTaskId, workflowId = wfId, sequenceNumber = 1,
                    status = TaskStatus.PROCESSING, handlerKey = "scatter.handler",
                ),
            )

            barrier.onTaskCompleted(scatterTaskId, wfId, 1, TaskStatus.COMPLETED, scatterResultJson)

            // Workflow advanced to sequence 2 (PARALLEL phase)
            val updatedWf = readWorkflowDirect(wfId)
            assertNotNull(updatedWf)
            assertEquals(2, (updatedWf["CURRENT_SEQUENCE"] as Number).toInt())

            // 3 sub-tasks created at sequence 2, one per scatter payload
            val seq2Tasks = readTasksDirect(wfId, 2)
            assertEquals(3, seq2Tasks.size)

            // All sub-tasks use fanOut.transition as handler key (PARALLEL phase mapping)
            assertTrue(seq2Tasks.all { it["HANDLER_KEY"] == "scatter.handler" })

            // All sub-tasks are PENDING
            assertTrue(seq2Tasks.all { it["STATUS"] == "PENDING" })

            // Each sub-task has the correct payload from the scatter result
            val payloads = seq2Tasks.map { it["PAYLOAD"] as String }.sorted()
            assertEquals(scatterPayloads.sorted(), payloads)
        }
    }
}
