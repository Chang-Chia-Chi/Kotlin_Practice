package com.workflow.engine

import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
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
class RepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: WorkflowRepository
    private lateinit var taskRepo: TaskRepository

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = WorkflowRepository(jdbi)
        taskRepo = TaskRepository(jdbi)
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
        definitionJson: String = """{"activities":[]}""",
        currentSequence: Int = 1,
        version: Int = 0,
        status: WorkflowStatus = WorkflowStatus.RUNNING,
        createdAt: Instant = now(),
        updatedAt: Instant = now(),
    ) = WorkflowRun(
        id = id,
        definitionJson = definitionJson,
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
    ) = Task(
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

    /** Insert a workflow directly via SQL for test setup (independent of repo under test). */
    private fun insertWorkflowDirect(run: WorkflowRun) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO workflow (id, definition, current_sequence, version, status, created_at, updated_at)
                   VALUES (:id, :definition, :currentSequence, :version, :status, :createdAt, :updatedAt)"""
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

    /** Insert a task directly via SQL for test setup (independent of repo under test). */
    private fun insertTaskDirect(task: Task) {
        jdbi.useHandle<Exception> { handle ->
            val stmt = handle.createUpdate(
                """INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, payload, result,
                   claimed_by, claimed_at, completed_at, retry_count, max_retries, deadline_at)
                   VALUES (:id, :workflowId, :sequenceNumber, :status, :handlerKey, :payload, :result,
                   :claimedBy, :claimedAt, :completedAt, :retryCount, :maxRetries, :deadlineAt)"""
            )
                .bind("id", task.id)
                .bind("workflowId", task.workflowId)
                .bind("sequenceNumber", task.sequenceNumber)
                .bind("status", task.status.name)
                .bind("handlerKey", task.handlerKey)
                .bind("retryCount", task.retryCount)
                .bind("maxRetries", task.maxRetries)

            // Oracle JDBC requires explicit type for null bindings
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

    /** Read a workflow row directly via SQL for assertion (independent of repo under test). */
    private fun readWorkflowDirect(id: String): Map<String, Any?>? {
        return jdbi.withHandle<Map<String, Any?>?, Exception> { handle ->
            handle.createQuery("SELECT * FROM workflow WHERE id = :id")
                .bind("id", id)
                .mapToMap()
                .findOne()
                .orElse(null)
                ?.let { raw ->
                    val ci = java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
                    raw.forEach { (k, v) -> ci[k] = if (v is java.sql.Clob) v.characterStream.readText() else v }
                    ci
                }
        }
    }

    /** Read a task row directly via SQL for assertion (independent of repo under test). */
    private fun readTaskDirect(id: String): Map<String, Any?>? {
        return jdbi.withHandle<Map<String, Any?>?, Exception> { handle ->
            handle.createQuery("SELECT * FROM task WHERE id = :id")
                .bind("id", id)
                .mapToMap()
                .findOne()
                .orElse(null)
                ?.let { raw ->
                    val ci = java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
                    raw.forEach { (k, v) -> ci[k] = if (v is java.sql.Clob) v.characterStream.readText() else v }
                    ci
                }
        }
    }

    /** Count tasks directly via SQL. */
    private fun countTasksDirect(workflowId: String, sequenceNumber: Int): Int {
        return jdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND sequence_number = :seq"
            )
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .mapTo(Int::class.java)
                .one()
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WorkflowRepository Tests
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class WorkflowRepositoryTests {

        // ── insert + findById ────────────────────────────────────────────

        @Test
        fun `insert and findById round-trip`() = runTest {
            val ts = now()
            val wf = makeWorkflow(
                definitionJson = """{"activities":[{"name":"step1"}]}""",
                currentSequence = 1,
                version = 0,
                status = WorkflowStatus.RUNNING,
                createdAt = ts,
                updatedAt = ts,
            )

            workflowRepo.insert(wf)
            val found = workflowRepo.findById(wf.id)

            assertNotNull(found)
            assertEquals(wf.id, found.id)
            assertEquals(wf.definitionJson, found.definitionJson)
            assertEquals(wf.currentSequence, found.currentSequence)
            assertEquals(wf.version, found.version)
            assertEquals(wf.status, found.status)
            assertEquals(wf.createdAt, found.createdAt)
            assertEquals(wf.updatedAt, found.updatedAt)
        }

        @Test
        fun `findById returns null for non-existent id`() = runTest {
            val found = workflowRepo.findById(randomId())
            assertNull(found)
        }

        @Test
        fun `insert preserves large CLOB definition`() = runTest {
            val largeEntries = (1..500).joinToString(",") { """{"name":"act_$it"}""" }
            val largeJson = """{"activities":[$largeEntries]}"""
            val wf = makeWorkflow(definitionJson = largeJson)

            workflowRepo.insert(wf)
            val found = workflowRepo.findById(wf.id)

            assertNotNull(found)
            assertEquals(largeJson, found.definitionJson)
        }

        // ── casAdvance ───────────────────────────────────────────────────

        @Test
        fun `casAdvance succeeds with matching sequence and version`() = runTest {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            workflowRepo.insert(wf)

            val result = workflowRepo.casAdvance(
                id = wf.id,
                expectedSequence = 1,
                nextSequence = 2,
                expectedVersion = 0,
            )

            assertTrue(result)
            val found = workflowRepo.findById(wf.id)
            assertNotNull(found)
            assertEquals(2, found.currentSequence)
            assertEquals(1, found.version)
        }

        @Test
        fun `casAdvance fails on version mismatch`() = runTest {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            workflowRepo.insert(wf)

            val result = workflowRepo.casAdvance(
                id = wf.id,
                expectedSequence = 1,
                nextSequence = 2,
                expectedVersion = 99,
            )

            assertFalse(result)
            // Row unchanged
            val found = workflowRepo.findById(wf.id)
            assertNotNull(found)
            assertEquals(1, found.currentSequence)
            assertEquals(0, found.version)
        }

        @Test
        fun `casAdvance fails on sequence mismatch`() = runTest {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            workflowRepo.insert(wf)

            val result = workflowRepo.casAdvance(
                id = wf.id,
                expectedSequence = 5,
                nextSequence = 6,
                expectedVersion = 0,
            )

            assertFalse(result)
            // Row unchanged
            val found = workflowRepo.findById(wf.id)
            assertNotNull(found)
            assertEquals(1, found.currentSequence)
            assertEquals(0, found.version)
        }

        @Test
        fun `casAdvance fails on both sequence and version mismatch`() = runTest {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            workflowRepo.insert(wf)

            val result = workflowRepo.casAdvance(
                id = wf.id,
                expectedSequence = 5,
                nextSequence = 6,
                expectedVersion = 99,
            )

            assertFalse(result)
        }

        @Test
        fun `casAdvance fails for non-existent id`() = runTest {
            val result = workflowRepo.casAdvance(
                id = randomId(),
                expectedSequence = 1,
                nextSequence = 2,
                expectedVersion = 0,
            )
            assertFalse(result)
        }

        @Test
        fun `casAdvance increments version on success`() = runTest {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            workflowRepo.insert(wf)

            // First CAS
            assertTrue(workflowRepo.casAdvance(wf.id, 1, 2, 0))
            val afterFirst = workflowRepo.findById(wf.id)!!
            assertEquals(2, afterFirst.currentSequence)
            assertEquals(1, afterFirst.version)

            // Second CAS with updated version
            assertTrue(workflowRepo.casAdvance(wf.id, 2, 3, 1))
            val afterSecond = workflowRepo.findById(wf.id)!!
            assertEquals(3, afterSecond.currentSequence)
            assertEquals(2, afterSecond.version)
        }

        @Test
        fun `casAdvance second attempt fails after first succeeds (stale version)`() = runTest {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            workflowRepo.insert(wf)

            // First CAS wins
            assertTrue(workflowRepo.casAdvance(wf.id, 1, 2, 0))
            // Second CAS with stale version loses
            assertFalse(workflowRepo.casAdvance(wf.id, 1, 2, 0))
        }

        // ── casAdvanceWithHandle ─────────────────────────────────────────

        @Test
        fun `casAdvanceWithHandle succeeds within transaction`() {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                workflowRepo.casAdvanceWithHandle(handle, wf.id, 1, 2, 0)
            }

            assertTrue(result)
            val row = readWorkflowDirect(wf.id)!!
            assertEquals(2, (row["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (row["VERSION"] as Number).toInt())
        }

        @Test
        fun `casAdvanceWithHandle fails on version mismatch`() {
            val wf = makeWorkflow(currentSequence = 1, version = 0)
            insertWorkflowDirect(wf)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                workflowRepo.casAdvanceWithHandle(handle, wf.id, 1, 2, 99)
            }

            assertFalse(result)
        }

        // ── findByIdWithHandle ────────────────────────────────────────────

        @Test
        fun `findByIdWithHandle returns workflow within transaction`() {
            val wf = makeWorkflow(
                definitionJson = """{"activities":[{"name":"txn-test"}]}""",
                currentSequence = 2,
                version = 3,
                status = WorkflowStatus.RUNNING,
            )
            insertWorkflowDirect(wf)

            val found = jdbi.inTransaction<WorkflowRun?, Exception> { handle ->
                workflowRepo.findByIdWithHandle(handle, wf.id)
            }

            assertNotNull(found)
            assertEquals(wf.id, found.id)
            assertEquals(wf.definitionJson, found.definitionJson)
            assertEquals(2, found.currentSequence)
            assertEquals(3, found.version)
            assertEquals(WorkflowStatus.RUNNING, found.status)
        }

        @Test
        fun `findByIdWithHandle returns null for non-existent id`() {
            val found = jdbi.inTransaction<WorkflowRun?, Exception> { handle ->
                workflowRepo.findByIdWithHandle(handle, randomId())
            }
            assertNull(found)
        }

        // ── updateStatus ─────────────────────────────────────────────────

        @Test
        fun `updateStatus changes workflow status`() = runTest {
            val wf = makeWorkflow(status = WorkflowStatus.RUNNING)
            workflowRepo.insert(wf)

            val result = workflowRepo.updateStatus(wf.id, WorkflowStatus.COMPLETED)

            assertTrue(result)
            val found = workflowRepo.findById(wf.id)!!
            assertEquals(WorkflowStatus.COMPLETED, found.status)
        }

        @Test
        fun `updateStatus to FAILED`() = runTest {
            val wf = makeWorkflow(status = WorkflowStatus.RUNNING)
            workflowRepo.insert(wf)

            assertTrue(workflowRepo.updateStatus(wf.id, WorkflowStatus.FAILED))
            assertEquals(WorkflowStatus.FAILED, workflowRepo.findById(wf.id)!!.status)
        }

        @Test
        fun `updateStatus returns false for non-existent id`() = runTest {
            val result = workflowRepo.updateStatus(randomId(), WorkflowStatus.COMPLETED)
            assertFalse(result)
        }

        // ── updateStatusWithHandle ───────────────────────────────────────

        @Test
        fun `updateStatusWithHandle changes status within transaction`() {
            val wf = makeWorkflow(status = WorkflowStatus.RUNNING)
            insertWorkflowDirect(wf)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                workflowRepo.updateStatusWithHandle(handle, wf.id, WorkflowStatus.COMPLETED)
            }

            assertTrue(result)
            val row = readWorkflowDirect(wf.id)!!
            assertEquals("COMPLETED", row["STATUS"])
        }

        // ── findStuck ────────────────────────────────────────────────────

        @Test
        fun `findStuck returns workflow with no non-terminal tasks past grace period`() = runTest {
            val pastTime = now().minus(Duration.ofMinutes(10))
            val wf = makeWorkflow(
                status = WorkflowStatus.RUNNING,
                currentSequence = 1,
                updatedAt = pastTime,
                createdAt = pastTime,
            )
            workflowRepo.insert(wf)

            // Insert only terminal tasks at current sequence
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))

            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertEquals(1, stuck.size)
            assertEquals(wf.id, stuck[0].id)
        }

        @Test
        fun `findStuck excludes workflow with non-terminal tasks`() = runTest {
            val pastTime = now().minus(Duration.ofMinutes(10))
            val wf = makeWorkflow(
                status = WorkflowStatus.RUNNING,
                currentSequence = 1,
                updatedAt = pastTime,
                createdAt = pastTime,
            )
            workflowRepo.insert(wf)

            // Insert one PENDING (non-terminal) task
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))

            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertTrue(stuck.isEmpty())
        }

        @Test
        fun `findStuck excludes workflow within grace period`() = runTest {
            val recentTime = now().minus(Duration.ofMinutes(1))
            val wf = makeWorkflow(
                status = WorkflowStatus.RUNNING,
                currentSequence = 1,
                updatedAt = recentTime,
                createdAt = recentTime,
            )
            workflowRepo.insert(wf)

            // No tasks at all — stuck but within grace period
            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertTrue(stuck.isEmpty())
        }

        @Test
        fun `findStuck excludes COMPLETED workflows`() = runTest {
            val pastTime = now().minus(Duration.ofMinutes(10))
            val wf = makeWorkflow(
                status = WorkflowStatus.COMPLETED,
                currentSequence = 1,
                updatedAt = pastTime,
                createdAt = pastTime,
            )
            workflowRepo.insert(wf)

            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertTrue(stuck.isEmpty())
        }

        @Test
        fun `findStuck excludes FAILED workflows`() = runTest {
            val pastTime = now().minus(Duration.ofMinutes(10))
            val wf = makeWorkflow(
                status = WorkflowStatus.FAILED,
                currentSequence = 1,
                updatedAt = pastTime,
                createdAt = pastTime,
            )
            workflowRepo.insert(wf)

            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertTrue(stuck.isEmpty())
        }

        @Test
        fun `findStuck ignores non-terminal tasks at different sequence`() = runTest {
            val pastTime = now().minus(Duration.ofMinutes(10))
            val wf = makeWorkflow(
                status = WorkflowStatus.RUNNING,
                currentSequence = 2,
                updatedAt = pastTime,
                createdAt = pastTime,
            )
            workflowRepo.insert(wf)

            // Non-terminal task at sequence 1 (not current), terminal task at sequence 2 (current)
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.COMPLETED))

            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertEquals(1, stuck.size)
            assertEquals(wf.id, stuck[0].id)
        }

        @Test
        fun `findStuck returns multiple stuck workflows`() = runTest {
            val pastTime = now().minus(Duration.ofMinutes(10))
            val wf1 = makeWorkflow(status = WorkflowStatus.RUNNING, updatedAt = pastTime, createdAt = pastTime)
            val wf2 = makeWorkflow(status = WorkflowStatus.RUNNING, updatedAt = pastTime, createdAt = pastTime)
            workflowRepo.insert(wf1)
            workflowRepo.insert(wf2)

            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertEquals(2, stuck.size)
            val ids = stuck.map { it.id }.toSet()
            assertTrue(ids.contains(wf1.id))
            assertTrue(ids.contains(wf2.id))
        }

        @Test
        fun `findStuck with PROCESSING task at current sequence is not stuck`() = runTest {
            val pastTime = now().minus(Duration.ofMinutes(10))
            val wf = makeWorkflow(
                status = WorkflowStatus.RUNNING,
                currentSequence = 1,
                updatedAt = pastTime,
                createdAt = pastTime,
            )
            workflowRepo.insert(wf)
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PROCESSING))

            val stuck = workflowRepo.findStuck(Duration.ofMinutes(5))
            assertTrue(stuck.isEmpty())
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // TaskRepository Tests
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class TaskRepositoryTests {

        // ── insertBatch ──────────────────────────────────────────────────

        @Test
        fun `insertBatch inserts all tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val tasks = (1..5).map { i ->
                makeTask(
                    workflowId = wf.id,
                    sequenceNumber = 1,
                    handlerKey = "test.handler.$i",
                    payloadJson = """{"index":$i}""",
                )
            }

            taskRepo.insertBatch(tasks)

            assertEquals(5, countTasksDirect(wf.id, 1))
        }

        @Test
        fun `insertBatch with single task`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(workflowId = wf.id, payloadJson = """{"key":"value"}""")
            taskRepo.insertBatch(listOf(task))

            val row = readTaskDirect(task.id)
            assertNotNull(row)
            assertEquals(wf.id, row["WORKFLOW_ID"])
        }

        @Test
        fun `insertBatch with empty list is no-op`() = runTest {
            taskRepo.insertBatch(emptyList())
            // No exception thrown, no rows inserted
        }

        @Test
        fun `insertBatch preserves all fields`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val ts = now()

            val task = makeTask(
                workflowId = wf.id,
                sequenceNumber = 3,
                status = TaskStatus.PENDING,
                handlerKey = "order.process",
                payloadJson = """{"orderId":42}""",
                maxRetries = 3,
                deadlineAt = ts.plus(Duration.ofHours(1)),
            )

            taskRepo.insertBatch(listOf(task))

            val row = readTaskDirect(task.id)!!
            assertEquals(task.id, row["ID"])
            assertEquals(wf.id, row["WORKFLOW_ID"])
            assertEquals(3, (row["SEQUENCE_NUMBER"] as Number).toInt())
            assertEquals("PENDING", row["STATUS"])
            assertEquals("order.process", row["HANDLER_KEY"])
            assertEquals(0, (row["RETRY_COUNT"] as Number).toInt())
            assertEquals(3, (row["MAX_RETRIES"] as Number).toInt())
        }

        // ── insertBatchWithHandle ────────────────────────────────────────

        @Test
        fun `insertBatchWithHandle inserts within transaction`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)

            val tasks = (1..3).map { makeTask(workflowId = wf.id) }

            jdbi.useTransaction<Exception> { handle ->
                taskRepo.insertBatchWithHandle(handle, tasks)
            }

            assertEquals(3, countTasksDirect(wf.id, 1))
        }

        // ── countNonTerminal ─────────────────────────────────────────────

        @Test
        fun `countNonTerminal counts PENDING and PROCESSING tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PROCESSING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))

            val count = taskRepo.countNonTerminal(wf.id, 1)
            assertEquals(2, count)
        }

        @Test
        fun `countNonTerminal returns zero when all tasks terminal`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))

            assertEquals(0, taskRepo.countNonTerminal(wf.id, 1))
        }

        @Test
        fun `countNonTerminal returns zero when no tasks exist`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            assertEquals(0, taskRepo.countNonTerminal(wf.id, 1))
        }

        @Test
        fun `countNonTerminal scoped to sequence number`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.PENDING))

            assertEquals(1, taskRepo.countNonTerminal(wf.id, 1))
            assertEquals(1, taskRepo.countNonTerminal(wf.id, 2))
        }

        // ── countNonTerminalWithHandle ───────────────────────────────────

        @Test
        fun `countNonTerminalWithHandle works within transaction`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))

            val count = jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.countNonTerminalWithHandle(handle, wf.id, 1)
            }

            assertEquals(1, count)
        }

        // ── countFailed ──────────────────────────────────────────────────

        @Test
        fun `countFailed counts FAILED tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))

            assertEquals(2, taskRepo.countFailed(wf.id, 1))
        }

        @Test
        fun `countFailed returns zero when no failures`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))

            assertEquals(0, taskRepo.countFailed(wf.id, 1))
        }

        @Test
        fun `countFailed scoped to sequence number`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.FAILED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.FAILED))

            assertEquals(1, taskRepo.countFailed(wf.id, 1))
            assertEquals(2, taskRepo.countFailed(wf.id, 2))
        }

        // ── countFailedWithHandle ────────────────────────────────────────

        @Test
        fun `countFailedWithHandle works within transaction`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))

            val count = jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.countFailedWithHandle(handle, wf.id, 1)
            }

            assertEquals(2, count)
        }

        // ── countTotal ───────────────────────────────────────────────────

        @Test
        fun `countTotal counts all tasks at sequence`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.FAILED))

            assertEquals(3, taskRepo.countTotal(wf.id, 1))
        }

        @Test
        fun `countTotal returns zero when no tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            assertEquals(0, taskRepo.countTotal(wf.id, 1))
        }

        @Test
        fun `countTotal scoped to sequence number`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.PENDING))

            assertEquals(1, taskRepo.countTotal(wf.id, 1))
            assertEquals(2, taskRepo.countTotal(wf.id, 2))
        }

        // ── countTotalWithHandle ─────────────────────────────────────────

        @Test
        fun `countTotalWithHandle works within transaction`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))

            val count = jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.countTotalWithHandle(handle, wf.id, 1)
            }

            assertEquals(2, count)
        }

        // ── updateStatus ─────────────────────────────────────────────────

        @Test
        fun `updateStatus changes task status and sets completed_at for terminal`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING)
            insertTaskDirect(task)

            taskRepo.updateStatus(task.id, TaskStatus.COMPLETED, """{"result":"ok"}""")

            val row = readTaskDirect(task.id)!!
            assertEquals("COMPLETED", row["STATUS"])
            assertNotNull(row["COMPLETED_AT"], "completed_at should be set for terminal status")
        }

        @Test
        fun `updateStatus sets resultJson`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING)
            insertTaskDirect(task)

            taskRepo.updateStatus(task.id, TaskStatus.COMPLETED, """{"output":42}""")

            val row = readTaskDirect(task.id)!!
            assertEquals("""{"output":42}""", row["RESULT"])
        }

        @Test
        fun `updateStatus with null resultJson`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING)
            insertTaskDirect(task)

            taskRepo.updateStatus(task.id, TaskStatus.FAILED, null)

            val row = readTaskDirect(task.id)!!
            assertEquals("FAILED", row["STATUS"])
            assertNull(row["RESULT"])
            assertNotNull(row["COMPLETED_AT"], "completed_at should be set for FAILED (terminal) status")
        }

        // ── updateStatusWithHandle ───────────────────────────────────────

        @Test
        fun `updateStatusWithHandle changes status within transaction`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING)
            insertTaskDirect(task)

            jdbi.useTransaction<Exception> { handle ->
                taskRepo.updateStatusWithHandle(handle, task.id, TaskStatus.COMPLETED, """{"done":true}""")
            }

            val row = readTaskDirect(task.id)!!
            assertEquals("COMPLETED", row["STATUS"])
        }

        // ── claimNext ────────────────────────────────────────────────────

        @Test
        fun `claimNext claims PENDING tasks and returns them`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val t1 = makeTask(workflowId = wf.id, status = TaskStatus.PENDING, handlerKey = "a.handler")
            val t2 = makeTask(workflowId = wf.id, status = TaskStatus.PENDING, handlerKey = "b.handler")
            insertTaskDirect(t1)
            insertTaskDirect(t2)

            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(2, claimed.size)
            claimed.forEach { task ->
                assertEquals(TaskStatus.PROCESSING, task.status)
                assertEquals("worker-1", task.claimedBy)
                assertNotNull(task.claimedAt)
            }
        }

        @Test
        fun `claimNext respects limit`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            repeat(5) {
                insertTaskDirect(makeTask(workflowId = wf.id, status = TaskStatus.PENDING))
            }

            val claimed = taskRepo.claimNext("worker-1", 2)
            assertEquals(2, claimed.size)
        }

        @Test
        fun `claimNext skips non-PENDING tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING))
            insertTaskDirect(makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED))
            insertTaskDirect(makeTask(workflowId = wf.id, status = TaskStatus.FAILED))
            val pending = makeTask(workflowId = wf.id, status = TaskStatus.PENDING, handlerKey = "only.pending")
            insertTaskDirect(pending)

            val claimed = taskRepo.claimNext("worker-1", 10)
            assertEquals(1, claimed.size)
            assertEquals("only.pending", claimed[0].handlerKey)
        }

        @Test
        fun `claimNext returns empty list when no PENDING tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED))

            val claimed = taskRepo.claimNext("worker-1", 10)
            assertTrue(claimed.isEmpty())
        }

        @Test
        fun `claimNext mutates task status in database`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.PENDING)
            insertTaskDirect(task)

            taskRepo.claimNext("worker-1", 1)

            val row = readTaskDirect(task.id)!!
            assertEquals("PROCESSING", row["STATUS"])
            assertEquals("worker-1", row["CLAIMED_BY"])
            assertNotNull(row["CLAIMED_AT"])
        }

        @Test
        fun `claimNext returns tasks with all fields populated`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val task = makeTask(
                workflowId = wf.id,
                sequenceNumber = 2,
                status = TaskStatus.PENDING,
                handlerKey = "order.process",
                payloadJson = """{"orderId":42}""",
                maxRetries = 3,
            )
            insertTaskDirect(task)

            val claimed = taskRepo.claimNext("worker-1", 1)
            assertEquals(1, claimed.size)
            val c = claimed[0]
            assertEquals(task.id, c.id)
            assertEquals(wf.id, c.workflowId)
            assertEquals(2, c.sequenceNumber)
            assertEquals("order.process", c.handlerKey)
            assertEquals("""{"orderId":42}""", c.payloadJson)
            assertEquals(3, c.maxRetries)
        }

        // ── findByWorkflowAndSequence ────────────────────────────────────

        @Test
        fun `findByWorkflowAndSequence returns matching tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, handlerKey = "h1"))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 1, handlerKey = "h2"))
            insertTaskDirect(makeTask(workflowId = wf.id, sequenceNumber = 2, handlerKey = "h3"))

            val tasks = taskRepo.findByWorkflowAndSequence(wf.id, 1)
            assertEquals(2, tasks.size)
            assertTrue(tasks.all { it.sequenceNumber == 1 })
        }

        @Test
        fun `findByWorkflowAndSequence returns empty for non-existent sequence`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val tasks = taskRepo.findByWorkflowAndSequence(wf.id, 99)
            assertTrue(tasks.isEmpty())
        }

        @Test
        fun `findByWorkflowAndSequence scoped to workflow`() = runTest {
            val wf1 = makeWorkflow()
            val wf2 = makeWorkflow()
            workflowRepo.insert(wf1)
            workflowRepo.insert(wf2)

            insertTaskDirect(makeTask(workflowId = wf1.id, sequenceNumber = 1))
            insertTaskDirect(makeTask(workflowId = wf2.id, sequenceNumber = 1))

            val tasks = taskRepo.findByWorkflowAndSequence(wf1.id, 1)
            assertEquals(1, tasks.size)
            assertEquals(wf1.id, tasks[0].workflowId)
        }

        // ── findExpired ──────────────────────────────────────────────────

        @Test
        fun `findExpired returns tasks past deadline`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val pastDeadline = now().minus(Duration.ofMinutes(5))
            val futureDeadline = now().plus(Duration.ofHours(1))

            insertTaskDirect(
                makeTask(
                    workflowId = wf.id,
                    status = TaskStatus.PROCESSING,
                    deadlineAt = pastDeadline,
                    claimedBy = "worker-1",
                    claimedAt = pastDeadline.minus(Duration.ofMinutes(10)),
                )
            )
            insertTaskDirect(
                makeTask(
                    workflowId = wf.id,
                    status = TaskStatus.PROCESSING,
                    deadlineAt = futureDeadline,
                    claimedBy = "worker-2",
                    claimedAt = now(),
                )
            )

            val expired = taskRepo.findExpired(now())
            assertEquals(1, expired.size)
        }

        @Test
        fun `findExpired returns empty when no tasks expired`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val futureDeadline = now().plus(Duration.ofHours(1))
            insertTaskDirect(
                makeTask(
                    workflowId = wf.id,
                    status = TaskStatus.PROCESSING,
                    deadlineAt = futureDeadline,
                )
            )

            val expired = taskRepo.findExpired(now())
            assertTrue(expired.isEmpty())
        }

        @Test
        fun `findExpired excludes terminal tasks even if past deadline`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val pastDeadline = now().minus(Duration.ofMinutes(5))
            insertTaskDirect(
                makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED, deadlineAt = pastDeadline)
            )
            insertTaskDirect(
                makeTask(workflowId = wf.id, status = TaskStatus.FAILED, deadlineAt = pastDeadline)
            )

            val expired = taskRepo.findExpired(now())
            assertTrue(expired.isEmpty())
        }

        @Test
        fun `findExpired excludes tasks with null deadline`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(
                makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING, deadlineAt = null)
            )

            val expired = taskRepo.findExpired(now())
            assertTrue(expired.isEmpty())
        }

        @Test
        fun `findExpired excludes PENDING tasks past deadline`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val pastDeadline = now().minus(Duration.ofMinutes(5))
            insertTaskDirect(
                makeTask(workflowId = wf.id, status = TaskStatus.PENDING, deadlineAt = pastDeadline)
            )

            val expired = taskRepo.findExpired(now())
            assertTrue(expired.isEmpty())
        }
    }
}
