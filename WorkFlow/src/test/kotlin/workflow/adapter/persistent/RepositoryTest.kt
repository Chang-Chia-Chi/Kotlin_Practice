package com.workflow.workflow.adapter.persistent

import com.workflow.infrastructure.persistence.OracleTestContainer

import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import kotlinx.coroutines.async
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
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
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
        definitionJson: String = """{"activities":{}}""",
        version: Int = 0,
        status: WorkflowStatus = WorkflowStatus.RUNNING,
        createdAt: Instant = now(),
        updatedAt: Instant = now(),
        deadlineAt: Instant = now().plus(java.time.Duration.ofMinutes(30)),
    ) = WorkflowRun(
        id = id,
        definitionJson = definitionJson,
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
    ) = Task(
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

    /** Insert a workflow directly via SQL for test setup (independent of repo under test). */
    private fun insertWorkflowDirect(run: WorkflowRun) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at)
                   VALUES (:id, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)"""
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

    /** Insert a task directly via SQL for test setup (independent of repo under test). */
    private fun insertTaskDirect(task: Task, enqueuedAt: Instant? = null) {
        jdbi.useHandle<Exception> { handle ->
            val columns = buildString {
                append("id, workflow_id, sequence_number, status, handler_key, item, result, ")
                append("claimed_by, claimed_at, completed_at, retry_count, max_retries, deadline_at, not_before, ")
                append("backoff_base, backoff_cap")
                if (enqueuedAt != null) append(", enqueued_at")
            }
            val values = buildString {
                append(":id, :workflowId, :sequenceNumber, :status, :handlerKey, :item, :result, ")
                append(":claimedBy, :claimedAt, :completedAt, :retryCount, :maxRetries, :deadlineAt, :notBefore, ")
                append(":backoffBase, :backoffCap")
                if (enqueuedAt != null) append(", :enqueuedAt")
            }
            val stmt = handle.createUpdate("INSERT INTO task ($columns) VALUES ($values)")
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

            bindStringOrNull("item", task.item)
            bindStringOrNull("result", task.resultJson)
            bindStringOrNull("claimedBy", task.claimedBy)
            bindTimestampOrNull("claimedAt", task.claimedAt)
            bindTimestampOrNull("completedAt", task.completedAt)
            bindTimestampOrNull("deadlineAt", task.deadlineAt)
            bindTimestampOrNull("notBefore", task.notBefore)
            stmt.bind("backoffBase", task.backoffBase)
            stmt.bind("backoffCap", task.backoffCap)
            if (enqueuedAt != null) {
                stmt.bind("enqueuedAt", LocalDateTime.ofInstant(enqueuedAt, ZoneOffset.UTC))
            }

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

        // ── findByIdForUpdate ────────────────────────────────────────────

        @Test
        fun `findByIdForUpdate returns workflow within transaction`() {
            val wf = makeWorkflow(
                definitionJson = """{"activities":[{"name":"lock-test"}]}""",
                version = 5,
                status = WorkflowStatus.RUNNING,
            )
            insertWorkflowDirect(wf)

            val found = jdbi.inTransaction<WorkflowRun?, Exception> { handle ->
                workflowRepo.findByIdForUpdate(handle, wf.id)
            }

            assertNotNull(found)
            assertEquals(wf.id, found.id)
            assertEquals(wf.definitionJson, found.definitionJson)
            assertEquals(5, found.version)
            assertEquals(WorkflowStatus.RUNNING, found.status)
        }

        @Test
        fun `findByIdForUpdate returns null for non-existent id`() {
            val found = jdbi.inTransaction<WorkflowRun?, Exception> { handle ->
                workflowRepo.findByIdForUpdate(handle, randomId())
            }
            assertNull(found)
        }

        // ── incrementVersionWithHandle ──────────────────────────────────

        @Test
        fun `incrementVersionWithHandle bumps version by 1`() {
            val wf = makeWorkflow(version = 0)
            insertWorkflowDirect(wf)

            jdbi.useTransaction<Exception> { handle ->
                workflowRepo.incrementVersionWithHandle(handle, wf.id)
            }

            val row = readWorkflowDirect(wf.id)!!
            assertEquals(1, (row["VERSION"] as Number).toInt())
        }

        @Test
        fun `incrementVersionWithHandle increments consecutively`() {
            val wf = makeWorkflow(version = 0)
            insertWorkflowDirect(wf)

            jdbi.useTransaction<Exception> { handle ->
                workflowRepo.incrementVersionWithHandle(handle, wf.id)
            }
            jdbi.useTransaction<Exception> { handle ->
                workflowRepo.incrementVersionWithHandle(handle, wf.id)
            }

            val row = readWorkflowDirect(wf.id)!!
            assertEquals(2, (row["VERSION"] as Number).toInt())
        }

        // ── findByIdWithHandle ────────────────────────────────────────────

        @Test
        fun `findByIdWithHandle returns workflow within transaction`() {
            val wf = makeWorkflow(
                definitionJson = """{"activities":[{"name":"txn-test"}]}""",

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

            val result = workflowRepo.updateStatus(wf.id, WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING)

            assertTrue(result)
            val found = workflowRepo.findById(wf.id)!!
            assertEquals(WorkflowStatus.COMPLETED, found.status)
        }

        @Test
        fun `updateStatus to FAILED`() = runTest {
            val wf = makeWorkflow(status = WorkflowStatus.RUNNING)
            workflowRepo.insert(wf)

            assertTrue(workflowRepo.updateStatus(wf.id, WorkflowStatus.FAILED, WorkflowStatus.RUNNING))
            assertEquals(WorkflowStatus.FAILED, workflowRepo.findById(wf.id)!!.status)
        }

        @Test
        fun `updateStatus returns false for non-existent id`() = runTest {
            val result = workflowRepo.updateStatus(randomId(), WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING)
            assertFalse(result)
        }

        // ── updateStatusWithHandle ───────────────────────────────────────

        @Test
        fun `updateStatusWithHandle changes status within transaction`() {
            val wf = makeWorkflow(status = WorkflowStatus.RUNNING)
            insertWorkflowDirect(wf)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                workflowRepo.updateStatusWithHandle(handle, wf.id, WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING)
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

            // Each workflow needs terminal tasks at max sequence to be detected as stuck
            insertTaskDirect(makeTask(workflowId = wf1.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))
            insertTaskDirect(makeTask(workflowId = wf2.id, sequenceNumber = 1, status = TaskStatus.COMPLETED))

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
                    item = """{"index":$i}""",
                )
            }

            taskRepo.insertBatch(tasks)

            assertEquals(5, countTasksDirect(wf.id, 1))
        }

        @Test
        fun `insertBatch with single task`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(workflowId = wf.id, item = """{"key":"value"}""")
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
                item = """{"orderId":42}""",
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

        @Test
        fun `updateStatusWithHandle returns false for already-terminal task`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED)
            insertTaskDirect(task)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(handle, task.id, TaskStatus.COMPLETED, """{"retry":true}""")
            }

            assertFalse(result, "should return false when task is already terminal")
            val row = readTaskDirect(task.id)!!
            assertNull(row["RESULT"], "result should remain unchanged")
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
                item = """{"orderId":42}""",
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
            assertEquals("""{"orderId":42}""", c.item)
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

        // ── R0.1 — Oracle null CLOB binding ─────────────────────────────

        @Test
        fun `updateStatusWithHandle with null resultJson does not throw on Oracle CLOB`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING)
            insertTaskDirect(task)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(handle, task.id, TaskStatus.FAILED, null)
            }

            assertTrue(result, "updateStatusWithHandle should succeed with null resultJson")
            val row = readTaskDirect(task.id)!!
            assertEquals("FAILED", row["STATUS"])
            assertNull(row["RESULT"], "result column should be null")
            assertNotNull(row["COMPLETED_AT"], "completed_at should be set for terminal status")
        }

        @Test
        fun `updateStatusWithHandle with non-null resultJson still works (regression guard)`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.PROCESSING)
            insertTaskDirect(task)

            val json = """{"output":"success"}"""
            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(handle, task.id, TaskStatus.COMPLETED, json)
            }

            assertTrue(result)
            val row = readTaskDirect(task.id)!!
            assertEquals("COMPLETED", row["STATUS"])
            assertEquals(json, row["RESULT"])
        }

        @Test
        fun `insertBatchWithHandle with null payload and null result does not throw on Oracle CLOB`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)

            val task = makeTask(
                workflowId = wf.id,
                item = null,
                resultJson = null,
            )

            jdbi.useTransaction<Exception> { handle ->
                taskRepo.insertBatchWithHandle(handle, listOf(task))
            }

            val row = readTaskDirect(task.id)!!
            assertEquals(task.id, row["ID"])
            assertNull(row["ITEM"], "item column should be null")
            assertNull(row["RESULT"], "result column should be null")
        }

        // ── R1.2 — DEAD_LETTER status ────────────────────────────────────

        @Test
        fun `deadLetterExhaustedTasks marks exhausted stale tasks as DEAD_LETTER`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val threshold = now().minus(Duration.ofMinutes(10))
            // Exhausted: retryCount >= maxRetries, claimed before threshold
            val exhausted = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "worker-1",
                claimedAt = threshold.minus(Duration.ofMinutes(5)),
                retryCount = 3,
                maxRetries = 3,
            )
            // Not exhausted: has retries remaining
            val retriable = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "worker-2",
                claimedAt = threshold.minus(Duration.ofMinutes(5)),
                retryCount = 1,
                maxRetries = 3,
            )
            // Not stale: claimed recently
            val fresh = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "worker-3",
                claimedAt = now(),
                retryCount = 3,
                maxRetries = 3,
            )
            insertTaskDirect(exhausted)
            insertTaskDirect(retriable)
            insertTaskDirect(fresh)

            val count = taskRepo.deadLetterExhaustedTasks(threshold)

            assertEquals(1, count)
            val row = readTaskDirect(exhausted.id)!!
            assertEquals("DEAD_LETTER", row["STATUS"])
            assertNotNull(row["COMPLETED_AT"])

            // Others unchanged
            assertEquals("PROCESSING", readTaskDirect(retriable.id)!!["STATUS"])
            assertEquals("PROCESSING", readTaskDirect(fresh.id)!!["STATUS"])
        }

        @Test
        fun `DEAD_LETTER is terminal`() {
            assertTrue(TaskStatus.DEAD_LETTER.isTerminal)
        }

        @Test
        fun `updateStatusWithHandle rejects update to DEAD_LETTER task`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER)
            insertTaskDirect(task)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(handle, task.id, TaskStatus.COMPLETED, """{"retry":true}""")
            }

            assertFalse(result, "should return false when task is DEAD_LETTER")
        }

        // ── Zombie guard via (claimed_by, claimed_at) ─────────────────────

        @Test
        fun `terminal update succeeds with matching claimedBy and claimedAt`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val claimedAt = now()
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "pod-A",
                claimedAt = claimedAt,
            )
            insertTaskDirect(task)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(
                    handle, task.id, TaskStatus.COMPLETED, null,
                    claimedBy = "pod-A", claimedAt = claimedAt,
                )
            }

            assertTrue(result, "should succeed with matching claim identity")
            assertEquals("COMPLETED", readTaskDirect(task.id)!!["STATUS"])
        }

        @Test
        fun `terminal update fails when claimedBy does not match (zombie detection)`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val claimedAt = now()
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "pod-B",
                claimedAt = claimedAt,
            )
            insertTaskDirect(task)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(
                    handle, task.id, TaskStatus.COMPLETED, null,
                    claimedBy = "pod-A", claimedAt = claimedAt,
                )
            }

            assertFalse(result, "zombie handler (different pod) should be rejected")
            assertEquals("PROCESSING", readTaskDirect(task.id)!!["STATUS"])
        }

        @Test
        fun `terminal update fails when claimedAt does not match (zombie after reclaim)`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val originalClaimedAt = now().minus(Duration.ofMinutes(10))
            val newClaimedAt = now()
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "pod-A",
                claimedAt = newClaimedAt,
            )
            insertTaskDirect(task)

            // Zombie from original claim attempts to complete with stale claimedAt
            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(
                    handle, task.id, TaskStatus.COMPLETED, null,
                    claimedBy = "pod-A", claimedAt = originalClaimedAt,
                )
            }

            assertFalse(result, "stale claimedAt should be rejected (same pod, reclaimed)")
            assertEquals("PROCESSING", readTaskDirect(task.id)!!["STATUS"])
        }

        @Test
        fun `terminal update with null claimedBy bypasses fence (watchdog path)`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "pod-A",
                claimedAt = now(),
            )
            insertTaskDirect(task)

            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(
                    handle, task.id, TaskStatus.FAILED, null,
                    claimedBy = null, claimedAt = null,
                )
            }

            assertTrue(result, "null claimedBy should bypass fence (watchdog path)")
            assertEquals("FAILED", readTaskDirect(task.id)!!["STATUS"])
        }

        // ── SKIP LOCKED concurrency ─────────────────────────────────────

        @Test
        fun `concurrent claimNext calls produce disjoint task sets via SKIP LOCKED`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            // Insert 5 PENDING tasks for the same workflow/sequence
            val taskIds = (1..5).map { i ->
                val task = makeTask(
                    workflowId = wf.id,
                    sequenceNumber = 1,
                    status = TaskStatus.PENDING,
                    handlerKey = "skip.locked.$i",
                )
                insertTaskDirect(task)
                task.id
            }

            // Launch two concurrent claimNext calls
            val deferred1 = async {
                taskRepo.claimNext("worker-1", 3)
            }
            val deferred2 = async {
                taskRepo.claimNext("worker-2", 3)
            }

            val claimed1 = deferred1.await()
            val claimed2 = deferred2.await()

            // Union of claimed task IDs has no duplicates (disjoint sets)
            val ids1 = claimed1.map { it.id }.toSet()
            val ids2 = claimed2.map { it.id }.toSet()
            val intersection = ids1.intersect(ids2)
            assertTrue(
                intersection.isEmpty(),
                "SKIP LOCKED must prevent overlapping claims, but found shared IDs: $intersection",
            )

            // Total claimed count <= 5 (no phantom reads)
            val totalClaimed = ids1.size + ids2.size
            assertTrue(
                totalClaimed <= 5,
                "Total claimed ($totalClaimed) must not exceed available tasks (5)",
            )

            // All claimed IDs must be from the original set
            val allClaimed = ids1 + ids2
            assertTrue(
                allClaimed.all { it in taskIds },
                "Claimed IDs must be from the original task set",
            )
        }

        // ── not_before claim filtering ────────────────────────────────────

        @Test
        fun `claimNext skips tasks with future not_before`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val futureNotBefore = now().plus(Duration.ofMinutes(5))
            insertTaskDirect(makeTask(
                workflowId = wf.id,
                status = TaskStatus.PENDING,
                handlerKey = "backed-off",
                notBefore = futureNotBefore,
            ))
            insertTaskDirect(makeTask(
                workflowId = wf.id,
                status = TaskStatus.PENDING,
                handlerKey = "ready",
            ))

            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(1, claimed.size)
            assertEquals("ready", claimed[0].handlerKey)
        }

        @Test
        fun `claimNext claims task after not_before has passed`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val pastNotBefore = now().minus(Duration.ofSeconds(1))
            insertTaskDirect(makeTask(
                workflowId = wf.id,
                status = TaskStatus.PENDING,
                handlerKey = "backoff-expired",
                notBefore = pastNotBefore,
            ))

            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(1, claimed.size)
            assertEquals("backoff-expired", claimed[0].handlerKey)
        }

        @Test
        fun `claimNext claims task with null not_before`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(
                workflowId = wf.id,
                status = TaskStatus.PENDING,
                handlerKey = "no-backoff",
                notBefore = null,
            ))

            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(1, claimed.size)
            assertEquals("no-backoff", claimed[0].handlerKey)
        }

        @Test
        fun `claimNext claims task with not_before equal to now`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            // not_before exactly at claim time — should be claimable with <= check
            val notBeforeExact = now().minus(Duration.ofMillis(1))
            insertTaskDirect(makeTask(
                workflowId = wf.id,
                status = TaskStatus.PENDING,
                handlerKey = "boundary-exact",
                notBefore = notBeforeExact,
            ))

            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(1, claimed.size)
            assertEquals("boundary-exact", claimed[0].handlerKey)
        }

        // ── resetForRetry backoff ─────────────────────────────────────────

        /** Read a nullable Oracle timestamp from raw row data for assertions. */
        private fun readNullableTimestampDirect(value: Any?): Instant? = when (value) {
            null -> null
            is java.sql.Timestamp -> value.toLocalDateTime().toInstant(ZoneOffset.UTC)
            else -> {
                // Oracle JDBC returns oracle.sql.TIMESTAMP — use reflection
                val method = value::class.java.getMethod("timestampValue")
                (method.invoke(value) as java.sql.Timestamp).toLocalDateTime().toInstant(ZoneOffset.UTC)
            }
        }

        @Test
        fun `resetForRetry sets not_before with exponential backoff`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "worker-1",
                claimedAt = now(),
                maxRetries = 5,
                retryCount = 0,
            )
            insertTaskDirect(task)

            val beforeReset = Instant.now()
            taskRepo.resetForRetry(task.id, 2, null, null) // retryCount=2, default base=1 → backoff = 1*2^2 = 4s

            val row = readTaskDirect(task.id)!!
            assertEquals("PENDING", row["STATUS"])
            assertNull(row["CLAIMED_BY"])
            assertNull(row["CLAIMED_AT"])
            assertEquals(2, (row["RETRY_COUNT"] as Number).toInt())

            val notBefore = readNullableTimestampDirect(row["NOT_BEFORE"])
            assertNotNull(notBefore, "not_before should be set after resetForRetry")
            // 1*2^2 = 4 seconds backoff, allow 2s tolerance for execution time
            val expectedMin = beforeReset.plusSeconds(2)
            val expectedMax = beforeReset.plusSeconds(6)
            assertTrue(
                notBefore.isAfter(expectedMin) && notBefore.isBefore(expectedMax),
                "not_before ($notBefore) should be ~4s after reset, " +
                    "expected between $expectedMin and $expectedMax",
            )
        }

        @Test
        fun `resetForRetry backoff caps at backoff_cap`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "worker-1",
                claimedAt = now(),
                maxRetries = 15,
                retryCount = 0,
            )
            insertTaskDirect(task)

            val beforeReset = Instant.now()
            taskRepo.resetForRetry(task.id, 10, null, null) // 1*2^10 = 1024, capped to 300

            val row = readTaskDirect(task.id)!!
            val notBefore = readNullableTimestampDirect(row["NOT_BEFORE"])
            assertNotNull(notBefore)
            val maxExpected = beforeReset.plusSeconds(305)
            assertTrue(
                notBefore.isBefore(maxExpected),
                "not_before ($notBefore) should be capped at ~300s, not exceed $maxExpected",
            )
            val minExpected = beforeReset.plusSeconds(295)
            assertTrue(
                notBefore.isAfter(minExpected),
                "not_before ($notBefore) should be at least ~300s ($minExpected)",
            )
        }

        // ── Per-activity backoff config ───────────────────────────────────

        @Test
        fun `resetForRetry uses per-task backoff_base and backoff_cap`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)
            // Custom backoff: base=5s, cap=60s
            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.PROCESSING,
                claimedBy = "worker-1",
                claimedAt = now(),
                maxRetries = 5,
                retryCount = 0,
                backoffBase = 5,
                backoffCap = 60,
            )
            insertTaskDirect(task)

            val beforeReset = Instant.now()
            taskRepo.resetForRetry(task.id, 2, null, null) // 5*2^2 = 20s

            val row = readTaskDirect(task.id)!!
            val notBefore = readNullableTimestampDirect(row["NOT_BEFORE"])
            assertNotNull(notBefore)
            val expectedMin = beforeReset.plusSeconds(18)
            val expectedMax = beforeReset.plusSeconds(22)
            assertTrue(
                notBefore.isAfter(expectedMin) && notBefore.isBefore(expectedMax),
                "not_before ($notBefore) should be ~20s with custom base=5, expected $expectedMin..$expectedMax",
            )
        }

        // ── Dead-letter replay — single ───────────────────────────────────

        @Test
        fun `replayDeadLetterTask resets DEAD_LETTER to PENDING`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.DEAD_LETTER,
                claimedBy = "old-worker",
                claimedAt = now().minus(Duration.ofHours(1)),
                completedAt = now().minus(Duration.ofMinutes(30)),
                resultJson = """{"error":"timeout"}""",
                retryCount = 3,
                maxRetries = 3,
                notBefore = now().plus(Duration.ofMinutes(5)),
            )
            insertTaskDirect(task)

            val result = taskRepo.replayDeadLetterTask(task.id)

            assertTrue(result)
            val row = readTaskDirect(task.id)!!
            assertEquals("PENDING", row["STATUS"])
            assertEquals(0, (row["RETRY_COUNT"] as Number).toInt())
            assertNull(row["CLAIMED_BY"])
            assertNull(row["CLAIMED_AT"])
            assertNull(row["COMPLETED_AT"])
            assertNull(row["RESULT"])
            assertNull(row["NOT_BEFORE"])
        }

        @Test
        fun `replayDeadLetterTask returns false for non-DEAD_LETTER task`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(workflowId = wf.id, status = TaskStatus.PENDING)
            insertTaskDirect(task)

            val result = taskRepo.replayDeadLetterTask(task.id)

            assertFalse(result)
        }

        @Test
        fun `replayDeadLetterTask returns false for non-existent task`() = runTest {
            val result = taskRepo.replayDeadLetterTask(randomId())
            assertFalse(result)
        }

        @Test
        fun `replayed task is claimable by workers`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(
                workflowId = wf.id,
                status = TaskStatus.DEAD_LETTER,
                retryCount = 3,
                maxRetries = 3,
            )
            insertTaskDirect(task)

            taskRepo.replayDeadLetterTask(task.id)
            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(1, claimed.size)
            assertEquals(task.id, claimed[0].id)
            assertEquals(TaskStatus.PROCESSING, claimed[0].status)
        }

        // ── Dead-letter replay — batch ────────────────────────────────────

        @Test
        fun `replayDeadLetterBatch replays all DEAD_LETTER tasks for workflow`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val dl1 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
            val dl2 = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
            val completed = makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED)
            insertTaskDirect(dl1)
            insertTaskDirect(dl2)
            insertTaskDirect(completed)

            val count = taskRepo.replayDeadLetterBatch(wf.id)

            assertEquals(2, count)
            assertEquals("PENDING", readTaskDirect(dl1.id)!!["STATUS"])
            assertEquals("PENDING", readTaskDirect(dl2.id)!!["STATUS"])
            assertEquals("COMPLETED", readTaskDirect(completed.id)!!["STATUS"])
        }

        @Test
        fun `replayDeadLetterBatch returns 0 when no DEAD_LETTER tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            insertTaskDirect(makeTask(workflowId = wf.id, status = TaskStatus.COMPLETED))

            val count = taskRepo.replayDeadLetterBatch(wf.id)
            assertEquals(0, count)
        }

        @Test
        fun `replayDeadLetterBatch scoped to workflow`() = runTest {
            val wf1 = makeWorkflow()
            val wf2 = makeWorkflow()
            workflowRepo.insert(wf1)
            workflowRepo.insert(wf2)

            val task1 = makeTask(workflowId = wf1.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
            val task2 = makeTask(workflowId = wf2.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
            insertTaskDirect(task1)
            insertTaskDirect(task2)

            val count = taskRepo.replayDeadLetterBatch(wf1.id)

            assertEquals(1, count)
            assertEquals("PENDING", readTaskDirect(task1.id)!!["STATUS"])
            assertEquals("DEAD_LETTER", readTaskDirect(task2.id)!!["STATUS"])
        }

        @Test
        fun `replayDeadLetterBatchWithHandle works within transaction`() {
            val wf = makeWorkflow()
            insertWorkflowDirect(wf)
            val task = makeTask(workflowId = wf.id, status = TaskStatus.DEAD_LETTER, retryCount = 3, maxRetries = 3)
            insertTaskDirect(task)

            val count = jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.replayDeadLetterBatchWithHandle(handle, wf.id)
            }

            assertEquals(1, count)
            assertEquals("PENDING", readTaskDirect(task.id)!!["STATUS"])
        }

        // ── R2.1 — FIFO ordering via enqueued_at ─────────────────────────

        /** Read DB server time for SYSTIMESTAMP assertions. */
        private fun readDbServerTime(): Instant {
            return jdbi.withHandle<Instant, Exception> { handle ->
                val raw = handle.createQuery("SELECT CAST(SYSTIMESTAMP AS TIMESTAMP) FROM DUAL")
                    .mapTo(java.sql.Timestamp::class.java)
                    .one()
                raw.toLocalDateTime().toInstant(ZoneOffset.UTC)
            }
        }

        @Test
        fun `claimNext returns tasks in enqueued_at ascending order`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val baseTime = Instant.now().truncatedTo(ChronoUnit.SECONDS)
            val t1 = makeTask(workflowId = wf.id, handlerKey = "oldest")
            val t2 = makeTask(workflowId = wf.id, handlerKey = "middle")
            val t3 = makeTask(workflowId = wf.id, handlerKey = "newest")

            // Insert with explicit enqueued_at to control FIFO order
            insertTaskDirect(t2, enqueuedAt = baseTime.plusSeconds(10))
            insertTaskDirect(t3, enqueuedAt = baseTime.plusSeconds(20))
            insertTaskDirect(t1, enqueuedAt = baseTime.plusSeconds(0))

            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(3, claimed.size)
            assertEquals("oldest", claimed[0].handlerKey)
            assertEquals("middle", claimed[1].handlerKey)
            assertEquals("newest", claimed[2].handlerKey)
        }

        @Test
        fun `claimNext FIFO ordering breaks ties by id`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val sameTime = Instant.now().truncatedTo(ChronoUnit.SECONDS)
            val t1 = makeTask(workflowId = wf.id, handlerKey = "a")
            val t2 = makeTask(workflowId = wf.id, handlerKey = "b")

            // Same enqueued_at — tiebreaker is id ASC
            insertTaskDirect(t1, enqueuedAt = sameTime)
            insertTaskDirect(t2, enqueuedAt = sameTime)

            val claimed = taskRepo.claimNext("worker-1", 10)

            assertEquals(2, claimed.size)
            val expectedOrder = listOf(t1.id, t2.id).sorted()
            assertEquals(expectedOrder, claimed.map { it.id })
        }

        @Test
        fun `enqueuedAt is populated after insert and read-back`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val beforeInsert = Instant.now().minusSeconds(2)
            val task = makeTask(workflowId = wf.id)
            insertTaskDirect(task) // enqueued_at filled by Oracle DEFAULT SYSTIMESTAMP

            val found = taskRepo.findByWorkflowAndSequence(wf.id, 1)

            assertEquals(1, found.size)
            val enqueuedAt = found[0].enqueuedAt
            assertTrue(
                enqueuedAt != Instant.EPOCH,
                "enqueuedAt should not be EPOCH sentinel after DB round-trip"
            )
            assertTrue(
                enqueuedAt.isAfter(beforeInsert),
                "enqueuedAt ($enqueuedAt) should be after test start ($beforeInsert)"
            )
        }

        @Test
        fun `insertBatch populates enqueuedAt via DB default`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val beforeInsert = Instant.now().minusSeconds(2)
            val task = makeTask(workflowId = wf.id)
            taskRepo.insertBatch(listOf(task))

            val found = taskRepo.findByWorkflowAndSequence(wf.id, 1)

            assertEquals(1, found.size)
            assertTrue(
                found[0].enqueuedAt != Instant.EPOCH,
                "enqueuedAt should be DB-assigned, not EPOCH sentinel"
            )
            assertTrue(
                found[0].enqueuedAt.isAfter(beforeInsert),
                "enqueuedAt should be recent, not stale"
            )
        }

        // ── R2.3 — SYSTIMESTAMP for claimed_at ───────────────────────────

        @Test
        fun `claimNext sets claimed_at from DB SYSTIMESTAMP not JVM clock`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(workflowId = wf.id, status = TaskStatus.PENDING)
            insertTaskDirect(task)

            val dbTimeBefore = readDbServerTime()
            val claimed = taskRepo.claimNext("worker-1", 1)
            val dbTimeAfter = readDbServerTime()

            assertEquals(1, claimed.size)
            val claimedAt = claimed[0].claimedAt
            assertNotNull(claimedAt)
            assertTrue(
                !claimedAt.isBefore(dbTimeBefore.minusSeconds(1)) && !claimedAt.isAfter(dbTimeAfter.plusSeconds(1)),
                "claimedAt ($claimedAt) should be within DB server time window [$dbTimeBefore, $dbTimeAfter]"
            )
        }

        @Test
        fun `claimNext re-read returns exact DB-assigned claimedAt for fencing`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(workflowId = wf.id, status = TaskStatus.PENDING)
            insertTaskDirect(task)

            val claimed = taskRepo.claimNext("worker-1", 1)
            assertEquals(1, claimed.size)

            // The returned claimedAt should match the DB row exactly
            val row = readTaskDirect(task.id)!!
            val dbClaimedAt = readNullableTimestampDirect(row["CLAIMED_AT"])
            assertNotNull(dbClaimedAt)

            // Compare with millisecond tolerance (Oracle TIMESTAMP precision)
            val diff = java.time.Duration.between(claimed[0].claimedAt, dbClaimedAt).abs()
            assertTrue(
                diff.toMillis() < 1000,
                "Returned claimedAt (${claimed[0].claimedAt}) must match DB row ($dbClaimedAt), diff=${diff.toMillis()}ms"
            )
        }

        @Test
        fun `fencing works with DB-assigned claimedAt from claimNext`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(workflowId = wf.id, status = TaskStatus.PENDING)
            insertTaskDirect(task)

            val claimed = taskRepo.claimNext("worker-1", 1)
            assertEquals(1, claimed.size)
            val claimedTask = claimed[0]

            // Use the returned claimedBy/claimedAt for fencing — should succeed
            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(
                    handle, claimedTask.id, TaskStatus.COMPLETED, """{"ok":true}""",
                    claimedBy = claimedTask.claimedBy, claimedAt = claimedTask.claimedAt,
                )
            }

            assertTrue(result, "fencing with DB-assigned claimedAt should succeed")
            assertEquals("COMPLETED", readTaskDirect(task.id)!!["STATUS"])
        }

        @Test
        fun `zombie rejected when claimedAt does not match DB-assigned value`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val task = makeTask(workflowId = wf.id, status = TaskStatus.PENDING)
            insertTaskDirect(task)

            val claimed = taskRepo.claimNext("worker-1", 1)
            assertEquals(1, claimed.size)

            // Simulate zombie with stale claimedAt
            val staleClaimedAt = Instant.now().minusSeconds(3600)
            val result = jdbi.inTransaction<Boolean, Exception> { handle ->
                taskRepo.updateStatusWithHandle(
                    handle, claimed[0].id, TaskStatus.COMPLETED, null,
                    claimedBy = "worker-1", claimedAt = staleClaimedAt,
                )
            }

            assertFalse(result, "zombie with wrong claimedAt should be rejected")
            assertEquals("PROCESSING", readTaskDirect(task.id)!!["STATUS"])
        }

        // ── R2.2 — Index changes (functional regression) ────────────────

        @Test
        fun `claim and reaper queries work correctly with bulk data`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val baseTime = Instant.now().truncatedTo(ChronoUnit.SECONDS)

            // Insert 100 PENDING tasks with spread enqueued_at
            val taskIds = (1..100).map { i ->
                val t = makeTask(workflowId = wf.id, status = TaskStatus.PENDING, handlerKey = "bulk.$i")
                insertTaskDirect(t, enqueuedAt = baseTime.plusSeconds(i.toLong()))
                t.id
            }

            // Claim first batch — should be FIFO ordered
            val batch1 = taskRepo.claimNext("worker-1", 10)
            assertEquals(10, batch1.size)
            assertEquals("bulk.1", batch1[0].handlerKey)
            assertEquals("bulk.10", batch1[9].handlerKey)

            // Claim second batch — continues FIFO
            val batch2 = taskRepo.claimNext("worker-2", 10)
            assertEquals(10, batch2.size)
            assertEquals("bulk.11", batch2[0].handlerKey)
            assertEquals("bulk.20", batch2[9].handlerKey)

            // No overlap between batches
            val ids1 = batch1.map { it.id }.toSet()
            val ids2 = batch2.map { it.id }.toSet()
            assertTrue(ids1.intersect(ids2).isEmpty(), "No overlap between claim batches")
        }

        // ── countStatusSummariesByWorkflowWithHandle ────────────────────

        @Test
        fun `countStatusSummariesByWorkflow returns counts grouped by sequence`() = runTest {
            val wfId = randomId()
            workflowRepo.insert(makeWorkflow(id = wfId))

            val tasks = listOf(
                makeTask(workflowId = wfId, sequenceNumber = 1, status = TaskStatus.COMPLETED),
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.PENDING),
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.COMPLETED),
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.FAILED),
            )
            taskRepo.insertBatch(tasks)

            val result = jdbi.withHandle<Map<Int, TaskStatusCounts>, Exception> { h ->
                taskRepo.countStatusSummariesByWorkflowWithHandle(h, wfId)
            }

            val seq1 = result[1]!!
            assertEquals(1, seq1.total)
            assertEquals(1, seq1.completed)
            assertEquals(0, seq1.nonTerminal)
            assertEquals(0, seq1.failed)

            val seq2 = result[2]!!
            assertEquals(3, seq2.total)
            assertEquals(1, seq2.completed)
            assertEquals(1, seq2.nonTerminal)
            assertEquals(1, seq2.failed)
        }

        // ── cancelPendingTasksWithHandle ────────────────────────────────

        @Test
        fun `cancelPendingTasksWithHandle cancels PENDING and DEFERRED tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            // Insert a PENDING task
            val pendingTask = makeTask(workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PENDING)
            insertTaskDirect(pendingTask)

            // Insert a PROCESSING task, then defer it to get a real DEFERRED task
            val toDefer = makeTask(
                workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.PROCESSING,
                claimedBy = "worker-1", claimedAt = now(),
            )
            insertTaskDirect(toDefer)
            val deferred = taskRepo.defer(toDefer.id, "sql-exec", """{"datasource":"test","sql":"SELECT 1"}""")
            assertTrue(deferred, "defer should succeed for PROCESSING task")

            // Verify the task is now DEFERRED
            assertEquals("DEFERRED", readTaskDirect(toDefer.id)!!["STATUS"])

            // Cancel all pending/deferred tasks
            val cancelled = jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.cancelPendingTasksWithHandle(handle, wf.id)
            }

            assertEquals(2, cancelled)

            // Verify both are CANCELLED
            assertEquals("CANCELLED", readTaskDirect(pendingTask.id)!!["STATUS"])
            assertEquals("CANCELLED", readTaskDirect(toDefer.id)!!["STATUS"])
        }

        @Test
        fun `cancelPendingTasksWithHandle does not cancel PROCESSING or terminal tasks`() = runTest {
            val wf = makeWorkflow()
            workflowRepo.insert(wf)

            val processingTask = makeTask(
                workflowId = wf.id, sequenceNumber = 1, status = TaskStatus.PROCESSING,
                claimedBy = "worker-1", claimedAt = now(),
            )
            val completedTask = makeTask(
                workflowId = wf.id, sequenceNumber = 2, status = TaskStatus.COMPLETED,
                completedAt = now(),
            )
            insertTaskDirect(processingTask)
            insertTaskDirect(completedTask)

            val cancelled = jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.cancelPendingTasksWithHandle(handle, wf.id)
            }

            assertEquals(0, cancelled)
            assertEquals("PROCESSING", readTaskDirect(processingTask.id)!!["STATUS"])
            assertEquals("COMPLETED", readTaskDirect(completedTask.id)!!["STATUS"])
        }

        // ── findByWorkflowIdWithHandle ──────────────────────────────────

        @Test
        fun `findByWorkflowIdWithHandle returns all tasks for a workflow`() = runTest {
            val wfId = randomId()
            val otherWfId = randomId()
            workflowRepo.insert(makeWorkflow(id = wfId))
            workflowRepo.insert(makeWorkflow(id = otherWfId))

            val tasks = listOf(
                makeTask(workflowId = wfId, sequenceNumber = 1, status = TaskStatus.COMPLETED),
                makeTask(workflowId = wfId, sequenceNumber = 2, status = TaskStatus.PENDING),
                makeTask(workflowId = otherWfId, sequenceNumber = 1, status = TaskStatus.PENDING),
            )
            taskRepo.insertBatch(tasks)

            val result = jdbi.withHandle<List<Task>, Exception> { h ->
                taskRepo.findByWorkflowIdWithHandle(h, wfId)
            }

            assertEquals(2, result.size)
            assertTrue(result.all { it.workflowId == wfId })
            assertEquals(setOf(1, 2), result.map { it.sequenceNumber }.toSet())
        }
    }
}
