package com.workflow.workflow.adapter.persistent

import com.workflow.infrastructure.persistence.DB_ZONE
import com.workflow.infrastructure.persistence.OracleTestContainer

import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.LocalDateTime
import java.util.TreeMap
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaTest {

    private lateinit var jdbi: Jdbi

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
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

    private fun now(): Instant = Instant.now()

    private fun caseInsensitiveMap(map: Map<String, Any?>): Map<String, Any?> =
        TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER).apply { putAll(map) }

    private fun insertWorkflow(
        id: String = randomId(),
        definition: String = """{"activities":{}}""",
        version: Int? = null,
        status: String = "RUNNING",
        createdAt: Instant = now(),
        updatedAt: Instant = now(),
    ): String {
        val createdAtLdt = LocalDateTime.ofInstant(createdAt, DB_ZONE)
        val updatedAtLdt = LocalDateTime.ofInstant(updatedAt, DB_ZONE)
        val deadlineAtLdt = createdAtLdt.plusHours(1)
        jdbi.useHandle<Exception> { handle ->
            if (version != null) {
                handle.createUpdate(
                    """INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at)
                       VALUES (:id, :definition, :version, :status, :createdAt, :updatedAt, :deadlineAt)"""
                )
                    .bind("id", id)
                    .bind("definition", definition)
                    .bind("version", version)
                    .bind("status", status)
                    .bind("createdAt", createdAtLdt)
                    .bind("updatedAt", updatedAtLdt)
                    .bind("deadlineAt", deadlineAtLdt)
                    .execute()
            } else {
                handle.createUpdate(
                    """INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at)
                       VALUES (:id, :definition, :status, :createdAt, :updatedAt, :deadlineAt)"""
                )
                    .bind("id", id)
                    .bind("definition", definition)
                    .bind("status", status)
                    .bind("createdAt", createdAtLdt)
                    .bind("updatedAt", updatedAtLdt)
                    .bind("deadlineAt", deadlineAtLdt)
                    .execute()
            }
        }
        return id
    }

    private fun insertTask(
        id: String = randomId(),
        workflowId: String,
        sequenceNumber: Int = 1,
        status: String = "PENDING",
        handlerKey: String = "test.handler",
        taskPayload: String? = null,
        result: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        completedAt: Instant? = null,
        retryCount: Int? = null,
        maxRetries: Int? = null,
        deadlineAt: Instant? = null,
    ): String {
        jdbi.useHandle<Exception> { handle ->
            val columns = mutableListOf(
                "id", "workflow_id", "sequence_number", "status", "handler_key"
            )
            val values = mutableListOf(
                ":id", ":workflowId", ":sequenceNumber", ":status", ":handlerKey"
            )
            val bindings = mutableMapOf<String, Any?>(
                "id" to id,
                "workflowId" to workflowId,
                "sequenceNumber" to sequenceNumber,
                "status" to status,
                "handlerKey" to handlerKey,
            )

            if (taskPayload != null) {
                columns.add("task_payload"); values.add(":taskPayload"); bindings["taskPayload"] = taskPayload
            }
            if (result != null) {
                columns.add("result"); values.add(":result"); bindings["result"] = result
            }
            if (claimedBy != null) {
                columns.add("claimed_by"); values.add(":claimedBy"); bindings["claimedBy"] = claimedBy
            }
            if (claimedAt != null) {
                columns.add("claimed_at"); values.add(":claimedAt")
                bindings["claimedAt"] = LocalDateTime.ofInstant(claimedAt, DB_ZONE)
            }
            if (completedAt != null) {
                columns.add("completed_at"); values.add(":completedAt")
                bindings["completedAt"] = LocalDateTime.ofInstant(completedAt, DB_ZONE)
            }
            if (retryCount != null) {
                columns.add("retry_count"); values.add(":retryCount"); bindings["retryCount"] = retryCount
            }
            if (maxRetries != null) {
                columns.add("max_retries"); values.add(":maxRetries"); bindings["maxRetries"] = maxRetries
            }
            if (deadlineAt != null) {
                columns.add("deadline_at"); values.add(":deadlineAt")
                bindings["deadlineAt"] = LocalDateTime.ofInstant(deadlineAt, DB_ZONE)
            }

            val sql = "INSERT INTO task (${columns.joinToString()}) VALUES (${values.joinToString()})"
            val update = handle.createUpdate(sql)
            bindings.forEach { (key, value) -> update.bind(key, value) }
            update.execute()
        }
        return id
    }

    // ── Test 1: Migration applies successfully ──────────────────────────

    @Test
    fun migrationAppliesSuccessfully() {
        jdbi.useHandle<Exception> { handle ->
            val tables = handle.createQuery(
                "SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME IN ('WORKFLOW', 'TASK')"
            ).mapTo(String::class.java).list()

            assertEquals(2, tables.size, "Expected both WORKFLOW and TASK tables to exist")
            assert(tables.contains("WORKFLOW")) { "WORKFLOW table missing" }
            assert(tables.contains("TASK")) { "TASK table missing" }
        }
    }

    // ── Test 2: Workflow insert and read round-trip ──────────────────────

    @Test
    fun workflowInsertAndReadRoundTrip() {
        val id = randomId()
        val definition = """{"activities":[{"name":"step1"}]}"""
        val ts = now()
        insertWorkflow(
            id = id,
            definition = definition,
            version = 5,
            status = "COMPLETED",
            createdAt = ts,
            updatedAt = ts,
        )

        jdbi.useHandle<Exception> { handle ->
            val raw = handle.createQuery("SELECT * FROM workflow WHERE id = :id")
                .bind("id", id)
                .mapToMap()
                .findOne()
                .orElse(null)

            assertNotNull(raw, "Workflow row should exist")
            val row = caseInsensitiveMap(raw)
            assertEquals(id, row["ID"])
            val defValue = row["DEFINITION"]
            val defStr = when (defValue) {
                is java.sql.Clob -> defValue.characterStream.readText()
                else -> defValue.toString()
            }
            assertEquals(definition, defStr)
            assertEquals(5, (row["VERSION"] as Number).toInt())
            assertEquals("COMPLETED", row["STATUS"])
            assertNotNull(row["CREATED_AT"])
            assertNotNull(row["UPDATED_AT"])
        }
    }

    // ── Test 3: Task insert and read round-trip ─────────────────────────

    @Test
    fun taskInsertAndReadRoundTrip() {
        val wfId = insertWorkflow()
        val taskId = randomId()
        val ts = now()

        insertTask(
            id = taskId,
            workflowId = wfId,
            sequenceNumber = 2,
            status = "PROCESSING",
            handlerKey = "order.process",
            taskPayload = """{"orderId":123}""",
            result = """{"status":"ok"}""",
            claimedBy = "worker-1",
            claimedAt = ts,
            completedAt = ts,
            retryCount = 1,
            maxRetries = 3,
            deadlineAt = ts,
        )

        jdbi.useHandle<Exception> { handle ->
            val raw = handle.createQuery("SELECT * FROM task WHERE id = :id")
                .bind("id", taskId)
                .mapToMap()
                .findOne()
                .orElse(null)

            assertNotNull(raw, "Task row should exist")
            val row = caseInsensitiveMap(raw)
            assertEquals(taskId, row["ID"])
            assertEquals(wfId, row["WORKFLOW_ID"])
            assertEquals(2, (row["SEQUENCE_NUMBER"] as Number).toInt())
            assertEquals("PROCESSING", row["STATUS"])
            assertEquals("order.process", row["HANDLER_KEY"])

            val itemVal = row["TASK_PAYLOAD"]
            val itemStr = when (itemVal) {
                is java.sql.Clob -> itemVal.characterStream.readText()
                else -> itemVal.toString()
            }
            assertEquals("""{"orderId":123}""", itemStr)

            val resultVal = row["RESULT"]
            val resultStr = when (resultVal) {
                is java.sql.Clob -> resultVal.characterStream.readText()
                else -> resultVal.toString()
            }
            assertEquals("""{"status":"ok"}""", resultStr)

            assertEquals("worker-1", row["CLAIMED_BY"])
            assertNotNull(row["CLAIMED_AT"])
            assertNotNull(row["COMPLETED_AT"])
            assertEquals(1, (row["RETRY_COUNT"] as Number).toInt())
            assertEquals(3, (row["MAX_RETRIES"] as Number).toInt())
            assertNotNull(row["DEADLINE_AT"])
        }
    }

    // ── Test 4: Workflow primary key enforced ────────────────────────────

    @Test
    fun workflowPrimaryKeyEnforced() {
        val id = insertWorkflow()

        assertThrows<UnableToExecuteStatementException> {
            insertWorkflow(id = id)
        }
    }

    // ── Test 5: Task primary key enforced ────────────────────────────────

    @Test
    fun taskPrimaryKeyEnforced() {
        val wfId = insertWorkflow()
        val taskId = randomId()
        insertTask(id = taskId, workflowId = wfId)

        assertThrows<UnableToExecuteStatementException> {
            insertTask(id = taskId, workflowId = wfId)
        }
    }

    // ── Test 6: Task foreign key enforced ────────────────────────────────

    @Test
    fun taskForeignKeyEnforced() {
        val nonExistentWfId = randomId()

        assertThrows<UnableToExecuteStatementException> {
            insertTask(workflowId = nonExistentWfId)
        }
    }

    // ── Test 6b: FK no-cascade — deleting workflow with tasks is rejected ─

    @Test
    fun workflowDeletionWithTasksRejected() {
        val wfId = insertWorkflow()
        insertTask(workflowId = wfId)

        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.execute("DELETE FROM workflow WHERE id = ?", wfId)
            }
        }
    }

    // ── Test 7: Workflow NOT NULL constraints ────────────────────────────

    @Test
    fun workflowNotNullConstraints() {
        val ts = LocalDateTime.ofInstant(now(), DB_ZONE)
        val dl = ts.plusHours(1)

        // null id
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at) VALUES (NULL, 'def', 'RUNNING', :ts, :ts, :dl)"
                ).bind("ts", ts).bind("dl", dl).execute()
            }
        }

        // null definition
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at) VALUES (:id, NULL, 'RUNNING', :ts, :ts, :dl)"
                ).bind("id", randomId()).bind("ts", ts).bind("dl", dl).execute()
            }
        }

        // null status
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at) VALUES (:id, 'def', NULL, :ts, :ts, :dl)"
                ).bind("id", randomId()).bind("ts", ts).bind("dl", dl).execute()
            }
        }

        // null created_at
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at) VALUES (:id, 'def', 'RUNNING', NULL, :ts, :dl)"
                ).bind("id", randomId()).bind("ts", ts).bind("dl", dl).execute()
            }
        }

        // null updated_at
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO workflow (id, definition, status, created_at, updated_at, deadline_at) VALUES (:id, 'def', 'RUNNING', :ts, NULL, :dl)"
                ).bind("id", randomId()).bind("ts", ts).bind("dl", dl).execute()
            }
        }

        // explicit null version (distinct from DEFAULT — verifies NOT NULL constraint)
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at) VALUES (:id, 'def', NULL, 'RUNNING', :ts, :ts, :dl)"
                ).bind("id", randomId()).bind("ts", ts).bind("dl", dl).execute()
            }
        }
    }

    // ── Test 8: Task NOT NULL constraints ────────────────────────────────

    @Test
    fun taskNotNullConstraints() {
        val wfId = insertWorkflow()

        // null id
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO task (id, workflow_id, sequence_number, status, handler_key) VALUES (NULL, :wfId, 1, 'PENDING', 'h')"
                ).bind("wfId", wfId).execute()
            }
        }

        // null workflow_id
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO task (id, workflow_id, sequence_number, status, handler_key) VALUES (:id, NULL, 1, 'PENDING', 'h')"
                ).bind("id", randomId()).execute()
            }
        }

        // null sequence_number
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO task (id, workflow_id, sequence_number, status, handler_key) VALUES (:id, :wfId, NULL, 'PENDING', 'h')"
                ).bind("id", randomId()).bind("wfId", wfId).execute()
            }
        }

        // null status
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO task (id, workflow_id, sequence_number, status, handler_key) VALUES (:id, :wfId, 1, NULL, 'h')"
                ).bind("id", randomId()).bind("wfId", wfId).execute()
            }
        }

        // null handler_key
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO task (id, workflow_id, sequence_number, status, handler_key) VALUES (:id, :wfId, 1, 'PENDING', NULL)"
                ).bind("id", randomId()).bind("wfId", wfId).execute()
            }
        }
    }

    // ── Test 9: Workflow version defaults to zero ────────────────────────

    @Test
    fun workflowVersionDefaultsToZero() {
        val id = insertWorkflow()

        jdbi.useHandle<Exception> { handle ->
            val version = handle.createQuery("SELECT version FROM workflow WHERE id = :id")
                .bind("id", id)
                .mapTo(Int::class.java)
                .one()
            assertEquals(0, version)
        }
    }

    // ── Test 10: Task retry_count defaults to zero ──────────────────────

    @Test
    fun taskRetryCountDefaultsToZero() {
        val wfId = insertWorkflow()
        val taskId = insertTask(workflowId = wfId)

        jdbi.useHandle<Exception> { handle ->
            val retryCount = handle.createQuery("SELECT retry_count FROM task WHERE id = :id")
                .bind("id", taskId)
                .mapTo(Int::class.java)
                .one()
            assertEquals(0, retryCount)
        }
    }

    // ── Test 11: Task max_retries defaults to zero ──────────────────────

    @Test
    fun taskMaxRetriesDefaultsToZero() {
        val wfId = insertWorkflow()
        val taskId = insertTask(workflowId = wfId)

        jdbi.useHandle<Exception> { handle ->
            val maxRetries = handle.createQuery("SELECT max_retries FROM task WHERE id = :id")
                .bind("id", taskId)
                .mapTo(Int::class.java)
                .one()
            assertEquals(0, maxRetries)
        }
    }

    // ── Test 12: Workflow status check constraint ────────────────────────

    @Test
    fun workflowStatusCheckConstraint() {
        assertThrows<UnableToExecuteStatementException> {
            insertWorkflow(status = "INVALID_STATUS")
        }
    }

    // ── Test 13: Task status check constraint ────────────────────────────

    @Test
    fun taskStatusCheckConstraint() {
        val wfId = insertWorkflow()

        assertThrows<UnableToExecuteStatementException> {
            insertTask(workflowId = wfId, status = "INVALID_STATUS")
        }
    }

    // ── Test 14: Workflow CLOB accepts large JSON ────────────────────────

    @Test
    fun workflowClobAcceptsLargeJson() {
        val largeEntries = (1..500).joinToString(",") { i ->
            """{"name":"activity_$i","transition":"handler.$i","retries":3}"""
        }
        val largeJson = """{"activities":[$largeEntries]}"""
        assert(largeJson.length > 10_000) { "Test data should be > 10KB, was ${largeJson.length} bytes" }

        val id = insertWorkflow(definition = largeJson)

        jdbi.useHandle<Exception> { handle ->
            val row = caseInsensitiveMap(
                handle.createQuery("SELECT definition FROM workflow WHERE id = :id")
                    .bind("id", id)
                    .mapToMap()
                    .one()
            )

            val defValue = row["DEFINITION"]
            val defStr = when (defValue) {
                is java.sql.Clob -> defValue.characterStream.readText()
                else -> defValue.toString()
            }
            assertEquals(largeJson, defStr)
        }
    }

    // ── Test 15: Task task_payload and result CLOB ────────────────────

    @Test
    fun taskItemAndResultClob() {
        val wfId = insertWorkflow()

        val largeItems = (1..500).joinToString(",") { i ->
            """{"item_$i":"${"x".repeat(20)}"}"""
        }
        val largeItem = """{"items":[$largeItems]}"""
        val largeResult = """{"results":[$largeItems]}"""
        assert(largeItem.length > 10_000) { "Item should be > 10KB, was ${largeItem.length} bytes" }
        assert(largeResult.length > 10_000) { "Result should be > 10KB, was ${largeResult.length} bytes" }

        val taskId = insertTask(
            workflowId = wfId,
            taskPayload = largeItem,
            result = largeResult,
        )

        jdbi.useHandle<Exception> { handle ->
            val row = caseInsensitiveMap(
                handle.createQuery("SELECT task_payload, result FROM task WHERE id = :id")
                    .bind("id", taskId)
                    .mapToMap()
                    .one()
            )

            val itemVal = row["TASK_PAYLOAD"]
            val itemStr = when (itemVal) {
                is java.sql.Clob -> itemVal.characterStream.readText()
                else -> itemVal.toString()
            }
            assertEquals(largeItem, itemStr)

            val resultVal = row["RESULT"]
            val resultStr = when (resultVal) {
                is java.sql.Clob -> resultVal.characterStream.readText()
                else -> resultVal.toString()
            }
            assertEquals(largeResult, resultStr)
        }
    }

    // ── Test 16: activity_name column exists on task ────────────────────

    @Test
    fun activityNameColumnExistsOnTask() {
        jdbi.useHandle<Exception> { handle ->
            val cols = handle.createQuery(
                "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = 'TASK' AND COLUMN_NAME = 'ACTIVITY_NAME'"
            ).mapTo(String::class.java).list()
            assertEquals(1, cols.size, "Expected ACTIVITY_NAME column on TASK table")
        }
    }

    // ── Test 17: current_sequence column removed from workflow ──────────

    @Test
    fun currentSequenceColumnAbsentFromWorkflow() {
        jdbi.useHandle<Exception> { handle ->
            val cols = handle.createQuery(
                "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = 'WORKFLOW' AND COLUMN_NAME = 'CURRENT_SEQUENCE'"
            ).mapTo(String::class.java).list()
            assertEquals(0, cols.size, "Expected CURRENT_SEQUENCE column to be absent from WORKFLOW table")
        }
    }

    // ── Test 18: SKIPPED is a valid task status ─────────────────────────

    @Test
    fun skippedStatusAccepted() {
        val wfId = insertWorkflow()
        val taskId = insertTask(workflowId = wfId, status = "SKIPPED")
        jdbi.useHandle<Exception> { handle ->
            val status = handle.createQuery("SELECT status FROM task WHERE id = :id")
                .bind("id", taskId)
                .mapTo(String::class.java)
                .one()
            assertEquals("SKIPPED", status)
        }
    }
}
