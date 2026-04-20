package com.workflow.infrastructure.persistence

import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.sql.Types
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.TreeMap
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DispatchSchemaTest {

    private lateinit var jdbi: Jdbi

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
    }

    @AfterEach
    fun cleanTables() {
        jdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM dispatch_event")
            handle.execute("DELETE FROM dispatch_batch")
            handle.execute("DELETE FROM dispatch_event_stg")
            handle.execute("DELETE FROM dispatch_batch_stg")
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun caseInsensitiveMap(map: Map<String, Any?>): Map<String, Any?> =
        TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER).apply { putAll(map) }

    private fun randomToken(): String = UUID.randomUUID().toString().take(64)

    private fun now(): LocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)

    private fun insertBatch(
        batchToken: String = randomToken(),
        status: String = "NORMAL",
        createdAt: LocalDateTime = now(),
        configCount: Int? = null,
        table: String = "dispatch_batch",
    ): String {
        jdbi.useHandle<Exception> { handle ->
            val update = handle.createUpdate(
                "INSERT INTO $table (batch_token, status, created_at, config_count) VALUES (:token, :status, :createdAt, :configCount)"
            )
                .bind("token", batchToken)
                .bind("status", status)
                .bind("createdAt", createdAt)
            if (configCount != null) update.bind("configCount", configCount)
            else update.bindNull("configCount", Types.INTEGER)
            update.execute()
        }
        return batchToken
    }

    private fun insertEvent(
        batchToken: String,
        configId: String = "cfg-1",
        dispatchOrder: Int = 1,
        productId: String = "prod-1",
        sourceBomId: String = "bom-1",
        qty: Int = 100,
        targetSiteId: String = "site-1",
        targetBomId: String? = null,
        siteGap: Int = 5,
        bomGap: Int? = null,
        table: String = "dispatch_event",
    ) {
        jdbi.useHandle<Exception> { handle ->
            val update = handle.createUpdate(
                """INSERT INTO $table
                   (batch_token, config_id, dispatch_order, product_id,
                    source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                   VALUES (:batchToken, :configId, :dispatchOrder, :productId,
                    :sourceBomId, :qty, :targetSiteId, :targetBomId, :siteGap, :bomGap)"""
            )
                .bind("batchToken", batchToken)
                .bind("configId", configId)
                .bind("dispatchOrder", dispatchOrder)
                .bind("productId", productId)
                .bind("sourceBomId", sourceBomId)
                .bind("qty", qty)
                .bind("targetSiteId", targetSiteId)
                .bind("siteGap", siteGap)

            if (targetBomId != null) update.bind("targetBomId", targetBomId)
            else update.bindNull("targetBomId", Types.VARCHAR)
            if (bomGap != null) update.bind("bomGap", bomGap)
            else update.bindNull("bomGap", Types.INTEGER)

            update.execute()
        }
    }

    // ── Test 1: All four tables exist ───────────────────────────────────

    @Test
    fun allTablesExist() {
        jdbi.useHandle<Exception> { handle ->
            val tables = handle.createQuery(
                """SELECT TABLE_NAME FROM USER_TABLES
                   WHERE TABLE_NAME IN ('DISPATCH_BATCH', 'DISPATCH_EVENT', 'DISPATCH_BATCH_STG', 'DISPATCH_EVENT_STG')
                   ORDER BY TABLE_NAME"""
            ).mapTo(String::class.java).list()

            assertEquals(4, tables.size, "Expected all 4 dispatch tables to exist, found: $tables")
            assertTrue(tables.contains("DISPATCH_BATCH"))
            assertTrue(tables.contains("DISPATCH_EVENT"))
            assertTrue(tables.contains("DISPATCH_BATCH_STG"))
            assertTrue(tables.contains("DISPATCH_EVENT_STG"))
        }
    }

    private fun assertColumnParity(prodTable: String, stgTable: String) {
        jdbi.useHandle<Exception> { handle ->
            fun columnsOf(table: String) = handle.createQuery(
                """SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
                   FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :table
                   ORDER BY COLUMN_NAME"""
            ).bind("table", table).mapToMap().list()

            val prodCols = columnsOf(prodTable)
            val stgCols = columnsOf(stgTable)

            assertEquals(prodCols.size, stgCols.size, "Column count mismatch between $prodTable and $stgTable")
            prodCols.zip(stgCols).forEach { (prod, stg) ->
                val colName = prod["COLUMN_NAME"]
                assertEquals(prod["COLUMN_NAME"], stg["COLUMN_NAME"], "Column name mismatch")
                assertEquals(prod["DATA_TYPE"], stg["DATA_TYPE"], "Data type mismatch for $colName")
                assertEquals(prod["DATA_LENGTH"], stg["DATA_LENGTH"], "Data length mismatch for $colName")
                assertEquals(prod["NULLABLE"], stg["NULLABLE"], "Nullable mismatch for $colName")
            }
        }
    }

    // ── Test 2: batch column parity (prod == stg) ───────────────────────

    @Test
    fun batchColumnParity() {
        assertColumnParity("DISPATCH_BATCH", "DISPATCH_BATCH_STG")
    }

    // ── Test 3: event column parity (prod == stg) ───────────────────────

    @Test
    fun eventColumnParity() {
        assertColumnParity("DISPATCH_EVENT", "DISPATCH_EVENT_STG")
    }

    // ── Test 4: PK constraints exist for all 4 tables ───────────────────

    @Test
    fun pkConstraintsExist() {
        jdbi.useHandle<Exception> { handle ->
            val pks = handle.createQuery(
                """SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS
                   WHERE CONSTRAINT_TYPE = 'P'
                   AND TABLE_NAME IN ('DISPATCH_BATCH', 'DISPATCH_EVENT', 'DISPATCH_BATCH_STG', 'DISPATCH_EVENT_STG')
                   ORDER BY CONSTRAINT_NAME"""
            ).mapTo(String::class.java).list()

            assertEquals(4, pks.size, "Expected 4 primary key constraints, found: $pks")
            assertTrue(pks.contains("PK_DISPATCH_BATCH"))
            assertTrue(pks.contains("PK_DISPATCH_EVENT"))
            assertTrue(pks.contains("PK_DISPATCH_BATCH_STG"))
            assertTrue(pks.contains("PK_DISPATCH_EVENT_STG"))
        }
    }

    // ── Test 5: PK rejects duplicate batch_token ────────────────────────

    @Test
    fun pkRejectsDuplicateBatchToken() {
        val token = insertBatch()

        assertThrows<UnableToExecuteStatementException> {
            insertBatch(batchToken = token)
        }

        val tokenStg = insertBatch(table = "dispatch_batch_stg")

        assertThrows<UnableToExecuteStatementException> {
            insertBatch(batchToken = tokenStg, table = "dispatch_batch_stg")
        }
    }

    // ── Test 6: FK constraints exist with correct R_CONSTRAINT_NAME ─────

    @Test
    fun fkConstraintsExist() {
        jdbi.useHandle<Exception> { handle ->
            val fks = handle.createQuery(
                """SELECT CONSTRAINT_NAME, R_CONSTRAINT_NAME FROM USER_CONSTRAINTS
                   WHERE CONSTRAINT_TYPE = 'R'
                   AND TABLE_NAME IN ('DISPATCH_EVENT', 'DISPATCH_EVENT_STG')
                   ORDER BY CONSTRAINT_NAME"""
            ).mapToMap().list()

            assertEquals(2, fks.size, "Expected 2 foreign key constraints, found: $fks")

            val normalized = fks.map { caseInsensitiveMap(it) }
            val fkMap = normalized.associate {
                it["CONSTRAINT_NAME"].toString() to it["R_CONSTRAINT_NAME"].toString()
            }
            assertEquals("PK_DISPATCH_BATCH", fkMap["FK_DISPATCH_EVENT_BATCH"], "FK on dispatch_event should reference pk_dispatch_batch")
            assertEquals("PK_DISPATCH_BATCH_STG", fkMap["FK_DISPATCH_EVENT_STG_BATCH"], "FK on dispatch_event_stg should reference pk_dispatch_batch_stg")
        }
    }

    // ── Test 7: CHECK constraint rejects invalid status ─────────────────

    @Test
    fun checkConstraintRejectsInvalid() {
        assertThrows<UnableToExecuteStatementException> {
            insertBatch(status = "INVALID")
        }

        assertThrows<UnableToExecuteStatementException> {
            insertBatch(status = "INVALID", table = "dispatch_batch_stg")
        }
    }

    // ── Test 8: CHECK constraint accepts NORMAL and DRYRUN ──────────────

    @Test
    fun checkConstraintAcceptsValid() {
        assertDoesNotThrow {
            insertBatch(status = "NORMAL")
        }

        assertDoesNotThrow {
            insertBatch(status = "DRYRUN")
        }

        assertDoesNotThrow {
            insertBatch(status = "NORMAL", table = "dispatch_batch_stg")
        }

        assertDoesNotThrow {
            insertBatch(status = "DRYRUN", table = "dispatch_batch_stg")
        }
    }

    // ── Test 9: Identity column auto-generates id ───────────────────────

    @Test
    fun identityColumnAutoGenerates() {
        val token = insertBatch()
        insertEvent(batchToken = token)

        jdbi.useHandle<Exception> { handle ->
            val id = handle.createQuery(
                "SELECT id FROM dispatch_event WHERE batch_token = :token"
            )
                .bind("token", token)
                .mapTo(Long::class.java)
                .one()

            assertNotNull(id, "Identity column should auto-generate an id")
            assertTrue(id > 0, "Generated id should be positive")
        }

        val tokenStg = insertBatch(table = "dispatch_batch_stg")
        insertEvent(batchToken = tokenStg, table = "dispatch_event_stg")

        jdbi.useHandle<Exception> { handle ->
            val id = handle.createQuery(
                "SELECT id FROM dispatch_event_stg WHERE batch_token = :token"
            )
                .bind("token", tokenStg)
                .mapTo(Long::class.java)
                .one()

            assertNotNull(id, "Identity column should auto-generate an id on stg table")
            assertTrue(id > 0, "Generated id should be positive")
        }
    }

    // ── Test 10: Nullable columns accept NULL ────────────────────────────

    @Test
    fun nullableColumnsAcceptNull() {
        val token = insertBatch(configCount = null)

        jdbi.useHandle<Exception> { handle ->
            val row = handle.createQuery(
                "SELECT config_count FROM dispatch_batch WHERE batch_token = :token"
            )
                .bind("token", token)
                .mapToMap()
                .one()

            assertEquals(null, row["CONFIG_COUNT"], "config_count should accept NULL")
        }

        val token2 = insertBatch()
        insertEvent(batchToken = token2, targetBomId = null, bomGap = null)

        jdbi.useHandle<Exception> { handle ->
            val row = handle.createQuery(
                "SELECT target_bom_id, bom_gap FROM dispatch_event WHERE batch_token = :token"
            )
                .bind("token", token2)
                .mapToMap()
                .one()

            assertEquals(null, row["TARGET_BOM_ID"], "target_bom_id should accept NULL")
            assertEquals(null, row["BOM_GAP"], "bom_gap should accept NULL")
        }
    }

    // ── Test 11: NOT NULL columns reject NULL insert ────────────────────

    @Test
    fun notNullColumnsRejectNull() {
        val ts = now()

        // batch_token NULL on dispatch_batch
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO dispatch_batch (batch_token, status, created_at) VALUES (NULL, 'NORMAL', :ts)"
                ).bind("ts", ts).execute()
            }
        }

        // status NULL on dispatch_batch
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO dispatch_batch (batch_token, status, created_at) VALUES (:token, NULL, :ts)"
                ).bind("token", randomToken()).bind("ts", ts).execute()
            }
        }

        // created_at NULL on dispatch_batch
        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "INSERT INTO dispatch_batch (batch_token, status, created_at) VALUES (:token, 'NORMAL', NULL)"
                ).bind("token", randomToken()).execute()
            }
        }

        // Each required event column must reject NULL
        val token = insertBatch()
        val baseValues = mapOf(
            "config_id" to "'c'", "dispatch_order" to "1", "product_id" to "'p'",
            "source_bom_id" to "'b'", "qty" to "1", "target_site_id" to "'s'", "site_gap" to "1",
        )

        for (nullColumn in baseValues.keys) {
            val values = baseValues.mapValues { (col, v) -> if (col == nullColumn) "NULL" else v }
            val columnList = "batch_token, ${values.keys.joinToString()}"
            val valueList = ":token, ${values.values.joinToString()}"

            assertThrows<UnableToExecuteStatementException>("$nullColumn should reject NULL") {
                jdbi.useHandle<Exception> { handle ->
                    handle.createUpdate(
                        "INSERT INTO dispatch_event ($columnList) VALUES ($valueList)"
                    ).bind("token", token).execute()
                }
            }
        }
    }

    // ── Test 12: All indexes exist ──────────────────────────────────────

    @Test
    fun allIndexesExist() {
        jdbi.useHandle<Exception> { handle ->
            val indexes = handle.createQuery(
                """SELECT INDEX_NAME FROM USER_INDEXES
                   WHERE TABLE_NAME IN ('DISPATCH_BATCH', 'DISPATCH_EVENT', 'DISPATCH_BATCH_STG', 'DISPATCH_EVENT_STG')
                   AND INDEX_NAME NOT LIKE 'SYS_%'
                   ORDER BY INDEX_NAME"""
            ).mapTo(String::class.java).list()

            val expectedIndexes = listOf(
                "IDX_DISPATCH_BATCH_STATUS_CREATED",
                "IDX_DISPATCH_BATCH_STG_STATUS_CREATED",
                "IDX_DISPATCH_EVENT_BATCH_CONFIG",
                "IDX_DISPATCH_EVENT_CONFIG_BATCH",
                "IDX_DISPATCH_EVENT_STG_BATCH_CONFIG",
                "IDX_DISPATCH_EVENT_STG_CONFIG_BATCH",
            )

            expectedIndexes.forEach { expected ->
                assertTrue(
                    indexes.contains(expected),
                    "Expected index $expected to exist, found: $indexes"
                )
            }

            // Verify column composition for representative indexes
            val expectedColumns = mapOf(
                "IDX_DISPATCH_EVENT_BATCH_CONFIG" to listOf("BATCH_TOKEN", "CONFIG_ID"),
                "IDX_DISPATCH_EVENT_CONFIG_BATCH" to listOf("CONFIG_ID", "BATCH_TOKEN"),
                "IDX_DISPATCH_BATCH_STATUS_CREATED" to listOf("STATUS", "CREATED_AT"),
            )
            for ((indexName, expectedCols) in expectedColumns) {
                val actualCols = handle.createQuery(
                    """SELECT COLUMN_NAME FROM USER_IND_COLUMNS
                       WHERE INDEX_NAME = :indexName
                       ORDER BY COLUMN_POSITION"""
                ).bind("indexName", indexName).mapTo(String::class.java).list()

                assertEquals(expectedCols, actualCols, "Index columns for $indexName")
            }
        }
    }

    // ── Test 13: FK rejects orphan event ────────────────────────────────

    @Test
    fun fkRejectsOrphanEvent() {
        val nonExistentToken = "nonexistent-batch-token"

        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    """INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, site_gap)
                       VALUES (:token, 'cfg-1', 1, 'prod-1', 'bom-1', 100, 'site-1', 5)"""
                ).bind("token", nonExistentToken).execute()
            }
        }

        assertThrows<UnableToExecuteStatementException> {
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    """INSERT INTO dispatch_event_stg (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, site_gap)
                       VALUES (:token, 'cfg-1', 1, 'prod-1', 'bom-1', 100, 'site-1', 5)"""
                ).bind("token", nonExistentToken).execute()
            }
        }
    }
}
