package com.workflow.dispatch.adapter.persistence

import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchDecision
import com.workflow.infrastructure.persistence.OracleTestContainer
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbiSimulationResultStoreTest {

    private lateinit var store: JdbiSimulationResultStore
    private lateinit var stgStore: JdbiSimulationResultStore

    @BeforeAll
    fun setup() {
        store = JdbiSimulationResultStore(OracleTestContainer.jdbi, "dispatch_batch", "dispatch_event")
        stgStore = JdbiSimulationResultStore(OracleTestContainer.jdbi, "dispatch_batch_stg", "dispatch_event_stg")
    }

    @AfterEach
    fun cleanTables() {
        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            h.execute("DELETE FROM dispatch_event")
            h.execute("DELETE FROM dispatch_batch")
            h.execute("DELETE FROM dispatch_event_stg")
            h.execute("DELETE FROM dispatch_batch_stg")
        }
    }

    // ── createBatch + findBatchStatus ─────────────────────────────────────

    @Test
    fun `createBatch and findBatchStatus round-trip NORMAL status`() = runTest {
        store.createBatch("batch-normal", BatchStatus.NORMAL, 2)

        val status = store.findBatchStatus("batch-normal")

        assertEquals(BatchStatus.NORMAL, status)
    }

    @Test
    fun `createBatch and findBatchStatus round-trip DRYRUN status`() = runTest {
        store.createBatch("batch-dryrun", BatchStatus.DRYRUN, 1)

        val status = store.findBatchStatus("batch-dryrun")

        assertEquals(BatchStatus.DRYRUN, status)
    }

    @Test
    fun `findBatchStatus throws for unknown token`() = runTest {
        assertThrows<IllegalStateException> {
            store.findBatchStatus("nonexistent-token")
        }
    }

    // ── saveDecisions + findByBatchToken ──────────────────────────────────

    @Test
    fun `saveDecisions and findByBatchToken round-trip with nullable fields null`() = runTest {
        store.createBatch("batch-1", BatchStatus.NORMAL, 1)
        val decisions = listOf(
            DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
        )
        store.saveDecisions("batch-1", "cfg1", decisions)

        val found = store.findByBatchToken("batch-1")

        assertEquals(1, found.size)
        assertEquals("P1", found[0].productId)
        assertEquals("BOM-A", found[0].sourceBomId)
        assertEquals(10, found[0].qty)
        assertEquals("SITE-X", found[0].targetSiteId)
        assertEquals(null, found[0].targetBomId)
        assertEquals(0, BigDecimal("5.0").compareTo(found[0].siteGap))
        assertEquals(null, found[0].bomGap)
    }

    @Test
    fun `saveDecisions and findByBatchToken round-trip with all fields populated`() = runTest {
        store.createBatch("batch-2", BatchStatus.NORMAL, 1)
        val decisions = listOf(
            DispatchDecision(1, "P2", "BOM-B", 5, "SITE-Y", "TGT-BOM-1", BigDecimal("3.00"), BigDecimal("1.50")),
        )
        store.saveDecisions("batch-2", "cfg1", decisions)

        val found = store.findByBatchToken("batch-2")

        assertEquals(1, found.size)
        assertEquals("P2", found[0].productId)
        assertEquals("TGT-BOM-1", found[0].targetBomId)
        assertEquals(0, BigDecimal("1.50").compareTo(found[0].bomGap))
    }

    @Test
    fun `findByBatchToken returns empty list for unknown token`() = runTest {
        val found = store.findByBatchToken("nonexistent-token")

        assertEquals(emptyList(), found)
    }

    @Test
    fun `saveDecisions with empty list is a no-op`() = runTest {
        store.createBatch("batch-empty", BatchStatus.NORMAL, 1)
        store.saveDecisions("batch-empty", "cfg1", emptyList())

        val found = store.findByBatchToken("batch-empty")

        assertEquals(emptyList(), found)
    }

    @Test
    fun `findByBatchToken returns decisions across multiple configIds ordered by config then order`() = runTest {
        store.createBatch("batch-3", BatchStatus.NORMAL, 2)
        store.saveDecisions("batch-3", "cfg-b", listOf(
            DispatchDecision(1, "P-B1", "BOM-B", 8, "SITE-Y", null, BigDecimal("3.0"), null),
        ))
        store.saveDecisions("batch-3", "cfg-a", listOf(
            DispatchDecision(1, "P-A1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
            DispatchDecision(2, "P-A2", "BOM-A", 4, "SITE-X", null, BigDecimal("2.0"), null),
        ))

        val found = store.findByBatchToken("batch-3")

        assertEquals(3, found.size)
        // ordered by config_id then dispatch_order: cfg-a (1,2), cfg-b (1)
        assertEquals("P-A1", found[0].productId)
        assertEquals("P-A2", found[1].productId)
        assertEquals("P-B1", found[2].productId)
    }

    // ── Table isolation ───────────────────────────────────────────────────

    @Test
    fun `prod store writes do not appear in stg store reads`() = runTest {
        store.createBatch("iso-batch", BatchStatus.NORMAL, 1)
        store.saveDecisions("iso-batch", "cfg1", listOf(
            DispatchDecision(1, "P1", "BOM-A", 10, "SITE-X", null, BigDecimal("5.0"), null),
        ))

        val stgResult = stgStore.findByBatchToken("iso-batch")

        assertTrue(stgResult.isEmpty(), "Prod writes must not appear in stg reads")
    }

    @Test
    fun `stg store writes do not appear in prod store reads`() = runTest {
        stgStore.createBatch("stg-iso-batch", BatchStatus.DRYRUN, 1)
        stgStore.saveDecisions("stg-iso-batch", "cfg1", listOf(
            DispatchDecision(1, "P-STG", "BOM-STG", 3, "SITE-STG", null, BigDecimal("1.0"), null),
        ))

        val prodResult = store.findByBatchToken("stg-iso-batch")

        assertTrue(prodResult.isEmpty(), "Stg writes must not appear in prod reads")
    }
}
