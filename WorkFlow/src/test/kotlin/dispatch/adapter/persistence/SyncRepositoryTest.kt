package com.workflow.dispatch.adapter.persistence

import com.workflow.infrastructure.persistence.OracleTestContainer
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SyncRepositoryTest {

    private lateinit var repo: SyncRepository

    @BeforeAll
    fun setup() {
        repo = SyncRepository(OracleTestContainer.jdbi)
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

    private fun seedProdData() {
        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            h.execute("INSERT INTO dispatch_batch VALUES ('batch1', 'NORMAL', CURRENT_TIMESTAMP, 2)")
            h.execute("INSERT INTO dispatch_batch VALUES ('batch2', 'DRYRUN', CURRENT_TIMESTAMP, 1)")
            h.execute("INSERT INTO dispatch_batch VALUES ('batch3', 'NORMAL', CURRENT_TIMESTAMP, 1)")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch1', 'cfg1', 1, 'P1', 'BOM-A', 10, 'SITE-X', NULL, 5.0, NULL)""")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch1', 'cfg2', 1, 'P2', 'BOM-B', 8, 'SITE-Y', NULL, 3.0, NULL)""")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch2', 'cfg1', 1, 'P3', 'BOM-C', 5, 'SITE-Z', NULL, 2.0, NULL)""")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, target_bom_id, site_gap, bom_gap)
                VALUES ('batch3', 'cfg1', 1, 'P4', 'BOM-D', 3, 'SITE-W', NULL, 1.0, NULL)""")
        }
    }

    @Test
    fun `sync copies NORMAL batch events for specified configs`() = runTest {
        seedProdData()

        val result = repo.syncFromProd(listOf("cfg1"))

        assertEquals(2, result.batchesCopied)
        assertEquals(2, result.eventsCopied)

        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            val stgBatches = h.createQuery("SELECT batch_token FROM dispatch_batch_stg ORDER BY batch_token")
                .mapTo(String::class.java).list()
            assertEquals(listOf("batch1", "batch3"), stgBatches)

            val stgEvents = h.createQuery("SELECT config_id FROM dispatch_event_stg ORDER BY product_id")
                .mapTo(String::class.java).list()
            assertEquals(listOf("cfg1", "cfg1"), stgEvents)
        }
    }

    @Test
    fun `sync replaces existing stg data for synced configs`() = runTest {
        seedProdData()
        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            h.execute("INSERT INTO dispatch_batch_stg VALUES ('old-batch', 'NORMAL', CURRENT_TIMESTAMP, 1)")
            h.execute("""INSERT INTO dispatch_event_stg (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, site_gap)
                VALUES ('old-batch', 'cfg1', 1, 'OLD', 'BOM-OLD', 1, 'SITE-OLD', 0.0)""")
        }

        val result = repo.syncFromProd(listOf("cfg1"))

        assertEquals(2, result.batchesCopied)
        assertEquals(2, result.eventsCopied)

        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            val stgEvents = h.createQuery("SELECT product_id FROM dispatch_event_stg ORDER BY product_id")
                .mapTo(String::class.java).list()
            assertEquals(listOf("P1", "P4"), stgEvents)

            val stgBatches = h.createQuery("SELECT batch_token FROM dispatch_batch_stg ORDER BY batch_token")
                .mapTo(String::class.java).list()
            assertEquals(listOf("batch1", "batch3"), stgBatches)
        }
    }

    @Test
    fun `sync preserves stg data for non-synced configs`() = runTest {
        seedProdData()
        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            h.execute("INSERT INTO dispatch_batch_stg VALUES ('stg-batch', 'NORMAL', CURRENT_TIMESTAMP, 1)")
            h.execute("""INSERT INTO dispatch_event_stg (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, site_gap)
                VALUES ('stg-batch', 'cfg2', 1, 'KEEP', 'BOM-KEEP', 1, 'SITE-KEEP', 0.0)""")
        }

        repo.syncFromProd(listOf("cfg1"))

        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            val cfg2Events = h.createQuery("SELECT product_id FROM dispatch_event_stg WHERE config_id = 'cfg2'")
                .mapTo(String::class.java).list()
            assertEquals(listOf("KEEP"), cfg2Events)
        }
    }

    @Test
    fun `sync with only DRYRUN batches returns zero counts and leaves stg untouched`() = runTest {
        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            h.execute("INSERT INTO dispatch_batch VALUES ('dryrun-only', 'DRYRUN', CURRENT_TIMESTAMP, 1)")
            h.execute("""INSERT INTO dispatch_event (batch_token, config_id, dispatch_order, product_id, source_bom_id, qty, target_site_id, site_gap)
                VALUES ('dryrun-only', 'cfg-dry', 1, 'P-DRY', 'BOM-DRY', 1, 'SITE-DRY', 0.0)""")
        }

        val result = repo.syncFromProd(listOf("cfg-dry"))

        assertEquals(0, result.batchesCopied)
        assertEquals(0, result.eventsCopied)

        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            val stgBatches = h.createQuery("SELECT COUNT(*) FROM dispatch_batch_stg")
                .mapTo(Int::class.java).one()
            assertEquals(0, stgBatches)
            val stgEvents = h.createQuery("SELECT COUNT(*) FROM dispatch_event_stg")
                .mapTo(Int::class.java).one()
            assertEquals(0, stgEvents)
        }
    }

    @Test
    fun `sync with no matching configs returns zero counts`() = runTest {
        seedProdData()

        val result = repo.syncFromProd(listOf("nonexistent-config"))

        assertEquals(0, result.batchesCopied)
        assertEquals(0, result.eventsCopied)

        OracleTestContainer.jdbi.useHandle<Exception> { h ->
            val count = h.createQuery("SELECT COUNT(*) FROM dispatch_event_stg")
                .mapTo(Int::class.java).one()
            assertEquals(0, count)
        }
    }

    @Test
    fun `sync with empty configIds returns zero counts without touching DB`() = runTest {
        seedProdData()

        val result = repo.syncFromProd(emptyList())

        assertEquals(0, result.batchesCopied)
        assertEquals(0, result.eventsCopied)
        assertTrue(result.syncedConfigs.isEmpty())
    }
}
