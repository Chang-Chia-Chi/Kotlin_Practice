package com.workflow.dispatch.adapter.persistence

import com.workflow.infrastructure.persistence.inTransactionSuspend
import org.jdbi.v3.core.Jdbi

/**
 * Result of a prod-to-stg sync operation.
 *
 * @property syncedConfigs the config IDs that were requested for sync
 * @property batchesCopied number of new batch records inserted into stg (excludes already-present batches)
 * @property eventsCopied number of event records copied into stg
 */
data class SyncResult(
    val syncedConfigs: List<String>,
    val batchesCopied: Int,
    val eventsCopied: Int,
)

/**
 * Copies dispatch data from prod tables (`dispatch_batch`, `dispatch_event`)
 * into stg tables (`dispatch_batch_stg`, `dispatch_event_stg`) for the
 * specified config IDs.
 *
 * The sync is a selective replace: stg events for the requested configs are
 * deleted first, orphaned stg batches are cleaned, then matching NORMAL
 * batches and their events are copied from prod.
 */
class SyncRepository(private val jdbi: Jdbi) {

    /**
     * Synchronizes dispatch data from prod to stg for the given [configIds]
     * in a single transaction.
     *
     * Algorithm:
     * 1. Delete stg events for the specified configIds
     * 2. Delete orphaned stg batches (no remaining events)
     * 3. Insert NORMAL batch records into stg (skip already-present batches)
     * 4. Copy prod events for specified configIds into stg
     */
    suspend fun syncFromProd(configIds: List<String>): SyncResult {
        if (configIds.isEmpty()) return SyncResult(configIds, 0, 0)

        return jdbi.inTransactionSuspend<SyncResult, Exception> { h ->
            // 1. Delete stg events for the specified configs
            h.createUpdate("DELETE FROM dispatch_event_stg WHERE config_id IN (<configIds>)")
                .bindList("configIds", configIds)
                .execute()

            // 2. Delete orphaned stg batches (no remaining events)
            h.createUpdate("""
                DELETE FROM dispatch_batch_stg
                WHERE batch_token NOT IN (SELECT DISTINCT batch_token FROM dispatch_event_stg)
            """).execute()

            // 3. Insert NORMAL batch records into stg (skip if already present)
            val batchesCopied = h.createUpdate("""
                INSERT INTO dispatch_batch_stg (batch_token, status, created_at, config_count)
                SELECT b.batch_token, b.status, b.created_at, b.config_count
                FROM dispatch_batch b
                WHERE b.status = 'NORMAL'
                  AND EXISTS (SELECT 1 FROM dispatch_event e
                              WHERE e.batch_token = b.batch_token
                                AND e.config_id IN (<configIds>))
                  AND NOT EXISTS (SELECT 1 FROM dispatch_batch_stg s
                                  WHERE s.batch_token = b.batch_token)
            """)
                .bindList("configIds", configIds)
                .execute()

            // 4. Copy events from prod to stg for the specified configs
            val eventsCopied = h.createUpdate("""
                INSERT INTO dispatch_event_stg
                    (batch_token, config_id, dispatch_order, product_id, source_bom_id,
                     qty, target_site_id, target_bom_id, site_gap, bom_gap)
                SELECT e.batch_token, e.config_id, e.dispatch_order, e.product_id, e.source_bom_id,
                       e.qty, e.target_site_id, e.target_bom_id, e.site_gap, e.bom_gap
                FROM dispatch_event e
                JOIN dispatch_batch b ON b.batch_token = e.batch_token
                WHERE b.status = 'NORMAL'
                  AND e.config_id IN (<configIds>)
            """)
                .bindList("configIds", configIds)
                .execute()

            SyncResult(configIds, batchesCopied, eventsCopied)
        }
    }
}
