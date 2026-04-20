package com.workflow.dispatch.adapter.persistence

import com.workflow.dispatch.model.BatchStatus
import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.persistence.SimulationResultStore
import com.workflow.infrastructure.persistence.DB_ZONE
import com.workflow.infrastructure.persistence.caseInsensitive
import com.workflow.infrastructure.persistence.inTransactionSuspend
import com.workflow.infrastructure.persistence.withHandleSuspend
import org.jdbi.v3.core.Jdbi
import java.math.BigDecimal
import java.sql.Types
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

/**
 * JDBI adapter for [SimulationResultStore], parameterized by table names.
 *
 * Table names come from [DispatchPersistenceProducer] based on `dispatch.env` config,
 * which selects either prod (`dispatch_batch` / `dispatch_event`) or
 * staging (`dispatch_batch_stg` / `dispatch_event_stg`) tables.
 */
class JdbiSimulationResultStore(
    private val jdbi: Jdbi,
    private val batchTable: String,
    private val eventTable: String,
) : SimulationResultStore {

    override suspend fun createBatch(batchToken: String, status: BatchStatus, configCount: Int) {
        val now = LocalDateTime.now(DB_ZONE).truncatedTo(ChronoUnit.MICROS)
        jdbi.inTransactionSuspend<Unit, Exception> { h ->
            h.createUpdate(
                "INSERT INTO $batchTable (batch_token, status, created_at, config_count) " +
                    "VALUES (:token, :status, :createdAt, :count)"
            )
                .bind("token", batchToken)
                .bind("status", status.name)
                .bind("createdAt", now)
                .bind("count", configCount)
                .execute()
        }
    }

    override suspend fun findBatchStatus(batchToken: String): BatchStatus {
        return jdbi.withHandleSuspend<BatchStatus, Exception> { h ->
            val status = h.createQuery("SELECT status FROM $batchTable WHERE batch_token = :token")
                .bind("token", batchToken)
                .mapTo(String::class.java)
                .one()
            BatchStatus.valueOf(status)
        }
    }

    override suspend fun saveDecisions(
        batchToken: String,
        configId: String,
        decisions: List<DispatchDecision>,
    ) {
        if (decisions.isEmpty()) return
        jdbi.inTransactionSuspend<Unit, Exception> { h ->
            val batch = h.prepareBatch(
                """INSERT INTO $eventTable
                   (batch_token, config_id, dispatch_order, product_id, source_bom_id,
                    qty, target_site_id, target_bom_id, site_gap, bom_gap)
                   VALUES (:batchToken, :configId, :dispatchOrder, :productId, :sourceBomId,
                           :qty, :targetSiteId, :targetBomId, :siteGap, :bomGap)"""
            )
            for (d in decisions) {
                batch.bind("batchToken", batchToken)
                    .bind("configId", configId)
                    .bind("dispatchOrder", d.dispatchOrder)
                    .bind("productId", d.productId)
                    .bind("sourceBomId", d.sourceBomId)
                    .bind("qty", d.qty)
                    .bind("targetSiteId", d.targetSiteId)
                    .bind("siteGap", d.siteGap)
                if (d.targetBomId != null) batch.bind("targetBomId", d.targetBomId)
                else batch.bindNull("targetBomId", Types.VARCHAR)
                if (d.bomGap != null) batch.bind("bomGap", d.bomGap)
                else batch.bindNull("bomGap", Types.NUMERIC)
                batch.add()
            }
            batch.execute()
        }
    }

    override suspend fun findByBatchTokenAndConfigs(
        batchToken: String,
        configIds: List<String>,
    ): List<DispatchDecision> {
        if (configIds.isEmpty()) return emptyList()
        return jdbi.withHandleSuspend<List<DispatchDecision>, Exception> { h ->
            h.createQuery(
                """SELECT dispatch_order, product_id, source_bom_id, qty,
                          target_site_id, target_bom_id, site_gap, bom_gap
                   FROM $eventTable
                   WHERE batch_token = :token AND config_id IN (<configIds>)
                   ORDER BY config_id, dispatch_order"""
            )
                .bind("token", batchToken)
                .bindList("configIds", configIds)
                .mapToMap()
                .list()
                .map { rawRow ->
                    val row = caseInsensitive(rawRow)
                    DispatchDecision(
                        dispatchOrder = (row["dispatch_order"] as Number).toInt(),
                        productId = row["product_id"] as String,
                        sourceBomId = row["source_bom_id"] as String,
                        qty = (row["qty"] as Number).toInt(),
                        targetSiteId = row["target_site_id"] as String,
                        targetBomId = row["target_bom_id"] as String?,
                        siteGap = row["site_gap"] as BigDecimal,
                        bomGap = row["bom_gap"] as BigDecimal?,
                    )
                }
        }
    }

    override suspend fun findByBatchToken(batchToken: String): List<DispatchDecision> {
        return jdbi.withHandleSuspend<List<DispatchDecision>, Exception> { h ->
            h.createQuery(
                """SELECT dispatch_order, product_id, source_bom_id, qty,
                          target_site_id, target_bom_id, site_gap, bom_gap
                   FROM $eventTable
                   WHERE batch_token = :token
                   ORDER BY config_id, dispatch_order"""
            )
                .bind("token", batchToken)
                .mapToMap()
                .list()
                .map { rawRow ->
                    val row = caseInsensitive(rawRow)
                    DispatchDecision(
                        dispatchOrder = (row["dispatch_order"] as Number).toInt(),
                        productId = row["product_id"] as String,
                        sourceBomId = row["source_bom_id"] as String,
                        qty = (row["qty"] as Number).toInt(),
                        targetSiteId = row["target_site_id"] as String,
                        targetBomId = row["target_bom_id"] as String?,
                        siteGap = row["site_gap"] as BigDecimal,
                        bomGap = row["bom_gap"] as BigDecimal?,
                    )
                }
        }
    }
}
