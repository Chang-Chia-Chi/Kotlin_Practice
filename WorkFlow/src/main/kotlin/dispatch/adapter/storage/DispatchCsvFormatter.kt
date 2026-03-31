package com.workflow.dispatch.adapter.storage

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.storage.CsvFormatter
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DispatchCsvFormatter : CsvFormatter {

    private val csvMapper = CsvMapper().apply {
        registerModule(KotlinModule.Builder().build())
    }

    private val schema = CsvSchema.builder()
        .addColumn("batch_token")
        .addColumn("config_id")
        .addColumn("dispatch_order", CsvSchema.ColumnType.NUMBER)
        .addColumn("product_id")
        .addColumn("source_bom_id")
        .addColumn("qty", CsvSchema.ColumnType.NUMBER)
        .addColumn("target_site_id")
        .addColumn("target_bom_id")
        .addColumn("site_gap", CsvSchema.ColumnType.NUMBER)
        .addColumn("bom_gap", CsvSchema.ColumnType.NUMBER)
        .build()
        .withHeader()

    override fun format(
        batchToken: String,
        configId: String,
        decisions: List<DispatchDecision>,
    ): ByteArray {
        val rows = decisions.map { d ->
            mapOf(
                "batch_token" to batchToken,
                "config_id" to configId,
                "dispatch_order" to d.dispatchOrder,
                "product_id" to d.productId,
                "source_bom_id" to d.sourceBomId,
                "qty" to d.qty,
                "target_site_id" to d.targetSiteId,
                "target_bom_id" to (d.targetBomId ?: ""),
                "site_gap" to d.siteGap,
                "bom_gap" to (d.bomGap ?: ""),
            )
        }
        return csvMapper.writer(schema).writeValueAsBytes(rows)
    }
}
