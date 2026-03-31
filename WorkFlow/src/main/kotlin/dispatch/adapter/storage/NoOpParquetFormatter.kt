package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory

@ApplicationScoped
class NoOpParquetFormatter : ParquetFormatter {
    private val log = LoggerFactory.getLogger(NoOpParquetFormatter::class.java)
    override fun format(decisions: List<DispatchDecision>): ByteArray {
        log.warn("NoOpParquetFormatter in use — parquet output is empty. Replace with real implementation.")
        return ByteArray(0)
    }
}
