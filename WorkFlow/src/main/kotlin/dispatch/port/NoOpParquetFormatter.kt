package com.workflow.dispatch.port

import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory

@ApplicationScoped
class NoOpParquetFormatter : ParquetFormatter {
    private val log = LoggerFactory.getLogger(NoOpParquetFormatter::class.java)
    override fun format(decisions: List<com.workflow.dispatch.model.DispatchDecision>): ByteArray {
        log.warn("NoOpParquetFormatter in use — parquet output is empty. Replace with real implementation.")
        return ByteArray(0)
    }
}
