package com.workflow.dispatch.usecase.port.outbound.storage

import com.workflow.dispatch.model.DispatchDecision

interface ParquetFormatter {
    fun format(decisions: List<DispatchDecision>): ByteArray
}
