package com.workflow.dispatch.usecase.port.outbound.storage

import com.workflow.dispatch.model.DispatchDecision

interface CsvFormatter {
    fun format(batchToken: String, configId: String, decisions: List<DispatchDecision>): ByteArray
}
