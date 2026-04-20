package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.Baseline
import com.workflow.dispatch.model.DispatchConfig

interface BaselineProvider {
    suspend fun loadBaseline(config: DispatchConfig): Baseline
}
