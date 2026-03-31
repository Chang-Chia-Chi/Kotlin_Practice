package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.CandidateProduct
import com.workflow.dispatch.model.DispatchConfig

interface CandidateQueryPort {
    suspend fun queryCandidates(config: DispatchConfig): List<CandidateProduct>
}
