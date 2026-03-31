package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.DispatchConfig
import java.time.LocalDateTime

interface DispatchConfigRepository {
    suspend fun findActiveConfigs(asOf: LocalDateTime): List<DispatchConfig>
    suspend fun findById(configId: String): DispatchConfig
}
