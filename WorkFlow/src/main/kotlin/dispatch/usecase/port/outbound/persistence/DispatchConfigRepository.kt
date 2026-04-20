package com.workflow.dispatch.usecase.port.outbound.persistence

import com.workflow.dispatch.model.DispatchCategory
import com.workflow.dispatch.model.DispatchConfig
import java.time.LocalDateTime

interface DispatchConfigRepository {
    /**
     * Return all active configs as of [asOf], optionally filtered by [categories].
     *
     * @param categories When empty, no category predicate is applied (returns all active
     *   configs). When non-empty, narrows the result to configs whose [DispatchConfig.category]
     *   is in the given set (SQL-equivalent `AND category IN (...)`).
     */
    suspend fun findActiveConfigs(
        asOf: LocalDateTime,
        categories: Set<DispatchCategory> = emptySet(),
    ): List<DispatchConfig>

    suspend fun findById(configId: String): DispatchConfig
}
