package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.usecase.port.inbound.phase.AdvancementStrategy
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class AdvancementStrategyRegistry {

    private val strategies = ConcurrentHashMap<PhaseType, AdvancementStrategy>()

    init {
        register(PhaseType.LINEAR, LinearAdvancementStrategy())
        register(PhaseType.PARALLEL, ParallelAdvancementStrategy())
    }

    fun register(type: PhaseType, strategy: AdvancementStrategy) {
        strategies[type] = strategy
    }

    fun resolve(type: PhaseType): AdvancementStrategy =
        strategies[type] ?: throw IllegalStateException("No strategy registered for phase type: $type")
}
