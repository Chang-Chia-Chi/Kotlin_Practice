package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.usecase.port.inbound.phase.PhaseStrategy
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class PhaseStrategyRegistry {

    private val strategies = ConcurrentHashMap<PhaseType, PhaseStrategy>()

    init {
        register(PhaseType.LINEAR, LinearPhaseStrategy())
        register(PhaseType.PARALLEL, ParallelPhaseStrategy())
    }

    fun register(type: PhaseType, strategy: PhaseStrategy) {
        strategies[type] = strategy
    }

    fun resolve(type: PhaseType): PhaseStrategy =
        strategies[type] ?: throw IllegalStateException("No strategy registered for phase type: $type")
}
