package com.workflow.engine

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
