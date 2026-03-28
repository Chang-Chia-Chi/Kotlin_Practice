package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class PhaseStrategyRegistry(objectMapper: ObjectMapper) {

    private val strategies = ConcurrentHashMap<PhaseType, PhaseStrategy>()

    init {
        register(PhaseType.LINEAR, LinearPhaseStrategy())
        register(PhaseType.SCATTER, ScatterPhaseStrategy(objectMapper))
        register(PhaseType.PARALLEL, ParallelPhaseStrategy(objectMapper))
    }

    fun register(type: PhaseType, strategy: PhaseStrategy) {
        strategies[type] = strategy
    }

    fun resolve(type: PhaseType): PhaseStrategy =
        strategies[type] ?: throw IllegalStateException("No strategy registered for phase type: $type")
}
