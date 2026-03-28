package com.workflow.engine

class LinearPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        val payload = context.tasks.firstOrNull()?.resultJson
        context.failOrAdvance(payload)?.let { return it }
        return context.advanceOrComplete(payload)
    }
}
