package com.workflow.engine

class LinearPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        context.failOrAdvance()?.let { return it }
        return context.advanceOrComplete()
    }
}
