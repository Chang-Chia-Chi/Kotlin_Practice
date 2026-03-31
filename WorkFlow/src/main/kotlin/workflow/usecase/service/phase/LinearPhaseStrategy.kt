package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.PhaseContext
import com.workflow.workflow.model.advanceOrComplete
import com.workflow.workflow.model.failOrAdvance
import com.workflow.workflow.usecase.port.inbound.phase.PhaseStrategy

class LinearPhaseStrategy : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        context.failOrAdvance()?.let { return it }
        return context.advanceOrComplete()
    }
}
