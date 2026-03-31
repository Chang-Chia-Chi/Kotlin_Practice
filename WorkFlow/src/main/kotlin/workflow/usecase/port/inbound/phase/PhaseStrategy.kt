package com.workflow.workflow.usecase.port.inbound.phase

import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.PhaseContext

interface PhaseStrategy {
    fun resolve(context: PhaseContext): AdvancementDecision
}
