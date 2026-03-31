package com.workflow.dispatch.model

import java.math.BigDecimal

data class SimulationResult(
    val decisions: List<DispatchDecision>,
    val finalSiteAllocations: Map<String, BigDecimal>,
    val finalBomAllocations: Map<SiteBomKey, BigDecimal>,
)
