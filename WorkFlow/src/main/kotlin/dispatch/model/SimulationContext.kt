package com.workflow.dispatch.model

import java.math.BigDecimal

class SimulationContext(
    val siteCurrents: MutableMap<String, BigDecimal>,
    val bomCurrents: MutableMap<SiteBomKey, BigDecimal>,
    var lastSiteId: String? = null,
    val lastBomIds: MutableMap<String, String> = mutableMapOf(),
    val decisions: MutableList<DispatchDecision> = mutableListOf(),
    var total: BigDecimal,
)
