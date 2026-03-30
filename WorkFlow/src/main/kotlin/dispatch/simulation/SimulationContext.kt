package com.workflow.dispatch.simulation

import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.model.SiteBomKey
import java.math.BigDecimal

class SimulationContext(
    val siteCurrents: MutableMap<String, BigDecimal>,
    val bomCurrents: MutableMap<SiteBomKey, BigDecimal>,
    var lastSiteId: String? = null,
    var lastBomId: String? = null,
    val decisions: MutableList<DispatchDecision> = mutableListOf(),
    var total: BigDecimal,
)
