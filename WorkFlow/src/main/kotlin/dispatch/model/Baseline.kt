package com.workflow.dispatch.model

import java.math.BigDecimal

data class Baseline(
    val siteAllocations: Map<String, BigDecimal>,
    val bomAllocations: Map<SiteBomKey, BigDecimal>,
)
