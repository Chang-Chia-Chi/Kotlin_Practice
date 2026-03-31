package com.workflow.dispatch.model

import java.math.BigDecimal

data class TargetBomAllocation(
    val targetBomId: String,
    val target: BigDecimal,
)
