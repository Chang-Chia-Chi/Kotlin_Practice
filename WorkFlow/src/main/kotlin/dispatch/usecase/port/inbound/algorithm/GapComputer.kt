package com.workflow.dispatch.usecase.port.inbound.algorithm

import java.math.BigDecimal

interface GapComputer {
    val useCumulativeTiebreaker: Boolean
    fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal
}
