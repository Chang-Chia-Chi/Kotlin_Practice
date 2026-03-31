package com.workflow.dispatch.usecase.port.inbound.algorithm

import java.math.BigDecimal

interface GapComputer {
    fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal
}
