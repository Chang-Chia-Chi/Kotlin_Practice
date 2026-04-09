package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.usecase.port.inbound.algorithm.GapComputer
import java.math.BigDecimal

class QtyGapComputer : GapComputer {
    override val useCumulativeTiebreaker: Boolean = false
    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal =
        current - target
}
