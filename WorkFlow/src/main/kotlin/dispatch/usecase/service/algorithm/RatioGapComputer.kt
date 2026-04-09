package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.usecase.port.inbound.algorithm.GapComputer
import java.math.BigDecimal
import java.math.RoundingMode

class RatioGapComputer : GapComputer {

    override val useCumulativeTiebreaker: Boolean = true

    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal {
        val currentRatio = if (total > BigDecimal.ZERO) {
            current.divide(total, 10, RoundingMode.HALF_UP)
        } else {
            BigDecimal.ZERO
        }
        val targetRatio = target.divide(HUNDRED, 10, RoundingMode.HALF_UP)
        return currentRatio - targetRatio
    }

    private companion object {
        val HUNDRED: BigDecimal = BigDecimal("100")
    }
}
