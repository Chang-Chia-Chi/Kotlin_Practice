package com.workflow.dispatch.algorithm

import java.math.BigDecimal
import java.math.RoundingMode

interface GapComputer {
    fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal
}

class QtyGapComputer : GapComputer {
    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal =
        current - target
}

class RatioGapComputer : GapComputer {
    override fun computeGap(current: BigDecimal, target: BigDecimal, total: BigDecimal): BigDecimal {
        val currentRatio = if (total > BigDecimal.ZERO) {
            current.divide(total, 10, RoundingMode.HALF_UP)
        } else {
            BigDecimal.ZERO
        }
        val targetRatio = target.divide(BigDecimal(100), 10, RoundingMode.HALF_UP)
        return currentRatio - targetRatio
    }
}
