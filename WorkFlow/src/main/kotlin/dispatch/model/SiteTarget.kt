package com.workflow.dispatch.model

import java.math.BigDecimal

data class SiteTarget(
    val siteId: String,
    /** Absolute quantity in QTY mode, percentage points (0-100) in RATIO mode. */
    val target: BigDecimal,
) {
    init {
        require(target > BigDecimal.ZERO) { "target must be positive, got $target" }
    }
}
