package com.workflow.dispatch.model

import java.math.BigDecimal

sealed interface TargetSelection {
    data class Selected(
        val siteId: String,
        val targetBomId: String?,
        val sourceBomConstraint: String?,
        val siteGap: BigDecimal,
        val bomGap: BigDecimal?,
    ) : TargetSelection

    data object NoTarget : TargetSelection
}
