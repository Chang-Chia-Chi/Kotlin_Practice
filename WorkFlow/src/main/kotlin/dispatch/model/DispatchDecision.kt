package com.workflow.dispatch.model

import java.math.BigDecimal

data class DispatchDecision(
    val dispatchOrder: Int,
    val productId: String,
    val sourceBomId: String,
    val qty: Int,
    val targetSiteId: String,
    val targetBomId: String?,
    val siteGap: BigDecimal,
    val bomGap: BigDecimal?,
)
