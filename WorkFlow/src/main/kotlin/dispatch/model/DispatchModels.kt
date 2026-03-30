package com.workflow.dispatch.model

import java.math.BigDecimal

enum class DispatchMode { QTY, RATIO }

data class DispatchConfig(
    val id: String,
    val mode: DispatchMode,
    val algorithmId: String,
    /** Source BOM ID prefix (or full) used by CandidateQueryPort to filter candidates. */
    val sourceBomPrefix: String,
    val siteTargets: List<SiteTarget>,
    /** Keyed by siteId. LV2 sourceBomId must be full and start with [sourceBomPrefix]. */
    val bomMappings: Map<String, BomMapping>?,
)

data class SiteTarget(
    val siteId: String,
    /** Absolute quantity in QTY mode, percentage points (0–100) in RATIO mode. */
    val target: BigDecimal,
) {
    init {
        require(target > BigDecimal.ZERO) { "target must be positive, got $target" }
    }
}

data class BomMapping(
    val sourceBomId: String,
    val targetAllocations: List<TargetBomAllocation>,
)

data class TargetBomAllocation(
    val targetBomId: String,
    val target: BigDecimal,
)

data class CandidateProduct(
    val productId: String,
    val sourceBomId: String,
    val qty: Int,
) {
    init {
        require(qty in 1..25) { "qty must be 1-25, got $qty" }
    }
}

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

data class SimulationResult(
    val decisions: List<DispatchDecision>,
    val finalSiteAllocations: Map<String, BigDecimal>,
    val finalBomAllocations: Map<SiteBomKey, BigDecimal>,
)

data class SiteBomKey(val siteId: String, val targetBomId: String)

data class Baseline(
    val siteAllocations: Map<String, BigDecimal>,
    val bomAllocations: Map<SiteBomKey, BigDecimal>,
)
