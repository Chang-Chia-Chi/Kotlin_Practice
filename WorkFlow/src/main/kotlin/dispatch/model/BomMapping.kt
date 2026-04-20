package com.workflow.dispatch.model

data class BomMapping(
    val sourceBomId: String,
    val targetAllocations: List<TargetBomAllocation>,
)
