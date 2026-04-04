package com.workflow.dispatch.usecase.service.algorithm

import java.math.BigDecimal

data class GapEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
)

fun selectByGap(entries: List<GapEntry>, lastSelected: String?): GapEntry? {
    if (entries.isEmpty()) return null
    return entries.minWithOrNull(
        compareBy<GapEntry> { it.gap }
            .thenByDescending { it.target }
            .thenBy { it.id == lastSelected },
    )
}
