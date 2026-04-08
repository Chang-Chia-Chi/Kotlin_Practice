package com.workflow.dispatch.usecase.service.algorithm

import java.math.BigDecimal

data class GapEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
)

fun selectByGap(entries: List<GapEntry>, lastSelected: String?): GapEntry? {
    if (entries.isEmpty()) return null
    val n = entries.size
    val lastIdx = entries.indexOfFirst { it.id == lastSelected }
    // (i - lastIdx - 1 + n) % n: gives rank 0 to the entry just after lastSelected, cycling forward.
    // +n ensures non-negative before %, since Kotlin % can return negative for negative dividends.
    val cyclicRank = entries.mapIndexed { i, e -> e.id to (i - lastIdx - 1 + n) % n }.toMap()
    return entries.minWithOrNull(
        compareBy<GapEntry> { it.gap }
            .thenByDescending { it.target }
            .thenBy { cyclicRank[it.id] },
    )
}
