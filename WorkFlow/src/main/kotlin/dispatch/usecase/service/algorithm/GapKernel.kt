package com.workflow.dispatch.usecase.service.algorithm

import java.math.BigDecimal

data class GapEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
    val current: BigDecimal,
)

fun selectByGap(entries: List<GapEntry>, lastSelected: String?, useCumulative: Boolean): GapEntry? {
    if (entries.isEmpty()) return null
    val lastIdx = entries.indexOfFirst { it.id == lastSelected }
    val comparator = compareBy<Int> { entries[it].gap }
        .thenByDescending { entries[it].target }
        .run { if (useCumulative) thenBy { entries[it].current } else this }
        .thenBy { if (it == lastIdx) 0 else 1 }
    return entries.indices.minWithOrNull(comparator)?.let { entries[it] }
}
