package com.workflow.dispatch.algorithm

import java.math.BigDecimal

data class SelectionEntry(
    val id: String,
    val gap: BigDecimal,
    val target: BigDecimal,
)

fun selectByGap(entries: List<SelectionEntry>, lastSelected: String?): String? {
    if (entries.isEmpty()) return null
    return entries
        .sortedWith(
            compareBy<SelectionEntry> { it.gap }
                .thenByDescending { it.target }
                .thenByDescending { it.id == lastSelected },
        )
        .first()
        .id
}
