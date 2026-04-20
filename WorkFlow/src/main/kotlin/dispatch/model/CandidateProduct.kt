package com.workflow.dispatch.model

data class CandidateProduct(
    val productId: String,
    val sourceBomId: String,
    val qty: Int,
) {
    init {
        require(qty in 1..25) { "qty must be 1-25, got $qty" }
    }
}
