package com.example.gauntlet.adapter

data class NewOrderRequest(
    val id: String? = null,
    val customerId: String? = null,
    val amountCents: Long? = null,
    val orderDate: String? = null,
)

data class OrderResponse(
    val id: String,
    val customerId: String,
    val amountCents: Long,
    val orderDate: String,
)

data class DailySummaryResponse(
    val date: String,
    val orderCount: Int,
    val totalAmountCents: Long,
    val maxAmountCents: Long,
    val averageAmountCents: Long,
)

/** RFC 9457 風格的錯誤輸出。 */
data class ProblemResponse(
    val type: String,
    val title: String,
    val status: Int,
    val detail: String,
)
