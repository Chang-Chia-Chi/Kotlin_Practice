package com.example.gauntlet.domain

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import java.time.LocalDate

/**
 * 每日彙整結果，是要寫進 DuckDB（OLAP）的東西。
 * 彙整邏輯刻意留在 domain，Pitest 才打得到它。
 */
data class DailySummary(
    val date: LocalDate,
    val orderCount: Int,
    val totalAmountCents: Long,
    val maxAmountCents: Long,
    val averageAmountCents: Long,
) {
    companion object {
        fun from(date: LocalDate, orders: List<Order>): Either<DomainError, DailySummary> = either {
            ensure(orders.isNotEmpty()) { DomainError.NoDataForDate(date.toString()) }

            val mismatched = orders.firstOrNull { it.orderDate != date }
            ensure(mismatched == null) {
                DomainError.StorageFailure(
                    "order ${mismatched?.id} belongs to ${mismatched?.orderDate}, not $date",
                )
            }

            val total = orders.sumOf { it.amountCents }
            DailySummary(
                date = date,
                orderCount = orders.size,
                totalAmountCents = total,
                maxAmountCents = orders.maxOf { it.amountCents },
                averageAmountCents = total / orders.size,
            )
        }
    }
}
