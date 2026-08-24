package com.example.gauntlet.domain

import arrow.core.Either
import java.time.LocalDate

/**
 * Domain 對外的出口。介面留在 domain，實作在 infrastructure。
 * 回傳型別一律 Either，實作端不准把例外漏出來。
 */
interface OrderRepository {
    fun save(order: Order): Either<DomainError, Unit>
    fun findById(id: String): Either<DomainError, Order>
    fun findByDate(date: LocalDate): Either<DomainError, List<Order>>
}

interface AnalyticsRepository {
    fun upsertDailySummary(summary: DailySummary): Either<DomainError, Unit>
    fun findDailySummary(date: LocalDate): Either<DomainError, DailySummary>
}
