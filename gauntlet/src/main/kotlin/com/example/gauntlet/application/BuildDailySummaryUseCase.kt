package com.example.gauntlet.application

import arrow.core.Either
import arrow.core.raise.either
import com.example.gauntlet.domain.AnalyticsRepository
import com.example.gauntlet.domain.DailySummary
import com.example.gauntlet.domain.DomainError
import com.example.gauntlet.domain.OrderRepository
import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDate

/**
 * OLTP -> OLAP：從 SQLite 讀當日訂單，在 domain 彙整，寫進 DuckDB。
 */
@ApplicationScoped
class BuildDailySummaryUseCase(
    private val orders: OrderRepository,
    private val analytics: AnalyticsRepository,
) {
    fun execute(date: LocalDate): Either<DomainError, DailySummary> = either {
        val ordersOfDay = orders.findByDate(date).bind()
        val summary = DailySummary.from(date, ordersOfDay).bind()
        analytics.upsertDailySummary(summary).bind()
        summary
    }

    fun read(date: LocalDate): Either<DomainError, DailySummary> = analytics.findDailySummary(date)
}
