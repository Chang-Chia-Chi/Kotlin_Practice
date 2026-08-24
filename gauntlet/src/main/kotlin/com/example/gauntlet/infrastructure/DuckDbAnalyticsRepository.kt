package com.example.gauntlet.infrastructure

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.example.gauntlet.domain.AnalyticsRepository
import com.example.gauntlet.domain.DailySummary
import com.example.gauntlet.domain.DomainError
import org.jdbi.v3.core.Jdbi
import java.time.LocalDate

/**
 * OLAP 落地。DuckDB 沒有 upsert 的必要語法差異問題就別鑽，
 * 直接 delete + insert 包在同一個交易裡，語意清楚。
 */
class DuckDbAnalyticsRepository(private val jdbi: Jdbi) : AnalyticsRepository {

    override fun upsertDailySummary(summary: DailySummary): Either<DomainError, Unit> = guard {
        jdbi.useTransaction<Exception> { handle ->
            val dao = handle.attach(AnalyticsDao::class.java)
            dao.deleteByDate(summary.date)
            dao.insert(
                DailySummaryRow(
                    summaryDate = summary.date,
                    orderCount = summary.orderCount,
                    totalAmountCents = summary.totalAmountCents,
                    maxAmountCents = summary.maxAmountCents,
                    avgAmountCents = summary.averageAmountCents,
                ),
            )
        }
    }

    override fun findDailySummary(date: LocalDate): Either<DomainError, DailySummary> {
        val found = guard {
            jdbi.withExtension<DailySummaryRow?, AnalyticsDao, Exception>(AnalyticsDao::class.java) { dao ->
                dao.findByDate(date)
            }
        }
        return found.flatMap { row ->
            if (row == null) {
                DomainError.NoDataForDate(date.toString()).left()
            } else {
                DailySummary(
                    date = row.summaryDate,
                    orderCount = row.orderCount,
                    totalAmountCents = row.totalAmountCents,
                    maxAmountCents = row.maxAmountCents,
                    averageAmountCents = row.avgAmountCents,
                ).right()
            }
        }
    }

    // 這裡是「唯一」允許 catch 泛型 Exception 的地方：JDBC 邊界。
    // 抓到之後立刻翻譯成 DomainError，往上不再有例外。
    @Suppress("TooGenericExceptionCaught")
    private fun <T> guard(block: () -> T): Either<DomainError, T> =
        try {
            block().right()
        } catch (ex: Exception) {
            DomainError.StorageFailure(ex.message ?: ex.javaClass.simpleName).left()
        }
}
