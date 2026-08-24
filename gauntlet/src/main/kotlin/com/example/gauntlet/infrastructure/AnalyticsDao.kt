package com.example.gauntlet.infrastructure

import org.jdbi.v3.sqlobject.customizer.Bind
import org.jdbi.v3.sqlobject.customizer.BindBean
import org.jdbi.v3.sqlobject.kotlin.RegisterKotlinMapper
import org.jdbi.v3.sqlobject.statement.SqlQuery
import org.jdbi.v3.sqlobject.statement.SqlUpdate
import java.time.LocalDate

data class DailySummaryRow(
    val summaryDate: LocalDate,
    val orderCount: Int,
    val totalAmountCents: Long,
    val maxAmountCents: Long,
    val avgAmountCents: Long,
)

@RegisterKotlinMapper(DailySummaryRow::class)
interface AnalyticsDao {

    @SqlUpdate("DELETE FROM daily_order_summary WHERE summary_date = :summaryDate")
    fun deleteByDate(@Bind("summaryDate") summaryDate: LocalDate)

    @SqlUpdate(
        """
        INSERT INTO daily_order_summary
            (summary_date, order_count, total_amount_cents, max_amount_cents, avg_amount_cents)
        VALUES
            (:summaryDate, :orderCount, :totalAmountCents, :maxAmountCents, :avgAmountCents)
        """,
    )
    fun insert(@BindBean row: DailySummaryRow)

    @SqlQuery(
        """
        SELECT summary_date, order_count, total_amount_cents, max_amount_cents, avg_amount_cents
        FROM daily_order_summary
        WHERE summary_date = :summaryDate
        """,
    )
    fun findByDate(@Bind("summaryDate") summaryDate: LocalDate): DailySummaryRow?
}
