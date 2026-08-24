package com.example.gauntlet.acceptance

import arrow.core.Either
import com.example.gauntlet.TestDatabases
import com.example.gauntlet.application.BuildDailySummaryUseCase
import com.example.gauntlet.application.NewOrderCommand
import com.example.gauntlet.application.ProcessOrderUseCase
import com.example.gauntlet.domain.DomainError
import com.example.gauntlet.infrastructure.DuckDbAnalyticsRepository
import com.example.gauntlet.infrastructure.SqliteOrderRepository
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.time.LocalDate

/**
 * 第一層：人類看得懂的業務規格。
 * 全程真實 SQLite（OLTP）+ 真實 DuckDB（OLAP），沒有 mock。
 */
class OrderProcessingSpec : BehaviorSpec({

    val dir = TestDatabases.newSqliteDir()
    val duck = TestDatabases.newDuckDb()
    val orders = SqliteOrderRepository(TestDatabases.newSqlite(dir))
    val analytics = DuckDbAnalyticsRepository(duck.jdbi)
    val processOrder = ProcessOrderUseCase(orders)
    val buildSummary = BuildDailySummaryUseCase(orders, analytics)

    afterSpec {
        duck.close()
        TestDatabases.deleteRecursively(dir)
    }

    fun newOrder(id: String, amount: Long, date: LocalDate) =
        NewOrderCommand(id, "CUST-1", amount, date)

    Given("一個乾淨的系統") {
        val day = LocalDate.of(2026, 8, 24)

        When("送進三筆當日訂單") {
            processOrder.execute(newOrder("D1-A", 1_000, day)).isRight() shouldBe true
            processOrder.execute(newOrder("D1-B", 2_000, day)).isRight() shouldBe true
            processOrder.execute(newOrder("D1-C", 3_500, day)).isRight() shouldBe true

            Then("SQLite 讀得回單筆訂單") {
                val found = processOrder.findById("D1-B")
                found.shouldBeInstanceOf<Either.Right<*>>()
                (found as Either.Right).value.amountCents shouldBe 2_000
            }

            Then("觸發每日彙整後，DuckDB 有正確的一列") {
                val summary = buildSummary.execute(day)
                summary.shouldBeInstanceOf<Either.Right<*>>()

                val stored = buildSummary.read(day)
                stored.shouldBeInstanceOf<Either.Right<*>>()
                val value = (stored as Either.Right).value
                value.orderCount shouldBe 3
                value.totalAmountCents shouldBe 6_500
                value.maxAmountCents shouldBe 3_500
                value.averageAmountCents shouldBe 2_166
            }

            Then("重跑彙整不會產生第二列") {
                buildSummary.execute(day).isRight() shouldBe true
                val value = (buildSummary.read(day) as Either.Right).value
                value.orderCount shouldBe 3
            }
        }
    }

    Given("業務錯誤情境") {
        val emptyDay = LocalDate.of(2026, 1, 1)

        When("查一筆不存在的訂單") {
            Then("回傳 OrderNotFound") {
                val result = processOrder.findById("does-not-exist")
                (result as Either.Left).value.shouldBeInstanceOf<DomainError.OrderNotFound>()
            }
        }

        When("送進金額為負的訂單") {
            Then("回傳 InvalidAmount，且不落地") {
                val result = processOrder.execute(newOrder("BAD-1", -1, emptyDay))
                (result as Either.Left).value.shouldBeInstanceOf<DomainError.InvalidAmount>()
                (processOrder.findById("BAD-1") as Either.Left)
                    .value.shouldBeInstanceOf<DomainError.OrderNotFound>()
            }
        }

        When("對一個沒有訂單的日期做彙整") {
            Then("回傳 NoDataForDate，而不是寫一列全 0 進 DuckDB") {
                val result = buildSummary.execute(emptyDay)
                (result as Either.Left).value.shouldBeInstanceOf<DomainError.NoDataForDate>()
                (buildSummary.read(emptyDay) as Either.Left)
                    .value.shouldBeInstanceOf<DomainError.NoDataForDate>()
            }
        }
    }
})
