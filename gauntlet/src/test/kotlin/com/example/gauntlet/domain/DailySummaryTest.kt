package com.example.gauntlet.domain

import arrow.core.Either
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import java.time.LocalDate

class DailySummaryTest {

    private val date: LocalDate = LocalDate.of(2026, 8, 24)

    private fun order(id: String, amount: Long, on: LocalDate = date): Order =
        (Order.create(id, "CUST-1", amount, on) as Either.Right).value

    @Test
    fun `aggregates count sum max and average`() {
        val summary = DailySummary.from(
            date,
            listOf(order("A", 100), order("B", 250), order("C", 51)),
        )
        val value = (summary as Either.Right).value
        assertEquals(3, value.orderCount)
        assertEquals(401L, value.totalAmountCents)
        assertEquals(250L, value.maxAmountCents)
        assertEquals(133L, value.averageAmountCents)
    }

    @Test
    fun `single order summarises to itself`() {
        val value = (DailySummary.from(date, listOf(order("A", 999))) as Either.Right).value
        assertEquals(1, value.orderCount)
        assertEquals(999L, value.totalAmountCents)
        assertEquals(999L, value.maxAmountCents)
        assertEquals(999L, value.averageAmountCents)
    }

    @Test
    fun `empty day is an error not a zero row`() {
        val result = DailySummary.from(date, emptyList())
        assertInstanceOf(DomainError.NoDataForDate::class.java, (result as Either.Left).value)
    }

    @Test
    fun `rejects an order that belongs to another day`() {
        val result = DailySummary.from(date, listOf(order("A", 10), order("B", 10, date.minusDays(1))))
        assertInstanceOf(DomainError.StorageFailure::class.java, (result as Either.Left).value)
    }
}
