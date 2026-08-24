package com.example.gauntlet.domain

import arrow.core.Either
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.LocalDate

/**
 * 純 domain 驗證。JUnit 5 寫法，讓 Pitest 穩定抓得到測試。
 */
class OrderTest {

    private val today: LocalDate = LocalDate.of(2026, 8, 24)

    @Nested
    @DisplayName("合法輸入")
    inner class Valid {

        @Test
        fun `builds an order and trims whitespace`() {
            val result = Order.create("  ORD-1 ", " CUST-9 ", 1_500L, today)
            val order = (result as Either.Right).value
            assertEquals("ORD-1", order.id)
            assertEquals("CUST-9", order.customerId)
            assertEquals(1_500L, order.amountCents)
            assertEquals(today, order.orderDate)
        }

        @Test
        fun `accepts the exact upper bound of amount`() {
            val result = Order.create("ORD-1", "CUST-9", Order.MAX_AMOUNT_CENTS, today)
            assertTrue(result.isRight())
        }

        @Test
        fun `accepts the exact maximum id length`() {
            val id = "A".repeat(Order.MAX_ID_LENGTH)
            assertTrue(Order.create(id, "CUST-9", 1L, today).isRight())
        }
    }

    @Nested
    @DisplayName("敵意輸入")
    inner class Hostile {

        @Test
        fun `rejects null id`() {
            assertError<DomainError.InvalidOrderId>(Order.create(null, "CUST-9", 1L, today))
        }

        @Test
        fun `rejects blank id`() {
            assertError<DomainError.InvalidOrderId>(Order.create("   ", "CUST-9", 1L, today))
        }

        @Test
        fun `rejects oversized id`() {
            val id = "A".repeat(Order.MAX_ID_LENGTH + 1)
            assertError<DomainError.InvalidOrderId>(Order.create(id, "CUST-9", 1L, today))
        }

        @Test
        fun `rejects null customer`() {
            assertError<DomainError.InvalidCustomer>(Order.create("ORD-1", null, 1L, today))
        }

        @Test
        fun `rejects a 10k character customer id`() {
            val customer = "C".repeat(10_000)
            assertError<DomainError.InvalidCustomer>(Order.create("ORD-1", customer, 1L, today))
        }

        @Test
        fun `rejects null amount`() {
            assertError<DomainError.InvalidAmount>(Order.create("ORD-1", "CUST-9", null, today))
        }

        @Test
        fun `rejects zero amount`() {
            assertError<DomainError.InvalidAmount>(Order.create("ORD-1", "CUST-9", 0L, today))
        }

        @Test
        fun `rejects negative amount`() {
            assertError<DomainError.InvalidAmount>(Order.create("ORD-1", "CUST-9", -1L, today))
        }

        @Test
        fun `rejects amount above the ceiling`() {
            val tooMuch = Order.MAX_AMOUNT_CENTS + 1
            assertError<DomainError.InvalidAmount>(Order.create("ORD-1", "CUST-9", tooMuch, today))
        }

        @Test
        fun `rejects null date`() {
            assertError<DomainError.InvalidDate>(Order.create("ORD-1", "CUST-9", 1L, null))
        }
    }

    private inline fun <reified E : DomainError> assertError(result: Either<DomainError, Order>) {
        val error = (result as Either.Left).value
        assertInstanceOf(E::class.java, error)
        assertTrue(error.message.isNotBlank())
    }
}
