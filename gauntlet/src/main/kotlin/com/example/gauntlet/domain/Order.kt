package com.example.gauntlet.domain

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import java.time.LocalDate

/**
 * 訂單。建構一律走 [create]，不合法的資料進不了型別。
 */
data class Order internal constructor(
    val id: String,
    val customerId: String,
    val amountCents: Long,
    val orderDate: LocalDate,
) {
    companion object {
        const val MAX_ID_LENGTH: Int = 36
        const val MAX_CUSTOMER_LENGTH: Int = 64
        const val MAX_AMOUNT_CENTS: Long = 100_000_000L

        fun create(
            id: String?,
            customerId: String?,
            amountCents: Long?,
            orderDate: LocalDate?,
        ): Either<DomainError, Order> = either {
            val cleanId = id?.trim()
            ensure(!cleanId.isNullOrEmpty()) { DomainError.InvalidOrderId("id must not be blank") }
            ensure(cleanId.length <= MAX_ID_LENGTH) {
                DomainError.InvalidOrderId("id longer than $MAX_ID_LENGTH")
            }

            val cleanCustomer = customerId?.trim()
            ensure(!cleanCustomer.isNullOrEmpty()) {
                DomainError.InvalidCustomer("customerId must not be blank")
            }
            ensure(cleanCustomer.length <= MAX_CUSTOMER_LENGTH) {
                DomainError.InvalidCustomer("customerId longer than $MAX_CUSTOMER_LENGTH")
            }

            val amount = ensureNotNull(amountCents) { DomainError.InvalidAmount("amount is required") }
            ensure(amount > 0L) { DomainError.InvalidAmount("amount must be positive") }
            ensure(amount <= MAX_AMOUNT_CENTS) {
                DomainError.InvalidAmount("amount exceeds $MAX_AMOUNT_CENTS")
            }

            val date = ensureNotNull(orderDate) { DomainError.InvalidDate("orderDate is required") }

            Order(cleanId, cleanCustomer, amount, date)
        }
    }
}
