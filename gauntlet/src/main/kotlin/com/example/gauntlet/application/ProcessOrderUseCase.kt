package com.example.gauntlet.application

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.example.gauntlet.domain.DomainError
import com.example.gauntlet.domain.Order
import com.example.gauntlet.domain.OrderRepository
import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDate

/**
 * OLTP 入口：驗證訂單、擋重複、寫進 SQLite。
 */
@ApplicationScoped
class ProcessOrderUseCase(private val orders: OrderRepository) {

    fun execute(command: NewOrderCommand): Either<DomainError, Order> = either {
        val order = Order.create(
            id = command.id,
            customerId = command.customerId,
            amountCents = command.amountCents,
            orderDate = command.orderDate,
        ).bind()

        ensure(isNew(order.id).bind()) { DomainError.DuplicateOrder(order.id) }

        orders.save(order).bind()
        order
    }

    fun findById(id: String?): Either<DomainError, Order> {
        if (id.isNullOrBlank()) {
            return DomainError.InvalidOrderId("id must not be blank").left()
        }
        return orders.findById(id)
    }

    private fun isNew(id: String): Either<DomainError, Boolean> =
        when (val existing = orders.findById(id)) {
            is Either.Right -> Either.Right(false)
            is Either.Left -> when (existing.value) {
                is DomainError.OrderNotFound -> Either.Right(true)
                else -> existing
            }
        }
}

data class NewOrderCommand(
    val id: String?,
    val customerId: String?,
    val amountCents: Long?,
    val orderDate: LocalDate?,
)
