package com.example.gauntlet.domain

/**
 * Domain 層唯一的失敗表達方式。這層不允許 throw，任何失敗都要變成這些型別之一。
 */
sealed interface DomainError {
    val message: String

    data class InvalidOrderId(override val message: String) : DomainError
    data class InvalidCustomer(override val message: String) : DomainError
    data class InvalidAmount(override val message: String) : DomainError
    data class InvalidDate(override val message: String) : DomainError

    data class OrderNotFound(val orderId: String) : DomainError {
        override val message: String = "order not found: $orderId"
    }

    data class DuplicateOrder(val orderId: String) : DomainError {
        override val message: String = "order already exists: $orderId"
    }

    data class NoDataForDate(val date: String) : DomainError {
        override val message: String = "no orders on $date"
    }

    /** infrastructure 把底層例外翻譯成這個，domain 只看得到這一層。 */
    data class StorageFailure(override val message: String) : DomainError
}
