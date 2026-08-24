package com.example.gauntlet.application

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.example.gauntlet.domain.DomainError
import com.example.gauntlet.domain.Order
import com.example.gauntlet.domain.OrderRepository
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import java.time.LocalDate

/**
 * 這裡 mock 的是 [OrderRepository]——我們自己定義的 port，不是 JDBI、不是 Connection。
 *
 * 存在的理由：像「寫入時磁碟壞掉」這種失敗，用真實 SQLite 逼不出來，
 * 硬要逼就得寫「指向不存在的路徑」那種取巧測試，又髒又不穩。
 * 這個 class 完全不碰 JDBI，所以 mock 用得理直氣壯。
 */
class ProcessOrderFailurePathTest {

    private val date: LocalDate = LocalDate.of(2026, 8, 24)
    private val command = NewOrderCommand("ORD-9", "CUST-1", 500L, date)

    @Test
    fun `a lookup failure is reported as StorageFailure, not swallowed`() {
        val repository = mockk<OrderRepository>()
        every { repository.findById(any()) } returns DomainError.StorageFailure("disk gone").left()

        val result = ProcessOrderUseCase(repository).execute(command)

        assertInstanceOf(DomainError.StorageFailure::class.java, (result as Either.Left).value)
        verify(exactly = 0) { repository.save(any()) }
    }

    @Test
    fun `a write failure is reported as StorageFailure`() {
        val repository = mockk<OrderRepository>()
        every { repository.findById(any()) } returns DomainError.OrderNotFound("ORD-9").left()
        every { repository.save(any()) } returns DomainError.StorageFailure("disk full").left()

        val result = ProcessOrderUseCase(repository).execute(command)

        assertInstanceOf(DomainError.StorageFailure::class.java, (result as Either.Left).value)
    }

    @Test
    fun `the order is only written once on the happy path`() {
        val repository = mockk<OrderRepository>()
        every { repository.findById(any()) } returns DomainError.OrderNotFound("ORD-9").left()
        every { repository.save(any()) } returns Unit.right()

        val result = ProcessOrderUseCase(repository).execute(command)

        val order = (result as Either.Right).value
        verify(exactly = 1) { repository.save(match<Order> { it.id == order.id }) }
    }
}
