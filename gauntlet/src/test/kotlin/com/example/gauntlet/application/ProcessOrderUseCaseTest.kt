package com.example.gauntlet.application

import arrow.core.Either
import com.example.gauntlet.TestDatabases
import com.example.gauntlet.domain.DomainError
import com.example.gauntlet.infrastructure.SqliteOrderRepository
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.time.LocalDate

/**
 * 走真實 SQLite 的路徑。這個 class 碰得到 JDBI，所以不准出現 mock
 * （由 MockBoundaryTest 強制）。逼不出來的失敗路徑放到
 * [com.example.gauntlet.application.ProcessOrderFailurePathTest]。
 */
class ProcessOrderUseCaseTest {

    private lateinit var dir: Path
    private lateinit var useCase: ProcessOrderUseCase
    private val date: LocalDate = LocalDate.of(2026, 8, 24)

    @BeforeEach
    fun setUp() {
        dir = TestDatabases.newSqliteDir()
        useCase = ProcessOrderUseCase(SqliteOrderRepository(TestDatabases.newSqlite(dir)))
    }

    @AfterEach
    fun tearDown() {
        TestDatabases.deleteRecursively(dir)
    }

    private fun command(id: String, amount: Long = 500L) =
        NewOrderCommand(id = id, customerId = "CUST-1", amountCents = amount, orderDate = date)

    @Test
    fun `stores a valid order and reads it back`() {
        assertTrue(useCase.execute(command("ORD-1")).isRight())

        val found = (useCase.findById("ORD-1") as Either.Right).value
        assertEquals("CUST-1", found.customerId)
        assertEquals(500L, found.amountCents)
        assertEquals(date, found.orderDate)
    }

    @Test
    fun `rejects the same id twice`() {
        assertTrue(useCase.execute(command("ORD-2")).isRight())

        val second = useCase.execute(command("ORD-2", amount = 999L))
        assertInstanceOf(DomainError.DuplicateOrder::class.java, (second as Either.Left).value)
    }

    @Test
    fun `invalid input never reaches the database`() {
        val result = useCase.execute(command("ORD-3", amount = -1L))
        assertInstanceOf(DomainError.InvalidAmount::class.java, (result as Either.Left).value)
        assertInstanceOf(
            DomainError.OrderNotFound::class.java,
            (useCase.findById("ORD-3") as Either.Left).value,
        )
    }

    @Test
    fun `missing order id is a validation error not a lookup`() {
        assertInstanceOf(
            DomainError.InvalidOrderId::class.java,
            (useCase.findById("  ") as Either.Left).value,
        )
    }

    @Test
    fun `unknown id yields OrderNotFound`() {
        assertInstanceOf(
            DomainError.OrderNotFound::class.java,
            (useCase.findById("nope") as Either.Left).value,
        )
    }
}
