package com.example.gauntlet.infrastructure

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.example.gauntlet.domain.DomainError
import com.example.gauntlet.domain.Order
import com.example.gauntlet.domain.OrderRepository
import org.jdbi.v3.core.Jdbi
import java.time.LocalDate

/**
 * OLTP 落地。所有 Handle 都走 withHandle / useHandle，
 * 保證原生檔案句柄一定會關，不靠人記得。
 */
class SqliteOrderRepository(private val jdbi: Jdbi) : OrderRepository {

    override fun save(order: Order): Either<DomainError, Unit> = guard {
        jdbi.useExtension<OrderDao, Exception>(OrderDao::class.java) { dao ->
            dao.insert(
                OrderRow(
                    id = order.id,
                    customerId = order.customerId,
                    amountCents = order.amountCents,
                    orderDate = order.orderDate.toString(),
                ),
            )
        }
    }

    override fun findById(id: String): Either<DomainError, Order> = either {
        val row = guard {
            jdbi.withExtension<OrderRow?, OrderDao, Exception>(OrderDao::class.java) { dao ->
                dao.findById(id)
            }
        }.bind() ?: raise(DomainError.OrderNotFound(id))
        toDomain(row).bind()
    }

    override fun findByDate(date: LocalDate): Either<DomainError, List<Order>> = either {
        val rows = guard {
            jdbi.withExtension<List<OrderRow>, OrderDao, Exception>(OrderDao::class.java) { dao ->
                dao.findByDate(date.toString())
            }
        }.bind()
        rows.map { toDomain(it).bind() }
    }

    private fun toDomain(row: OrderRow): Either<DomainError, Order> =
        Order.create(
            id = row.id,
            customerId = row.customerId,
            amountCents = row.amountCents,
            orderDate = parseDate(row.orderDate),
        )

    private fun parseDate(raw: String): LocalDate? =
        runCatching { LocalDate.parse(raw) }.getOrNull()

    /** 把 JDBC/JDBI 的例外收斂成 DomainError，不讓它漏到 application 層。 */
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
