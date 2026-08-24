package com.example.gauntlet.infrastructure

import org.jdbi.v3.sqlobject.customizer.Bind
import org.jdbi.v3.sqlobject.customizer.BindBean
import org.jdbi.v3.sqlobject.kotlin.RegisterKotlinMapper
import org.jdbi.v3.sqlobject.statement.SqlQuery
import org.jdbi.v3.sqlobject.statement.SqlUpdate

/**
 * SQLite 的資料列型別。刻意與 domain 的 Order 分開，
 * 這樣 DB schema 變動不會直接污染 domain。
 */
data class OrderRow(
    val id: String,
    val customerId: String,
    val amountCents: Long,
    val orderDate: String,
)

@RegisterKotlinMapper(OrderRow::class)
interface OrderDao {

    @SqlUpdate(
        """
        INSERT INTO orders (id, customer_id, amount_cents, order_date)
        VALUES (:id, :customerId, :amountCents, :orderDate)
        """,
    )
    fun insert(@BindBean row: OrderRow)

    @SqlQuery("SELECT id, customer_id, amount_cents, order_date FROM orders WHERE id = :id")
    fun findById(@Bind("id") id: String): OrderRow?

    @SqlQuery(
        """
        SELECT id, customer_id, amount_cents, order_date
        FROM orders
        WHERE order_date = :orderDate
        ORDER BY id
        """,
    )
    fun findByDate(@Bind("orderDate") orderDate: String): List<OrderRow>
}
