package repo

import io.vertx.kotlin.coroutines.coAwait
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Tuple
import jakarta.enterprise.context.ApplicationScoped
import model.Product
import model.SaleInfo

@ApplicationScoped
class OracleRepo(
    private val pool: Pool,
) {
    suspend fun querySaleInfo(product: Product): SaleInfo? {
        val sql =
            """
            SELECT * FROM SALE_INFO
             WHERE PRODUCT_ID = ?
            """.trimIndent()

        return pool
            .preparedQuery(sql)
            .execute(Tuple.of(product.productId))
            .coAwait()
            .map(SaleInfo::fromRow)
            .firstOrNull()
    }
}
