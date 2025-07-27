package model

import io.vertx.sqlclient.Row
import org.jdbi.v3.core.mapper.reflect.ColumnName

data class Product(
    @ColumnName("id") val productId: Int,
    @ColumnName("product_name") val productName: String,
    @ColumnName("price") val price: Double,
) {
    companion object {
        fun fromRow(row: Row): Product =
            with(row) {
                Product(
                    productId = getInteger("ID"),
                    productName = getString("PRODUCT_NAME"),
                    price = getDouble("PRICE"),
                )
            }
    }
}
