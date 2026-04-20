package model

import io.vertx.sqlclient.Row
import org.jdbi.v3.core.mapper.reflect.ColumnName

data class Product(
    @ColumnName("id") val id: Int,
    @ColumnName("name") val name: String,
    @ColumnName("price") val price: Double,
) {
    companion object {
        fun fromRow(row: Row): Product =
            with(row) {
                Product(
                    id = getInteger("ID"),
                    name = getString("NAME"),
                    price = getDouble("PRICE"),
                )
            }
    }
}
