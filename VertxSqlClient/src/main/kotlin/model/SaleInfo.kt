package model

import io.vertx.sqlclient.Row
import org.jdbi.v3.core.mapper.reflect.ColumnName

data class SaleInfo(
    @ColumnName("product_id") val productId: Int,
    @ColumnName("sale_unit") val saleUnit: Long = 0,
) {
    companion object {
        fun fromRow(row: Row): SaleInfo =
            with(row) {
                SaleInfo(
                    productId = getInteger("PRODUCT_ID"),
                    saleUnit = getLong("SALE_UNIT"),
                )
            }
    }
}
