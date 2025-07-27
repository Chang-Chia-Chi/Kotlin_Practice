package model

import io.vertx.sqlclient.Row
import org.jdbi.v3.core.mapper.reflect.ColumnName

data class SaleInfo(
    @ColumnName("id") val infoId: Int,
    @ColumnName("product_id") val productId: Int,
    @ColumnName("sale_unit") val saleUnit: Long = 0,
) {
    companion object {
        fun fromRow(row: Row): SaleInfo =
            with(row) {
                SaleInfo(
                    infoId = getInteger("ID"),
                    productId = getInteger("PRODUCT_ID"),
                    saleUnit = getLong("SALE_UNIT"),
                )
            }
    }
}
