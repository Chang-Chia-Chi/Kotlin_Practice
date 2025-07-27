package model

import org.jdbi.v3.core.mapper.reflect.ColumnName

data class ProductSaleInfo(
    @ColumnName("id") val productId: Int,
    @ColumnName("name") val productName: String,
    @ColumnName("total") val total: Double,
)
