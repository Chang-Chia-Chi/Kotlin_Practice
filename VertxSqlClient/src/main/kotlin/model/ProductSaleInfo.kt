package model

import org.jdbi.v3.core.mapper.reflect.ColumnName

data class ProductSaleInfo(
    @ColumnName("id") val id: Int,
    @ColumnName("name") val name: String,
    @ColumnName("total") val total: Double,
)
