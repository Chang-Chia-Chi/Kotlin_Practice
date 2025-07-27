package service

import extension.unorderedMapAsync
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.flow.filterNotNull
import model.Product
import model.ProductSaleInfo
import repo.DuckDBRepo
import repo.OracleRepo

@ApplicationScoped
class EtlService(
    private val config: EtlConfig,
    private val duckDBRepo: DuckDBRepo,
    private val oracleRepo: OracleRepo,
    private val s3Service: S3Service,
) {
    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun run(parquet: String) {
        duckDBRepo.createTableFromDataClass("product_sale_info", ProductSaleInfo::class)
        duckDBRepo
            .readParquetAsFlow<Product>(parquet)
            .buffer(4000)
            .unorderedMapAsync(32) { product ->
                val info = oracleRepo.querySaleInfo(product) ?: return@unorderedMapAsync null
                ProductSaleInfo(
                    productId = product.productId,
                    productName = product.productName,
                    total = product.price * info.saleUnit,
                )
            }.filterNotNull()
            .chunked(2000)
            .collect { chunk ->
                duckDBRepo.writeTo("product_sale_info", chunk)
            }

        val file = duckDBRepo.copyTo("product_sale_info", "parquet")
        s3Service.upload(file.toUri().path, config.s3BucketName(), config.s3Prefix())
    }
}
