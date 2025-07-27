package service

import extension.unorderedMapAsync
import io.quarkus.logging.Log
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.flow.filterNotNull
import model.Product
import model.ProductSaleInfo
import repo.DuckDBRepo
import repo.OracleRepo
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

@ApplicationScoped
class EtlService(
    private val config: EtlConfig,
    private val duckDBRepo: DuckDBRepo,
    private val oracleRepo: OracleRepo,
) {
    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun run(parquet: String): Path {
        val counter = AtomicInteger(0)
        duckDBRepo.createTableFromDataClass("product_sale_info", ProductSaleInfo::class)
        duckDBRepo
            .readParquetAsFlow<Product>(parquet)
            .buffer(4000)
            .unorderedMapAsync(32) { product ->
                val info = oracleRepo.querySaleInfo(product) ?: return@unorderedMapAsync null
                ProductSaleInfo(
                    id = product.id,
                    name = product.name,
                    total = product.price * info.saleUnit,
                )
            }.filterNotNull()
            .chunked(2000)
            .collect { chunk ->
                duckDBRepo.writeTo("product_sale_info", chunk)
                Log.info("Current Row Processed: ${counter.addAndGet(chunk.size)}")
            }

        return duckDBRepo.copyTo("product_sale_info", "parquet")
    }
}
