import io.quarkus.logging.Log
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.vertx.mutiny.core.Vertx
import jakarta.inject.Inject
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import model.Product
import model.ProductSaleInfo
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import repo.DuckDBRepo
import service.EtlService
import java.nio.file.Files
import java.nio.file.Path
import kotlin.system.measureTimeMillis

@QuarkusTest
@QuarkusTestResource(SetupTestContainers::class)
class EtlServiceTest {
    @Inject
    private lateinit var etlService: EtlService

    @Inject
    private lateinit var duckDBRepo: DuckDBRepo

    @Inject
    private lateinit var vertx: Vertx

    private var parquetPath: String? = null

    @BeforeEach
    fun setUp() {
        generateParquetInput()
    }

    private fun generateParquetInput() =
        runBlocking {
            duckDBRepo.createTableFromDataClass("product", Product::class)

            val products: MutableList<Product> = ArrayList(100000)
            for (i in 1..100000) {
                products.add(Product(i, "Product-$i", i * 1.0))
            }
            duckDBRepo.writeTo("product", products)

            val temp = Files.createTempFile("products", ".parquet")
            parquetPath =
                duckDBRepo
                    .copyTo("product", "parquet", temp.toAbsolutePath().toString())
                    .toString()
        }

    @Test
    @Throws(Exception::class)
    fun runEtl_and_verifyOutputParquet() =
        runBlocking {
            var path: Path
            val executionTime =
                measureTimeMillis {
                    path = etlService.run(parquetPath!!)
                }
            Log.info("Time take: ${executionTime}ms")

            val results: List<ProductSaleInfo> =
                duckDBRepo
                    .readParquetAsFlow<ProductSaleInfo>(path.toAbsolutePath().toString())
                    .toList()

            Assertions.assertEquals(100000, results.size)
        }
}
