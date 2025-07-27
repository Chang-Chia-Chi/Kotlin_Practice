import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.mutiny.core.Vertx
import kotlinx.coroutines.runBlocking
import service.EtlService

@QuarkusMain
class Main(
    private val vertx: Vertx,
    private val etlService: EtlService,
) : QuarkusApplication {
    override fun run(vararg args: String?): Int {
        runBlocking(vertx.delegate.dispatcher()) {
            etlService.run(args[0]!!)
        }
        return 0
    }
}
