import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import kotlinx.coroutines.runBlocking
import service.EtlService

@QuarkusMain
class Main(
    private val etlService: EtlService,
) : QuarkusApplication {
    override fun run(vararg args: String?): Int {
        runBlocking {
            etlService.run(args[0]!!)
        }
        return 0
    }
}
