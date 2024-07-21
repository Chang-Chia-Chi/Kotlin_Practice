import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.enterprise.context.ApplicationScoped
import usecase.CoordinatorUseCase
import usecase.config.JobConfig

@QuarkusMain
@ApplicationScoped
class Main(
    val coordinatorUseCase: CoordinatorUseCase,
    val jobConfig: JobConfig,
) : QuarkusApplication {
    override fun run(vararg args: String?): Int {
        repeat(jobConfig.writerCount) {
        }
        return 0
    }
}
