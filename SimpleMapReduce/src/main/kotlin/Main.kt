import arrow.fx.coroutines.parMap
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flowOf
import usecase.WorkerUseCase
import usecase.config.JobConfig

@QuarkusMain
@ApplicationScoped
class Main(
    val workerUseCase: WorkerUseCase,
    val jobConfig: JobConfig,
) : QuarkusApplication {
    @OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
    override fun run(vararg args: String?): Int {
        flowOf(1..jobConfig.workerCount).parMap {
            workerUseCase.start()
        }
        return 0
    }
}
