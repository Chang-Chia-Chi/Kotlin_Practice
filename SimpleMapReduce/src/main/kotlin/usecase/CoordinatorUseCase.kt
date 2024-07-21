package usecase

import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.channels.Channel
import model.ReportModel
import model.task.DoneTask
import model.task.TaskModel
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class CoordinatorUseCase {
    private val iterMap = ConcurrentHashMap<String, Channel<ReportModel>>()

    fun hasNextTask(): Boolean = true

    fun getNextTask(): TaskModel = DoneTask("0")

    suspend fun put(
        key: String,
        report: ReportModel,
    ) = get(key).send(report)

    fun get(key: String): Channel<ReportModel> = iterMap.getOrPut(key) { Channel() }
}
