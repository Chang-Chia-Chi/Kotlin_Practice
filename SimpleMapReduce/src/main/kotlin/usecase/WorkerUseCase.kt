package usecase

import adapter.reader.ReaderAdapter
import adapter.writer.WriterAdapter
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import model.CityModel
import model.ReportModel
import model.task.DoneTask
import model.task.ReadTask
import model.task.TaskModel

@ApplicationScoped
class WorkerUseCase(
    val reader: ReaderAdapter,
    val writer: WriterAdapter,
) {
    fun getNextTask(): TaskModel = DoneTask("0")

    suspend fun start() =
        flow {
            while (true) {
                when (getNextTask()) {
                    is ReadTask ->
                        reader
                            .read()
                            .map { convertCityToReport(it) }
                            .map { emit(it) }
                    is DoneTask -> return@flow
                }
            }
        }.flowOn(Dispatchers.IO)
            .collect { report ->
                writer.write(report.city, report)
            }

    private fun convertCityToReport(city: CityModel): ReportModel = ReportModel()
}
