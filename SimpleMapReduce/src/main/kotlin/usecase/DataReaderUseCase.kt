package usecase

import model.CityModel
import model.ReportModel
import model.task.DoneTask
import model.task.ReadTask
import usecase.io.DataReader

class DataReaderUseCase(
    val reader: DataReader<CityModel>,
    val coordinator: CoordinatorUseCase,
) {
    suspend fun run() {
        while (true) {
            when (coordinator.getNextTask()) {
                is ReadTask ->
                    reader
                        .read()
                        .map { convertCityToReport(it) }
                        .map { coordinator.put(it.key(), it) }
                is DoneTask -> return
            }
        }
    }

    private fun convertCityToReport(city: CityModel): ReportModel = ReportModel()
}
