package usecase

import model.ReportModel
import usecase.io.DataWriter

class DataWriterUseCase(
    private val writer: DataWriter<ReportModel>,
    private val coordinator: CoordinatorUseCase,
) {
    var key = ""

    suspend fun run() {
        writer.open(key)
        for (report in coordinator.get(key)) {
            writer.write(key, report)
        }
        writer.close(key)
    }
}
