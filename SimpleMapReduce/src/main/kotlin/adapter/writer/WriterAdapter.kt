package adapter.writer

import model.ReportModel
import repo.AvroRepo
import usecase.io.DataWriter
import java.io.File
import java.util.concurrent.ConcurrentHashMap

class WriterAdapter(
    val repo: AvroRepo,
    override val fileMap: ConcurrentHashMap<String, File>,
) : DataWriter<ReportModel> {
    override fun open(name: String): String = name

    override fun close(name: String) {
        TODO("Not yet implemented")
    }

    override fun write(
        name: String,
        obj: ReportModel,
    ) {
        TODO("Not yet implemented")
    }

    override fun flush() {
        TODO("Not yet implemented")
    }
}
