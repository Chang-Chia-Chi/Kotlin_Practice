package usecase.config

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class JobConfig {
    @ConfigProperty(name = "worker.reader.count")
    private lateinit var readers: String

    @ConfigProperty(name = "worker.writer.count")
    private lateinit var writers: String

    val readerCount: Int by lazy { readers.toInt() }
    val writerCount: Int by lazy { writers.toInt() }
}
