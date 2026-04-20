package usecase.config

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class JobConfig {
    @ConfigProperty(name = "worker.count")
    private lateinit var workers: String

    val workerCount: Int by lazy { workers.toInt() }
}
