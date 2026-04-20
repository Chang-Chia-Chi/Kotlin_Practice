package com.workflow.infrastructure.queryexporter.bootstrap

import com.workflow.infrastructure.queryexporter.config.ExporterConfig
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.infrastructure.shutdown.ShutdownParticipant
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration

@ApplicationScoped
class QueryExporterLifecycle(
    private val dataSourceProvider: DataSourceProvider,
    private val meterRegistry: MeterRegistry,
    private val leaderGuard: LeaderGuard,
) : ShutdownParticipant {
    private val log = LoggerFactory.getLogger(QueryExporterLifecycle::class.java)

    private var bootstrap: QueryExporterBootstrap? = null

    fun onStart(@Observes ev: StartupEvent) {
        val stream = Thread.currentThread().contextClassLoader.getResourceAsStream(CONFIG_RESOURCE)
        if (stream == null) {
            log.warn("Config resource '{}' not found on classpath — query exporter disabled", CONFIG_RESOURCE)
            return
        }
        try {
            val config = stream.use { ExporterConfig.load(it) }
            val bs = QueryExporterBootstrap(config, dataSourceProvider, meterRegistry, leaderGuard)
            bs.start()
            bootstrap = bs
            log.info("Query exporter started with {} queries", config.queries.size)
        } catch (e: Exception) {
            log.warn("Failed to start query exporter — continuing without metrics export", e)
        }
    }

    override val shutdownOrder: Int = 10
    override val shutdownTimeout: Duration = Duration.ofSeconds(5)

    override suspend fun shutdown() {
        bootstrap?.stop()
    }

    companion object {
        const val CONFIG_RESOURCE = "query-exporter.yaml"
    }
}
