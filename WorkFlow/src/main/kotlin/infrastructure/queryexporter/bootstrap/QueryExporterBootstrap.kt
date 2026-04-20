package com.workflow.infrastructure.queryexporter.bootstrap

import com.workflow.infrastructure.queryexporter.config.ExporterConfig
import com.workflow.infrastructure.queryexporter.config.ExporterConfigValidator
import com.workflow.infrastructure.queryexporter.core.MetricWriter
import com.workflow.infrastructure.queryexporter.core.QueryExecutor
import com.workflow.infrastructure.queryexporter.core.QueryScheduler
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import java.time.Clock

class QueryExporterBootstrap(
    private val config: ExporterConfig,
    private val dataSourceProvider: DataSourceProvider,
    private val meterRegistry: MeterRegistry,
    private val leaderGuard: LeaderGuard = LeaderGuard.ALWAYS,
    private val clock: Clock = Clock.systemUTC(),
) {
    private lateinit var scheduler: QueryScheduler
    private lateinit var writer: MetricWriter
    private lateinit var scope: CoroutineScope

    fun start(): QueryScheduler {
        ExporterConfigValidator.validate(config)

        val executor = QueryExecutor()
        writer = MetricWriter(meterRegistry)
        scheduler = QueryScheduler(executor, writer, dataSourceProvider, leaderGuard, clock)
        scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

        scheduler.start(config, scope)
        return scheduler
    }

    suspend fun stop() {
        if (::scheduler.isInitialized) {
            scheduler.stop()
        }
        if (::writer.isInitialized) {
            writer.close()
        }
        if (::scope.isInitialized) {
            scope.cancel()
        }
    }
}
