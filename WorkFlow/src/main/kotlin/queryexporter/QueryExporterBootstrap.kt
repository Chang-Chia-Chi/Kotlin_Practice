package com.workflow.queryexporter

import com.workflow.queryexporter.config.ExporterConfig
import com.workflow.queryexporter.config.ExporterConfigValidator
import com.workflow.queryexporter.core.MetricWriter
import com.workflow.queryexporter.core.QueryExecutor
import com.workflow.queryexporter.core.QueryScheduler
import com.workflow.queryexporter.spi.DataSourceProvider
import com.workflow.queryexporter.spi.LeaderGuard
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
