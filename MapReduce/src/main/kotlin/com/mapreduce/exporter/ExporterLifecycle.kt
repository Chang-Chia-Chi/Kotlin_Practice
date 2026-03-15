package com.mapreduce.exporter

import io.agroal.api.AgroalDataSource
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.arc.Arc
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.quarkus.scheduler.Scheduler
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Default
import jakarta.interceptor.Interceptor
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Lifecycle orchestrator for the Query Exporter.
 *
 * Startup sequence (high-to-low priority):
 * 1. Validate all config — fail-fast with accumulated errors
 * 2. Build JDBI instance cache (one per datasource)
 * 3. Register meta-metrics (present even with zero queries)
 * 4. Wire Quarkus Scheduler jobs → coroutine bridge
 *
 * Shutdown: cancel the [CoroutineScope], which cooperatively cancels in-flight queries.
 * JDBI handles and scheduler jobs are cleaned up by Quarkus lifecycle automatically.
 */
@ApplicationScoped
class ExporterLifecycle(
    private val config: ExporterConfig,
    private val validator: ExporterValidator,
    private val metricBridge: MetricBridge,
    private val meterRegistry: MeterRegistry,
    private val scheduler: Scheduler,
) {

    private val log = Logger.getLogger(ExporterLifecycle::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val jdbiCache = ConcurrentHashMap<String, Jdbi>()

    private lateinit var ioDispatcher: CoroutineDispatcher
    private lateinit var queryExecutor: QueryExecutor

    @OptIn(ExperimentalCoroutinesApi::class)
    fun onStart(@Observes @Priority(Interceptor.Priority.APPLICATION) ev: StartupEvent) {
        val queries = config.queries()
        if (queries.isEmpty()) {
            log.info("Query exporter: no queries configured — meta-metrics only")
            initExecutor()
            queryExecutor.registerMetaMetrics(emptyList())
            return
        }

        // Step 1: Validate and resolve config (throws StartupException on any error)
        val resolved = validator.validateAndResolve(config)

        // Step 2: Set up JDBI cache per datasource
        val datasources = resolved.map { it.datasource }.distinct()
        for (dsName in datasources) {
            val dataSource = resolveDataSource(dsName)
            jdbiCache[dsName] = Jdbi.create(dataSource).apply {
                installPlugin(KotlinPlugin())
            }
        }

        // Step 3: Configure cardinality limit and IO dispatcher
        metricBridge.cardinalityLimit = config.cardinalityLimit()
        initExecutor()

        // Step 4: Register meta-metrics
        queryExecutor.registerMetaMetrics(resolved)

        // Step 5: Schedule queries via Quarkus Scheduler → coroutine bridge
        for (query in resolved) {
            val jdbi = jdbiCache[query.datasource]
                ?: throw StartupException("JDBI instance not found for datasource '${query.datasource}'")

            val jobBuilder = scheduler.newJob("qe-${query.name}")

            if (query.intervalSeconds != null) {
                jobBuilder.setInterval("${query.intervalSeconds}s")
            } else if (query.cron != null) {
                jobBuilder.setCron(query.cron)
            }

            jobBuilder.setTask { _ ->
                scope.launch {
                    queryExecutor.execute(query, jdbi)
                }
            }

            jobBuilder.schedule()
            log.infof("Scheduled query '%s' [datasource=%s, %s]",
                query.name, query.datasource,
                if (query.intervalSeconds != null) "interval=${query.intervalSeconds}s" else "cron=${query.cron}")
        }

        log.infof("Query exporter started: %d queries across %d datasource(s)",
            resolved.size, datasources.size)
    }

    fun onStop(@Observes ev: ShutdownEvent) {
        scope.cancel()
        log.info("Query exporter stopped — coroutine scope cancelled")
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun initExecutor() {
        ioDispatcher = Dispatchers.IO.limitedParallelism(config.ioParallelism())
        queryExecutor = QueryExecutor(metricBridge, meterRegistry, ioDispatcher)
    }

    private fun resolveDataSource(name: String): AgroalDataSource {
        if (name == "<default>") {
            val container = Arc.container()
                ?: throw StartupException("CDI container not available")
            val handle = container.select(AgroalDataSource::class.java, Default.Literal.INSTANCE)
            if (!handle.isResolvable) {
                throw StartupException("Default datasource is not resolvable in CDI container")
            }
            return handle.get()
        }

        return ExporterValidator.findNamedDataSource(name)
            ?: throw StartupException("Datasource '$name' is not resolvable in CDI container")
    }
}
