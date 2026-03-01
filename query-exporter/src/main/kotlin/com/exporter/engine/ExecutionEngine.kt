package com.exporter.engine

import com.exporter.config.ExporterConfig
import com.exporter.config.ResolvedQuery
import com.exporter.db.QueryExecutor
import com.exporter.metrics.MetricStateRegistry
import com.exporter.validation.ConfigValidationException
import com.exporter.validation.ConfigValidator
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.quarkus.scheduler.Scheduler
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.*
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Central orchestrator. On startup:
 * 1. Validates configuration (fail-fast).
 * 2. Registers scheduled jobs for each query.
 * 3. Each job fires a coroutine to execute the query and update metrics.
 *
 * On shutdown, cancels the coroutine scope to drain in-flight work.
 */
@ApplicationScoped
class ExecutionEngine(
    private val config: ExporterConfig,
    private val validator: ConfigValidator,
    private val scheduler: Scheduler,
    private val queryExecutor: QueryExecutor,
    private val metricRegistry: MetricStateRegistry,
) {

    private val log = Logger.getLogger(ExecutionEngine::class.java)

    private val scope = CoroutineScope(
        SupervisorJob() + Dispatchers.Default + CoroutineName("query-exporter")
    )

    private val runningJobs = ConcurrentHashMap<String, AtomicBoolean>()

    private var resolvedQueries: List<ResolvedQuery> = emptyList()

    fun onStart(@Observes event: StartupEvent) {
        log.info("Query Exporter starting...")

        // Phase 1: Validate
        try {
            resolvedQueries = validator.validate(config)
        } catch (e: ConfigValidationException) {
            log.fatal(e.message)
            throw e // Quarkus will abort startup
        }

        // Phase 2: Register scheduled jobs
        for (query in resolvedQueries) {
            registerJob(query)
        }

        log.infof("Query Exporter started: %d jobs registered", resolvedQueries.size)
    }

    fun onStop(@Observes event: ShutdownEvent) {
        log.info("Query Exporter shutting down, cancelling coroutine scope...")
        scope.cancel("Application shutting down")
    }

    private fun registerJob(query: ResolvedQuery) {
        val running = runningJobs.computeIfAbsent(query.name) { AtomicBoolean(false) }

        val jobBuilder = scheduler.newJob("query-exporter-${query.name}")
            .setTask { _ ->
                if (!running.compareAndSet(false, true)) {
                    log.warnf("Query '%s' still running from previous cycle, skipping", query.name)
                    return@setTask
                }
                // Fire-and-forget coroutine per execution cycle
                scope.launch {
                    try {
                        val job = QueryJob(query, queryExecutor, metricRegistry)
                        val startNs = System.nanoTime()
                        try {
                            val rowCount = job.execute()
                            val elapsedMs = (System.nanoTime() - startNs) / 1_000_000
                            log.debugf("Query '%s' completed: %d rows in %d ms",
                                query.name, rowCount, elapsedMs)
                        } catch (e: CancellationException) {
                            throw e // Don't swallow coroutine cancellation
                        } catch (e: Exception) {
                            log.errorf(e, "Unhandled error in query '%s'", query.name)
                        }
                    } finally {
                        running.set(false)
                    }
                }
            }

        val schedule = query.schedule
        if (schedule.interval != null) {
            jobBuilder.setInterval(schedule.interval.toString())
        } else if (schedule.cron != null) {
            jobBuilder.setCron(schedule.cron)
        }

        jobBuilder.schedule()
        log.infof("Scheduled query '%s' on datasource '%s' [%s]",
            query.name, query.datasource,
            schedule.interval?.let { "every $it" } ?: "cron: ${schedule.cron}")
    }

    /** Exposed for integration testing. */
    fun getResolvedQueries(): List<ResolvedQuery> = resolvedQueries
}
