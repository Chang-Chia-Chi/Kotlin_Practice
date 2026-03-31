package com.workflow.infrastructure.queryexporter.core

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import com.workflow.infrastructure.queryexporter.config.ExporterConfig
import com.workflow.infrastructure.queryexporter.config.QueryConfig
import com.workflow.infrastructure.queryexporter.config.ScheduleConfig
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.jboss.logging.Logger
import java.time.Clock
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class QueryScheduler(
    private val executor: QueryExecutor,
    private val writer: MetricWriter,
    private val dataSourceProvider: DataSourceProvider,
    private val leaderGuard: LeaderGuard,
    private val clock: Clock = Clock.systemUTC(),
    private val ioContext: CoroutineContext = EmptyCoroutineContext,
) {
    private val log = Logger.getLogger(QueryScheduler::class.java)

    private var queryJobs = listOf<Job>()
    private val monitorJobRef = AtomicReference<Job?>(null)

    fun start(config: ExporterConfig, scope: CoroutineScope) {
        val job = scope.launch {
            var wasLeader = false
            leaderGuard.leaderState.collect { isLeader ->
                if (isLeader && !wasLeader) {
                    log.info("Became leader — starting query loops")
                    queryJobs = config.queries.map { (name, queryConfig) ->
                        scope.launch { runQueryLoop(name, queryConfig) }
                    }
                } else if (!isLeader && wasLeader) {
                    log.info("Lost leadership — cancelling query loops")
                    queryJobs.forEach { it.cancelAndJoin() }
                    queryJobs = emptyList()
                }
                wasLeader = isLeader
            }
        }
        if (!monitorJobRef.compareAndSet(null, job)) {
            job.cancel()
            return
        }
    }

    private suspend fun runQueryLoop(name: String, config: QueryConfig) {
        val ds = dataSourceProvider.resolve(config.datasource)
        while (true) {
            try {
                val rows = if (ioContext === EmptyCoroutineContext) {
                    executor.execute(ds, config.sql)
                } else {
                    withContext(ioContext) { executor.execute(ds, config.sql) }
                }
                config.metrics.forEach { metric -> writer.write(metric, rows) }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warnf(e, "Query '%s' failed, will retry next cycle", name)
            }

            delayUntilNextExecution(config.schedule)
        }
    }

    private suspend fun delayUntilNextExecution(schedule: ScheduleConfig) {
        if (schedule.interval != null) {
            delay(schedule.interval.toMillis())
        } else {
            val cronDef = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
            val cron = CronParser(cronDef).parse(schedule.cron!!)
            val executionTime = ExecutionTime.forCron(cron)
            val next = executionTime.nextExecution(ZonedDateTime.now(clock))
            if (next.isPresent) {
                val delayMs = Duration.between(ZonedDateTime.now(clock), next.get()).toMillis()
                if (delayMs > 0) delay(delayMs)
            }
        }
    }

    suspend fun stop(timeout: Duration = Duration.ofSeconds(5)) {
        queryJobs.forEach { it.cancel() }
        val monitorJob = monitorJobRef.getAndSet(null)
        monitorJob?.cancel()
        withTimeoutOrNull(timeout.toMillis()) {
            queryJobs.forEach { it.join() }
            monitorJob?.join()
        }
        queryJobs = emptyList()
    }
}
