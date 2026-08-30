package etlhost

import infra.etl.micrometer.MicrometerTaskMetrics
import infra.etl.task.CacheBinding
import infra.etl.task.EtlWiring
import infra.etl.task.TaskRunListener
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import infra.snapshotcache.bootstrap.openSnapshotCache
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.inject.Instance
import jakarta.enterprise.inject.Produces
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.SqlStatements

/**
 * The composition root, as CDI producers.
 *
 * **The documented order - cache first - is expressed as a dependency, not as a sequence.**
 * `composed-host-example`'s recipe step 1 says `openSnapshotCache` before `EtlWiring` because a
 * `CacheBinding` needs a live `SnapshotCache`. Written as two statements in a startup method that
 * would be a comment; written as [etlWiring] taking [ManagedSnapshotCache] as a parameter, the
 * container cannot construct them in the wrong order and no one has to remember. That is the whole
 * reason these are producers rather than a `HostBuilder`.
 *
 * Nothing here is `@ApplicationScoped`. `@Singleton` is not a normal scope, so it needs no client
 * proxy, which keeps a Kotlin class's finality irrelevant and keeps a stack trace through a
 * producer readable.
 *
 * **Nothing here closes anything.** Shutdown has a fixed order (`wired.close()` then
 * `managed.close()`, both after the readiness flag is raised) and `@Disposes` would run it in
 * whatever order the container tears beans down in. [EtlHost] owns the sequence.
 */
@Singleton
class Producers(private val config: HostConfig) {

    /**
     * The source behind every group, and spec 8.6's timeout row.
     *
     * `queryTimeout` is set on the `Jdbi` rather than on a pool because it is the only lever this
     * host has: `TaskEngine.run` is an ordinary blocking call with no `Statement` handle for anyone
     * to cancel, so a wedged driver otherwise parks a task's `limitedParallelism(1)` dispatcher
     * permanently - the task reads `busy` forever and every later firing is skipped as
     * `AlreadyRunning`, in silence. Surfaced as `SQLTimeoutException`, which SimpleEtl spec 5.3
     * already classes transient, so a step with `retries` retries rather than merely failing.
     */
    @Produces
    @Singleton
    @Named(SOURCE)
    fun sourceJdbi(): Jdbi = jdbi(config.sourceUrl, config.sourceUsername.orElse(null), config.sourcePassword.orElse(null))

    /**
     * The host's second, consumer-side datasource: what the shipped task files name as a pipe
     * target.
     *
     * A connection per handle, which is what a real target - Oracle, Postgres - wants and what
     * SimpleEtl spec 7.1's pool arithmetic assumes. The demo configuration points it at a DuckDB
     * file, where that is safe only because the handles do not overlap: a shape-D task's pipe step
     * *reads from scratch* and writes here, so it holds one connection from this datasource and not
     * the two that spec 7.1's same-datasource deadlock needs. A deployment that points two
     * overlapping tasks at one DuckDB file gets a file-lock failure, not a deadlock, and should
     * point them at a real database instead.
     */
    @Produces
    @Singleton
    @Named(TARGET)
    fun targetJdbi(): Jdbi =
        jdbi(config.targetUrl, config.targetUsername.orElse(null), config.targetPassword.orElse(null))

    @Produces
    @Singleton
    fun taskMetrics(registry: MeterRegistry): MicrometerTaskMetrics = MicrometerTaskMetrics(registry)

    /**
     * Both frameworks take `java.time.Clock` injected and forbid a custom time abstraction, so the
     * host supplies the one instance everything reads. A test that needs to move time constructs
     * the class under test with its own; nothing here reaches for `System.currentTimeMillis`.
     */
    @Produces
    @Singleton
    fun clock(): java.time.Clock = java.time.Clock.systemUTC()

    /**
     * snapshotcache spec 5.4's front door. Steps 1 and 2 of spec 10.1 - the stale-file wipe and the
     * serving instance's limits - happen inside this call; steps 3 to 5 are [EtlHost]'s.
     *
     * The map's keys are the groups this host serves, derived from the one `etl-host.cache.sql`
     * property, so there is no second list of group names to disagree with it.
     */
    @Produces
    @Singleton
    fun snapshotCache(events: CacheMetrics, @Named(SOURCE) source: Jdbi): ManagedSnapshotCache {
        val storage = mkdirs(config.storagePath)
        val temp = mkdirs(config.tempDirectory)
        return openSnapshotCache(
            config = SnapshotCacheConfig(
                storagePath = storage,
                tempDirectory = temp,
                servingMemoryLimit = config.servingMemoryLimit,
                defaultWaitBudget = config.waitBudget,
                leaseDeadline = config.leaseDeadline,
                leaseDrainTimeout = config.leaseDrainTimeout,
                jdbcFetchSize = config.fetchSize,
            ),
            sources = config.groupSql.entries.associate { (group, sql) ->
                GroupId(group) to JdbcGenerationSource(source, sql, group, config.fetchSize)
            },
            events = events,
        )
    }

    /**
     * SimpleEtl spec 11.2's front door, holding the cache the previous producer opened.
     *
     * Four of spec 8.6's rows are discharged by handing this class one of each thing rather than
     * two: one cache map, one datasource map, one `TaskHooks`, one `TaskRunListener`. The loader
     * and the engine below it cannot then disagree about a name.
     *
     * **`onTasksLoaded = metrics::seed` is the fifth**, and it is the only line in this host that
     * discharges spec 8.6's seeding row. That row used to say "call `seed` after the initial load
     * and after every reload", justified by the adapter boundary - `infra.etl.task` may not name
     * `io.micrometer`. A depth review refuted the justification the day this module was written:
     * *invoking a `(Set<String>) -> Unit` names nothing*, and the load/reload pairing is
     * `TaskAdmin`'s own moment, not a host's. So there is no `seed` call site anywhere below, and
     * `MetricsTest` would still pass if this host forgot one - because there is nothing left
     * for it to forget. That is what the row shrinking from a pairing to an argument bought.
     */
    @Produces
    @Singleton
    fun etlWiring(
        cache: ManagedSnapshotCache,
        cron: QuarkusCronScheduler,
        listeners: Instance<TaskRunListener>,
        metrics: MicrometerTaskMetrics,
        @Named(TARGET) target: Jdbi,
    ): EtlWiring = EtlWiring(
        scratchDirectory = mkdirs(config.scratchDirectory),
        cron = cron,
        datasources = mapOf(config.targetName to target),
        caches = config.groupSql.keys.associateWith { CacheBinding(cache.cache, GroupId(it)) },
        scratchMemoryLimitMb = config.scratchMemoryLimitMb,
        // Every TaskRunListener bean, composed. `TaskEngine` takes one listener, so spec 9.2's
        // "the host's existing in-house logging mechanism" plus anything else has nowhere to meet
        // except `of`, which also isolates them from each other exactly as the engine does.
        listener = TaskRunListener.of(*listeners.stream().toList().toTypedArray()),
        metrics = metrics,
        onTasksLoaded = metrics::seed,
    )

    private fun jdbi(url: String, user: String?, password: String?): Jdbi {
        val jdbi = if (user == null) Jdbi.create(url) else Jdbi.create(url, user, password)
        jdbi.configure(SqlStatements::class.java) { it.queryTimeout = config.queryTimeoutSeconds }
        return jdbi
    }

    companion object {
        const val SOURCE = "etl-host/source"
        const val TARGET = "etl-host/target"

        fun mkdirs(path: Path): Path = path.also { Files.createDirectories(it) }

        /** Opens a DuckDB file connection the way both frameworks do, for tests and for fixtures. */
        fun duckDb(url: String): Connection = DriverManager.getConnection(url)
    }
}
