package etlhost

import infra.snapshotarchive.ArchiveMaintenance
import infra.snapshotarchive.Archiver
import infra.snapshotarchive.ManifestDao
import infra.snapshotarchive.ObjectStore
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.minio.MinioClient
import io.quarkus.scheduler.Scheduled
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Clock
import java.util.concurrent.Future
import org.jboss.logging.Logger
import org.jdbi.v3.core.Jdbi

/**
 * snapshotcache spec 18's archive layer, hosted - the half of M3 that was built and never run by
 * anything but its own tests.
 *
 * Four classes, none of which knows what a container is: an [ObjectStore] over a MinIO client, a
 * [ManifestDao] over a `Jdbi`, an [Archiver] that exports a leased snapshot to Parquet and uploads
 * it, and an [ArchiveMaintenance] that resolves crashed runs and enforces retention. Everything a
 * deployment has to decide - endpoint, bucket, credentials, which tables, how long to keep them -
 * arrives here as configuration and nowhere else.
 *
 * ### Why the archiver's own scheduler is never started
 *
 * Both `Archiver` and `ArchiveMaintenance` own a `ScheduledExecutorService` and a `start(interval)`
 * that uses it. Calling either would give this process **three** scheduling models - Quarkus's, the
 * archiver's and maintenance's - each with its own idea of what shutdown means, and a host whose
 * timing lives in three places is a host nobody can reason about at 3am. Both classes also expose a
 * synchronous tick (`submit`/`sweep`), so not starting them costs two idle threads and buys one
 * scheduler.
 *
 * That scheduler is Quarkus's, the same one [CacheTick] uses, driven from [tick] below. It is a
 * *second `@Scheduled` method*, not a second mechanism, and it is separate from [CacheTick] because
 * the two cadences are genuinely different - refresh is a ten-minute gap, archiving is hourly - and
 * folding one into the other would mean hand-rolled "every sixth tick" bookkeeping to save a
 * three-line method.
 *
 * `submit` rather than `runOnce`: `submit` queues onto the archiver's own bounded run pool, which
 * is the only path [Archiver.close] can interrupt. A `runOnce` called from the scheduler thread
 * would run outside anything shutdown can reach, and the lease it holds would outlive the drain.
 *
 * ### Why it is off unless configured
 *
 * The layer creates neither of its two prerequisites - the bucket, and the manifest table - by
 * design, both being things provisioned ahead of the process. `enabled=false` is therefore the
 * honest default: a host that flipped it on by default would boot green and fail one run an hour
 * against a bucket nobody made. See [HostConfig.archiveEnabled].
 *
 * ### Shutdown
 *
 * [close] runs from [EtlHost.onStop] **before `managed.close()`**, and that ordering is not
 * cosmetic: an archive run holds a snapshot lease for its whole export-upload-commit sequence, so
 * a cache that began draining first would be draining a lease whose holder nothing had told to
 * stop. It is not `@PreDestroy` for the same reason the rest of this host's shutdown is not - CDI
 * does not promise a destruction order, and this one is load-bearing.
 *
 * A leftover PENDING row after a killed run is deliberately not repaired here. The watchdog inside
 * [ArchiveMaintenance] owns that, so a crash and a clean shutdown converge on one recovery path
 * that is exercised every time either happens (D33).
 */
@Singleton
class ArchiveWiring @Inject constructor(
    config: HostConfig,
    managed: ManagedSnapshotCache,
    clock: Clock,
    /**
     * The manifest lives in the same database the groups are read from, which is a reference
     * host's simplification and not a rule: `ManifestDao` takes any `Jdbi`, and its SQL is Oracle
     * (`seq.NEXTVAL`, `FETCH FIRST`), so a deployment that keeps its manifest elsewhere points
     * this at that instead. It is the source `Jdbi` rather than a fifth one so the statement
     * timeout spec 8.6 requires already applies to it.
     */
    @param:Named(Producers.SOURCE) manifestJdbi: Jdbi,
) : AutoCloseable {

    private val enabled: Boolean = config.archiveEnabled

    val groups: List<GroupId> = if (!enabled) emptyList() else config.archiveTables.keys.map(::GroupId)

    private val archiver: Archiver?
    private val maintenance: ArchiveMaintenance?

    init {
        if (!enabled) {
            archiver = null
            maintenance = null
            log.info("archive layer disabled; no snapshot is checkpointed to object storage")
        } else {
            val tables = tablesFor(config.archiveTables, config.groupSql.keys)

            val client = MinioClient.builder()
                .endpoint(config.archiveEndpoint)
                .credentials(config.archiveAccessKey, config.archiveSecretKey)
                .build()
            // The one setting that decides whether shutdown is graceful. A socket read is not
            // interruptible, so a run parked in one drains when this fires and not when close()
            // asks - see HostConfig.archiveHttpTimeout.
            val timeout = config.archiveHttpTimeout.toMillis()
            client.setTimeout(timeout, timeout, timeout)

            val objects = ObjectStore(client, config.archiveBucket)
            val manifest = ManifestDao(manifestJdbi, config.archiveBucket, clock)

            archiver = Archiver(
                cache = managed.cache,
                manifest = manifest,
                objects = objects,
                tables = tables,
                tempRoot = Producers.mkdirs(config.archiveTempDirectory),
                // The archiver deletes tempRoot itself on close, so this directory is its own and
                // is shared with nothing.
                drainBudget = config.leaseDrainTimeout,
            )
            maintenance = ArchiveMaintenance(
                manifest = manifest,
                objects = objects,
                groups = tables.keys,
                clock = clock,
                retention = config.archiveRetention,
            )
            log.infov(
                "archive layer enabled: bucket {0} at {1}, groups {2}, retention {3}",
                config.archiveBucket, config.archiveEndpoint, tables.keys, config.archiveRetention,
            )
        }
    }

    /**
     * One archive round for every configured group, then one maintenance sweep.
     *
     * `delayed` for the same reason [CacheTick.tick] has it: Quarkus fires an `every` trigger when
     * the *scheduler* starts, not one interval later, so without it the first archive attempt races
     * the startup refresh and archives whatever generation happens to exist - or, with none yet,
     * spends the cache's whole wait budget on the scheduler thread before failing.
     */
    @Scheduled(
        every = "{etl-host.archive.interval}",
        delayed = "{etl-host.archive.interval}",
        concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
    )
    fun tick() {
        if (!enabled) return
        groups.forEach { submit(it) }
        sweep()
    }

    /**
     * Queues one run for [group]; null when the layer is off. Never throws - the archiver catches
     * its own failures, because a scheduled method that escapes with an exception is silently never
     * run again.
     */
    fun submit(group: GroupId): Future<*>? = archiver?.submit(group)

    /** One maintenance pass: watchdog, purge, staleness, per group. Idempotent by design. */
    fun sweep() = maintenance?.sweep()

    override fun close() {
        archiver?.let { a -> runCatching { a.close() }.onFailure { log.warn("archiver close failed", it) } }
        maintenance?.let { m -> runCatching { m.close() }.onFailure { log.warn("archive maintenance close failed", it) } }
    }

    companion object {

        /**
         * Parses `etl-host.archive.tables` and rejects a key that is not a group.
         *
         * A separate function so it is checkable without a MinIO client, an Oracle or a boot, which
         * is the only reason it is not three lines inside `init`. The check itself earns its keep:
         * unvalidated, a typo'd group name reaches `Archiver.publish`'s `requireNotNull(tables[g])`
         * and takes out **one run per hour**, logged as an archiver failure rather than as the
         * configuration mistake it is - a pod that looks healthy and quietly checkpoints nothing.
         */
        fun tablesFor(configured: Map<String, String>, groups: Set<String>): Map<GroupId, List<String>> {
            val unknown = configured.keys - groups
            require(unknown.isEmpty()) {
                "etl-host.archive.tables names $unknown, which are not groups; " +
                    "etl-host.cache.sql's keys are $groups"
            }
            return configured.entries.associate { (group, list) ->
                GroupId(group) to list.split(",").map(String::trim).filter(String::isNotEmpty)
            }
        }

        private val log: Logger = Logger.getLogger(ArchiveWiring::class.java)
    }
}
