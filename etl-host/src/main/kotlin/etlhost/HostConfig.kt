package etlhost

import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.Duration
import org.eclipse.microprofile.config.inject.ConfigProperty

/**
 * Every knob this host reads, in one place, so that "what is configuration and what is code" is
 * answerable by reading one file.
 *
 * **None of these values is a recommendation.** README.md says why at length; the short version is
 * that the operating point - how many tasks overlap, at what memory limits, in what pod - is a
 * statement about a memory request and a real schedule, and no test in this reactor is evidence
 * about it. What is fixed here is the *shape*: which settings exist, which two are load-bearing
 * anywhere (`quarkus.scheduler.start-mode` and `-Dkotlinx.coroutines.debug`), and which framework
 * parameter each one reaches.
 *
 * `@ConfigProperty` rather than `@ConfigMapping`: the mapping interface is nicer, and it would put
 * the defaults in a Kotlin interface where a deployment cannot see them. A host's config surface is
 * read by whoever writes the deployment, and `application.properties` beside a commented default is
 * where they look.
 */
@Singleton
class HostConfig {

    // ---- snapshot cache (spec 13) ----

    @ConfigProperty(name = "etl-host.cache.storage-path")
    lateinit var storagePath: Path

    @ConfigProperty(name = "etl-host.cache.temp-directory")
    lateinit var tempDirectory: Path

    /**
     * One entry per group: the group name, and the SQL that produces its single table.
     *
     * The map's keys are the groups this host serves - there is no second "which groups" setting
     * to disagree with it. The table inside a generation takes the group's name, so a `cacheCopy`
     * step reads `select ... from <group>`.
     *
     * **Each statement must project an `id` column.** `VerifyConfig.keyUnique` defaults to true and
     * the gate runs `COUNT(id), COUNT(DISTINCT id)` over every table of a candidate, so a group
     * without one fails its first refresh and the symptom surfaces two systems away as a failed
     * `cacheCopy`. Named here because this is the property whose value decides it.
     */
    @ConfigProperty(name = "etl-host.cache.sql")
    lateinit var groupSql: Map<String, String>

    @ConfigProperty(name = "etl-host.cache.serving-memory-limit")
    lateinit var servingMemoryLimit: String

    @ConfigProperty(name = "etl-host.cache.wait-budget")
    lateinit var waitBudget: Duration

    @ConfigProperty(name = "etl-host.cache.lease-deadline")
    lateinit var leaseDeadline: Duration

    /** Keep `terminationGracePeriodSeconds` above this plus headroom (snapshotcache spec 11.3). */
    @ConfigProperty(name = "etl-host.cache.lease-drain-timeout")
    lateinit var leaseDrainTimeout: Duration

    // ---- the source behind every group ----

    @ConfigProperty(name = "etl-host.source.url")
    lateinit var sourceUrl: String

    @ConfigProperty(name = "etl-host.source.username")
    lateinit var sourceUsername: java.util.Optional<String>

    @ConfigProperty(name = "etl-host.source.password")
    lateinit var sourcePassword: java.util.Optional<String>

    /**
     * Spec 8.6's timeout row, which is the host's and has no framework equivalent: `TaskEngine.run`
     * is a blocking call with no `Statement` handle to cancel, so a wedged driver parks a task's
     * dispatcher forever and every later firing is skipped in silence. Applied to every statement
     * this host's `Jdbi` instances issue.
     */
    @ConfigProperty(name = "etl-host.source.query-timeout-seconds")
    var queryTimeoutSeconds: Int = 0

    @ConfigProperty(name = "etl-host.source.fetch-size")
    var fetchSize: Int = 0

    // ---- SimpleEtl (spec 11.2) ----

    @ConfigProperty(name = "etl-host.etl.task-directory")
    lateinit var taskDirectory: Path

    @ConfigProperty(name = "etl-host.etl.scratch-directory")
    lateinit var scratchDirectory: Path

    @ConfigProperty(name = "etl-host.etl.scratch-memory-limit-mb")
    var scratchMemoryLimitMb: Int = 0

    /** The one non-scratch datasource the shipped task files name as a pipe target. */
    @ConfigProperty(name = "etl-host.etl.target-url")
    lateinit var targetUrl: String

    @ConfigProperty(name = "etl-host.etl.target-name")
    lateinit var targetName: String

    /**
     * The target's credentials, which existed nowhere until a real deployment needed them.
     *
     * The shipped demo target is a DuckDB file and DuckDB authenticates nobody, so `targetJdbi`
     * was written as `Jdbi.create(url)` with no user at all - and every test in this module and
     * the two before it points `report` at DuckDB, so nothing could notice. The staging stack
     * points it at Oracle, which is when the `pipe` step of the one worked task file failed with
     * `ORA-01017: invalid credential or not authorized`. Nullable, so a target that wants no
     * credentials keeps the `Jdbi.create(url)` it had.
     */
    @ConfigProperty(name = "etl-host.etl.target-username")
    lateinit var targetUsername: java.util.Optional<String>

    @ConfigProperty(name = "etl-host.etl.target-password")
    lateinit var targetPassword: java.util.Optional<String>

    // ---- the archive layer (snapshotcache spec 18) ----

    /**
     * Off by default, and that is a statement about prerequisites rather than about taste.
     *
     * The archive layer needs two things provisioned *ahead of the process* and creates neither:
     * a bucket ([ObjectStore][infra.snapshotarchive.ObjectStore]'s KDoc declines to auto-create
     * one, since a bucket made by whichever pod started first is exactly the ambient side effect
     * the layer's ordering guarantees exist to avoid) and the manifest table
     * (`ManifestSchema.DDL`, applied by the DBA). A host that flipped this on by default would
     * boot fine and then fail one run per hour against a bucket nobody made.
     */
    @ConfigProperty(name = "etl-host.archive.enabled")
    var archiveEnabled: Boolean = false

    @ConfigProperty(name = "etl-host.archive.endpoint")
    lateinit var archiveEndpoint: String

    /**
     * The bucket, which is one string used twice on purpose.
     *
     * `ObjectStore` writes into it and `ManifestDao` derives every row's `uri_prefix` from it, and
     * the two **must** be the same value: the DAO stores `"$bucket/snapshots/..."` and strips
     * exactly `"$bucket/"` back off to recover an object key. Two settings here would be two ways
     * to spell one fact, and a mismatch would surface as a key that resolves to nothing.
     */
    @ConfigProperty(name = "etl-host.archive.bucket")
    lateinit var archiveBucket: String

    @ConfigProperty(name = "etl-host.archive.access-key")
    lateinit var archiveAccessKey: String

    @ConfigProperty(name = "etl-host.archive.secret-key")
    lateinit var archiveSecretKey: String

    /**
     * Which tables each group archives, comma-separated - explicit, never discovered from the
     * snapshot's catalog, because D36 requires archived tables to have stable primary keys and
     * that is a property of the schema contract rather than of whatever happens to be attached.
     *
     * The keys must be group names `etl-host.cache.sql` also declares; a key that is not a group
     * is rejected at boot rather than failing one run an hour later.
     */
    @ConfigProperty(name = "etl-host.archive.tables")
    lateinit var archiveTables: Map<String, String>

    @ConfigProperty(name = "etl-host.archive.temp-directory")
    lateinit var archiveTempDirectory: Path

    /** How often a group is offered for archiving, and how often maintenance sweeps. */
    @ConfigProperty(name = "etl-host.archive.interval")
    lateinit var archiveInterval: Duration

    /** Spec 18.5's retention window. The newest COMPLETE version is never reclaimed, whatever this says. */
    @ConfigProperty(name = "etl-host.archive.retention")
    lateinit var archiveRetention: Duration

    /**
     * The MinIO client's own read timeout, and the reason it is configurable at all.
     *
     * `Archiver.close()` interrupts in-flight runs so the snapshot lease comes back inside the
     * cache's drain. A thread parked in a socket read is not interruptible, so a run stuck on a
     * slow link drains only when *this* timeout fires - which means a value above
     * `etl-host.cache.lease-drain-timeout` turns a graceful shutdown into an outstanding lease the
     * framework then warns about. Keep it below the drain timeout.
     */
    @ConfigProperty(name = "etl-host.archive.http-timeout")
    lateinit var archiveHttpTimeout: Duration
}
