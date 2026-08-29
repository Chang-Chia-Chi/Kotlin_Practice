package infra.snapshotarchive

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCache
import org.jboss.logging.Logger
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** One row of the spec 18.2 inventory: what was exported, and how a reader recognises it again. */
data class ArchivedObject @JsonCreator constructor(
    @param:JsonProperty("table") @get:JsonProperty("table") val table: String,
    @param:JsonProperty("object_key") @get:JsonProperty("object_key") val objectKey: String,
    @param:JsonProperty("bytes") @get:JsonProperty("bytes") val bytes: Long,
    @param:JsonProperty("checksum") @get:JsonProperty("checksum") val checksum: String,
    @param:JsonProperty("row_count") @get:JsonProperty("row_count") val rowCount: Long,
)

/**
 * The inventory CLOB of spec 18.2, which is the manifest row's whole reason for being
 * written before the first upload: it is the complete list of what a version must contain,
 * so nothing downstream ever has to LIST the bucket to find out (D33).
 *
 * `object_key` is relative to the row's `uri_prefix`; the absolute layout lives in
 * [ManifestSchema]/[ManifestDao] and is not duplicated here.
 */
object Inventory {

    private val json = ObjectMapper()

    fun encode(objects: List<ArchivedObject>): String = json.writeValueAsString(objects)

    fun decode(inventory: String): List<ArchivedObject> =
        json.readValue(inventory, object : TypeReference<List<ArchivedObject>>() {})
}

/**
 * Interleaving points between the spec 18.3 steps, for the crash-matrix and serialization
 * tests. Production passes the no-op, so these cost one virtual call - the same bargain the
 * framework's own [infra.snapshotcache.api.Hook] makes (spec 17.4).
 *
 * These are deliberately a separate enum rather than new [infra.snapshotcache.api.Hook]
 * values: that enum is frozen, and the archive layer is a consumer, not part of the
 * framework (D30).
 */
enum class ArchiveStep {
    /** Every table exported and the inventory computed; no manifest row exists yet. */
    AFTER_EXPORT,

    /** PENDING row committed; not one object uploaded. */
    AFTER_PENDING_ROW,

    /** About to upload one object. Fires once per object, so a mid-upload crash is reachable. */
    BEFORE_EACH_UPLOAD,

    /** Every object uploaded and verified; the row is still PENDING. */
    AFTER_UPLOAD,

    /** The conditional flip has run; the lease is still held and the temp dir still exists. */
    AFTER_COMPLETE,
}

/** Why a run ended. Anything worse than these propagates as an exception. */
enum class RunOutcome {
    PUBLISHED,

    /** The group was already running. Runs are not queued (spec 18.2). */
    SKIPPED_BUSY,

    /** The D31 monotonicity guard refused the publish; skipped and alerted (spec 18.3 step 2). */
    SKIPPED_NOT_NEWER,

    /** The conditional flip moved nothing - the ticket-04 watchdog resolved the row first (D33). */
    LOST_RACE,
}

/**
 * The hourly archiver of spec 18.2/18.3.
 *
 * The step order in [publish] is the entire safety argument and is fixed: the PENDING row,
 * carrying the complete inventory, is committed before the first object is uploaded, so an
 * object without a covering manifest row is impossible and this layer owns no LIST-based
 * orphan sweep (D33). Nothing here is clever; everything here is ordered.
 *
 * [tables] declares which tables each group archives. It is explicit rather than discovered
 * from the snapshot's catalog because D36 requires archived tables to have stable primary
 * keys, which is a property of the schema contract, not of whatever happens to be attached.
 *
 * Scheduling is dull on purpose: different groups run in parallel on a bounded pool, the
 * same group never runs twice at once, and a run that finds its group busy skips and logs
 * rather than queueing - a queued hourly run would still be exporting yesterday's snapshot.
 */
class Archiver(
    private val cache: SnapshotCache,
    private val manifest: ManifestDao,
    private val objects: ObjectStore,
    private val tables: Map<GroupId, List<String>>,
    private val tempRoot: Path,
    private val drainBudget: Duration = Duration.ofSeconds(30),
    exportParallelism: Int = 4,
    runParallelism: Int = tables.size.coerceAtLeast(1),
    private val steps: (GroupId, ArchiveStep) -> Unit = { _, _ -> },
) : AutoCloseable {

    private val scheduler = Executors.newSingleThreadScheduledExecutor(named("archiver-scheduler"))
    private val runs = Executors.newFixedThreadPool(runParallelism, named("archiver-run"))
    private val exports = Executors.newFixedThreadPool(exportParallelism, named("archiver-export"))

    /** Presence means "a run for this group is in flight"; the skip-if-busy rule of spec 18.2. */
    private val busy = ConcurrentHashMap<GroupId, Boolean>()

    /** Schedules every configured group at [interval]. Scheduling stops in [close]. */
    fun start(interval: Duration) {
        val millis = interval.toMillis()
        tables.keys.forEach { group ->
            scheduler.scheduleAtFixedRate({ submit(group) }, millis, millis, TimeUnit.MILLISECONDS)
        }
    }

    /**
     * Queues one run on the bounded run pool - the path the scheduler uses, and the one that
     * makes an in-flight run interruptible by [close].
     */
    fun submit(group: GroupId): Future<*> = runs.submit {
        try {
            runOnce(group)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            log.debugf("archiver run for group '%s' was interrupted by shutdown", group)
        } catch (e: Exception) {
            // A failed run leaves whatever it left; the ticket-04 watchdog resolves it. There
            // is deliberately no compensating cleanup here - see the shutdown note on [close].
            log.errorf(e, "archiver run for group '%s' failed", group)
        }
    }

    /** One run, on the calling thread. Serialized per group; different groups may overlap. */
    fun runOnce(group: GroupId): RunOutcome {
        if (busy.putIfAbsent(group, true) != null) {
            log.infof("skipping archive run for group '%s': a run is already in flight", group)
            return RunOutcome.SKIPPED_BUSY
        }
        return try {
            publish(group)
        } finally {
            busy.remove(group)
        }
    }

    /** Spec 18.3, in its fixed order. */
    private fun publish(group: GroupId): RunOutcome {
        val groupTables = requireNotNull(tables[group]) { "no tables configured for group '$group'" }
        Files.createDirectories(tempRoot)
        val temp = Files.createTempDirectory(tempRoot, "$group-")
        try {
            return cache.withSnapshot(group) { snapshot ->
                // 1. export every table under the lease, in parallel, and inventory the result.
                val exported = exportAll(group, snapshot, groupTables, temp)
                steps(group, ArchiveStep.AFTER_EXPORT)

                // 2 + 3. the monotonicity guard and the PENDING insert share the DAO's one
                // transaction, so no concurrent publisher can slip a newer COMPLETE between them.
                val entry = try {
                    manifest.insertPending(
                        group = group.value,
                        dataAsOf = snapshot.dataAsOf,
                        inventory = Inventory.encode(exported),
                        generation = snapshot.generation,
                    )
                } catch (regression: DataAsOfRegression) {
                    log.warnf(
                        "ALERT: refusing to archive group '%s': data_as_of %s is not newer than " +
                            "the newest COMPLETE version's %s - the archiver is publishing a " +
                            "snapshot no fresher than the last one it published",
                        group, regression.offered, regression.newestComplete,
                    )
                    return@withSnapshot RunOutcome.SKIPPED_NOT_NEWER
                }
                steps(group, ArchiveStep.AFTER_PENDING_ROW)

                // 4. upload, then verify against the inventory that was committed above.
                uploadAndVerify(group, entry, exported, temp)
                steps(group, ArchiveStep.AFTER_UPLOAD)

                // 5. conditional flip, then the lease releases as withSnapshot returns.
                val won = manifest.markComplete(group.value, entry.version)
                steps(group, ArchiveStep.AFTER_COMPLETE)
                if (won) {
                    log.infof(
                        "archived group '%s' as version %d (%d objects, data_as_of %s)",
                        group, entry.version, exported.size, entry.dataAsOf,
                    )
                    RunOutcome.PUBLISHED
                } else {
                    log.warnf(
                        "archive version %d for group '%s' was already resolved by the watchdog; " +
                            "this run's flip moved nothing",
                        entry.version, group,
                    )
                    RunOutcome.LOST_RACE
                }
            }
        } finally {
            temp.toFile().deleteRecursively()
        }
    }

    /**
     * Per-table export tasks in parallel on the bounded export pool (plan P12). Each task
     * takes its own connection off the snapshot, so they do not contend on one.
     *
     * A failure cancels the siblings rather than letting them finish writing files into a
     * temp dir nothing will ever read.
     */
    private fun exportAll(
        group: GroupId,
        snapshot: Snapshot,
        groupTables: List<String>,
        temp: Path,
    ): List<ArchivedObject> {
        val futures = groupTables.map { table ->
            exports.submit(Callable { exportTable(snapshot, table, temp) })
        }
        return try {
            futures.map { it.get() }
        } catch (e: Exception) {
            futures.forEach { it.cancel(true) }
            when (e) {
                is ExecutionException -> throw IllegalStateException("archive export failed for group '$group'", e.cause ?: e)
                else -> throw e
            }
        }
    }

    /**
     * The export statement settled by the ticket-01 spike, in its production home.
     *
     * It runs directly on the READ_ONLY-attached snapshot connection, so no `copyOut`
     * staging step is needed (spec 18.6 item 1). The row count comes from a separate
     * `COUNT(*)` and never from COPY's own update count: an empty table and a driver that
     * stopped classifying COPY as DML both report 0, nothing downstream could tell them
     * apart, and this number is committed into the PENDING row the watchdog later verifies a
     * real object against.
     *
     * It lives here, not beside `copyOut`, because reaching it from `infra.snapshotcache`
     * would mean a seam on a frozen spi interface, while the public API already assumes its
     * callers speak DuckDB - `CopyOutSpec` takes caller SQL and a caller connection.
     */
    private fun exportTable(snapshot: Snapshot, table: String, temp: Path): ArchivedObject {
        val file = temp.resolve("$table.parquet")
        val rows = snapshot.connection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    "COPY (SELECT * FROM ${ident(table)}) TO " +
                        "'${literal(file.toAbsolutePath().toString())}' (FORMAT PARQUET)",
                )
                statement.executeQuery("SELECT COUNT(*) FROM ${ident(table)}").use { rs ->
                    check(rs.next()) { "row count query returned nothing for table $table" }
                    rs.getLong(1)
                }
            }
        }
        return ArchivedObject(
            table = table,
            objectKey = "$table.parquet",
            bytes = Files.size(file),
            checksum = sha256(file),
            rowCount = rows,
        )
    }

    /**
     * Spec 18.3 step 4. Verification asks the store what actually landed rather than
     * trusting the upload call's return: the inventory is the contract the ticket-04 watchdog
     * will later re-check with the same question, so a version that would fail that check
     * must never reach COMPLETE here.
     */
    private fun uploadAndVerify(
        group: GroupId,
        entry: ManifestEntry,
        exported: List<ArchivedObject>,
        temp: Path,
    ) {
        for (obj in exported) {
            if (Thread.interrupted()) {
                throw InterruptedException("archive run for group '$group' interrupted before ${obj.objectKey}")
            }
            steps(group, ArchiveStep.BEFORE_EACH_UPLOAD)
            objects.put(objectKey(entry, obj.objectKey, objects.bucket), temp.resolve(obj.objectKey))
        }
        for (obj in exported) {
            val key = objectKey(entry, obj.objectKey, objects.bucket)
            val stored = objects.sizeOf(key)
            check(stored == obj.bytes) {
                "archive version ${entry.version} for group '$group' does not match its inventory: " +
                    "$key is ${stored ?: "absent"}, inventory says ${obj.bytes} bytes"
            }
        }
    }

    /**
     * Spec 18.3 shutdown: stop scheduling, interrupt in-flight runs, let the lease release
     * inside the framework's drain, delete the temp directory.
     *
     * A leftover PENDING row is deliberately left alone. Resolving it here would create a
     * second recovery path that only graceful exits exercise and only crashes need; instead
     * the ticket-04 watchdog resolves it, so a crash and a clean shutdown converge on one
     * path that is tested every time either happens (D33, mirroring spec 10.2).
     *
     * The interrupt lands at this class's own checkpoints and at any interruptible blocking
     * call. A run parked in a socket read inside the MinIO client drains only when that
     * client's own timeout fires, and the framework then reports the still-outstanding lease
     * exactly as spec 10.2 step 4 says it does.
     */
    override fun close() {
        scheduler.shutdownNow()
        runs.shutdownNow()
        exports.shutdownNow()
        val budget = drainBudget.toMillis()
        val startedAt = System.nanoTime()
        // Both pools are awaited, and deliberately not with `&&`: short-circuiting on a
        // run pool that misses its budget would skip the export await entirely, and the
        // temp delete below would then race export threads still writing Parquet into it.
        // An interrupt here must not skip that delete either - it is the only thing that
        // removes this run's files, so the flag is restored and the cleanup still happens.
        var drained = false
        try {
            val runsDrained = runs.awaitTermination(budget, TimeUnit.MILLISECONDS)
            val left = (budget - (System.nanoTime() - startedAt) / 1_000_000).coerceAtLeast(0)
            val exportsDrained = exports.awaitTermination(left, TimeUnit.MILLISECONDS)
            drained = runsDrained && exportsDrained
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        if (!drained) {
            log.warnf("archiver did not drain within %s; deleting temp files anyway", drainBudget)
        }
        tempRoot.toFile().deleteRecursively()
    }

    private companion object {

        val log: Logger = Logger.getLogger(Archiver::class.java)


        fun sha256(file: Path): String {
            val digest = MessageDigest.getInstance("SHA-256")
            Files.newInputStream(file).use { input -> digest.consume(input) }
            return digest.digest().joinToString("") { "%02x".format(it) }
        }

        fun MessageDigest.consume(input: InputStream) {
            val buffer = ByteArray(64 * 1024)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) return
                update(buffer, 0, read)
            }
        }
    }
}
