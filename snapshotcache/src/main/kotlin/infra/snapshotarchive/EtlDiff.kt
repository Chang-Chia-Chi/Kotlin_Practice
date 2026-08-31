package infra.snapshotarchive

import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCache
import org.jboss.logging.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.ResultSet
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/** What happened to one row between the baseline checkpoint and the live snapshot. */
enum class DiffOp { I, U, D }

/**
 * Why a run has no usable baseline and must full-compare.
 *
 * All three are ordinary, expected answers rather than errors - which is precisely what lets
 * retention stay a dumb fixed window instead of a consumer registration scheme. The
 * reason is carried only so an operator can tell "this ETL is new" from "this ETL fell
 * further behind than retention", which are the same code path and very different problems.
 */
enum class FallbackReason {
    /** The ETL has no recorded watermark: its first run ever. */
    ABSENT,

    /** The recorded version is gone - the job ran slower than the retention window. */
    PURGED,

    /** The recorded version exists but is not COMPLETE, so its objects cannot be trusted. */
    NOT_COMPLETE,
}

/**
 * One changed row, as a diff emits it: `(pk, op, changed_columns, current values)`.
 *
 * [changedColumns] is the uniform rule with no special cases - the non-key columns whose
 * current value differs from the baseline's, by SQL `IS DISTINCT FROM` rather than by JVM
 * equality, so a driver's choice of `BigDecimal` scale cannot invent or hide a change. For an
 * insert that is every column that is not null in the new row; for a delete it is empty,
 * because there is no current row for a column to be current in.
 */
data class ChangedRow(
    val key: List<Any?>,
    val op: DiffOp,
    val changedColumns: List<String>,
    val values: Map<String, Any?>,
)

/**
 * What [EtlDiff.withDiff] hands the ETL, for the length of one lease.
 *
 * Two shapes rather than a nullable baseline, so the fallback is a branch the caller must
 * handle rather than one it can forget: there is no `changes()` to call on [FullCompare], and
 * `when` over a sealed type says so at compile time.
 */
sealed class Diff(
    /** The live snapshot, leased for the whole diff. Valid only inside the `withDiff` block. */
    val snapshot: Snapshot,
    private val watermark: () -> Long?,
) {

    /**
     * The value the ETL must record as its watermark: `max(version) WHERE
     * status='COMPLETE' AND data_as_of <= snapshot.dataAsOf`. Null when the group has no
     * COMPLETE version at or before this snapshot's moment, which the next run reads back as
     * [FallbackReason.ABSENT].
     *
     * The helper computes it and hands it back; it never writes it. Per-consumer state
     * belongs to the consumer, committed in the same transaction as the ETL's own output - a
     * watermark committed separately from the output it describes is one that can outlive a
     * rolled-back run.
     *
     * It is a function, not a value, because the ETL records it at commit time. Asking
     * late is safe: the `data_as_of <= T` half of the predicate is evaluated against the
     * leased snapshot's moment, so a checkpoint published while this run was working - which
     * describes state this run never saw - can never be selected however long the run took.
     */
    fun nextWatermark(): Long? = watermark()

    /**
     * No usable baseline. The ETL anti-joins its own tables against [snapshot], which needs
     * nothing from this layer.
     */
    class FullCompare internal constructor(
        snapshot: Snapshot,
        watermark: () -> Long?,
        val reason: FallbackReason,
    ) : Diff(snapshot, watermark)

    /** The baseline checkpoint is downloaded and joinable; [changes] answers per table. */
    class Incremental internal constructor(
        snapshot: Snapshot,
        watermark: () -> Long?,
        val baselineVersion: Long,
        private val baseline: Map<String, Path>,
        private val primaryKeys: Map<String, List<String>>,
    ) : Diff(snapshot, watermark) {

        /**
         * The `FULL OUTER JOIN` on primary key: the downloaded checkpoint against the live
         * snapshot, in the caller's local DuckDB.
         *
         * Computed per call rather than for every table up front, so an ETL can apply one
         * table's changes and let them go before asking for the next - at ~1M rows and ~100k
         * changes a table, the difference is the whole working set.
         *
         * A table whose columns have changed since the checkpoint fails loudly here. It is
         * the one case this helper refuses rather than answers: comparing the columns the two
         * shapes happen to share would silently miss every change in the columns they do not,
         * which is the under-report the whole design exists to make impossible. Reset the
         * watermark and full-compare instead.
         */
        fun changes(table: String): List<ChangedRow> {
            val file = requireNotNull(baseline[table]) {
                "table '$table' is not in archive version $baselineVersion's inventory"
            }
            val key = requireNotNull(primaryKeys[table]) {
                "no primary key is configured for table '$table'; archived tables declare one (D36)"
            }
            return snapshot.connection().use { connection ->
                val live = columnsOf(connection, ident(table))
                val archived = columnsOf(connection, parquet(file))
                check(live == archived) {
                    "table '$table' has a different shape than archive version $baselineVersion " +
                        "recorded (now $live, then $archived); that baseline cannot say what " +
                        "changed without under-reporting the columns it does not have - reset " +
                        "the watermark and full-compare"
                }
                require(live.containsAll(key)) {
                    "primary key $key of table '$table' is not a subset of its columns $live"
                }
                connection.createStatement().use { statement ->
                    statement.executeQuery(joinSql(table, file, live, key)).use { rows ->
                        read(rows, live, key)
                    }
                }
            }
        }

        /**
         * The join, and the only interesting SQL in this layer.
         *
         * The projection is the live row, then the baseline's key columns, then one
         * `IS DISTINCT FROM` flag per non-key column. The key columns come back from both
         * sides because a deleted row has no live key to report, and the flags are computed
         * by the engine because the alternative - comparing driver objects in the JVM - would
         * disagree with the `WHERE` clause that selected the row on exactly the types where
         * getting it wrong is silent.
         */
        private fun joinSql(table: String, file: Path, live: List<String>, key: List<String>): String {
            val nonKey = live - key.toSet()
            val distinct = nonKey.map { "l.${ident(it)} IS DISTINCT FROM b.${ident(it)}" }
            val projection = live.map { "l.${ident(it)}" } +
                key.map { "b.${ident(it)}" } +
                distinct.map { "($it)" }
            return """
                SELECT ${projection.joinToString(", ")}
                  FROM ${ident(table)} AS l
                  FULL OUTER JOIN ${parquet(file)} AS b
                    ON ${key.joinToString(" AND ") { "l.${ident(it)} = b.${ident(it)}" }}
                 WHERE l.${ident(key[0])} IS NULL
                    OR b.${ident(key[0])} IS NULL
                    OR ${if (distinct.isEmpty()) "FALSE" else distinct.joinToString(" OR ")}
            """.trimIndent()
        }

        private fun read(rows: ResultSet, live: List<String>, key: List<String>): List<ChangedRow> {
            val nonKey = live - key.toSet()
            val changed = mutableListOf<ChangedRow>()
            while (rows.next()) {
                val current = live.withIndex().associate { (i, c) -> c to rows.getObject(i + 1) }
                val baselineKey = key.indices.map { rows.getObject(live.size + it + 1) }
                // Archived tables declare NOT NULL keys, so a null one means that side had no row.
                val op = when {
                    baselineKey[0] == null -> DiffOp.I
                    current[key[0]] == null -> DiffOp.D
                    else -> DiffOp.U
                }
                changed += ChangedRow(
                    key = if (op == DiffOp.D) baselineKey else key.map { current[it] },
                    op = op,
                    changedColumns = if (op == DiffOp.D) emptyList() else {
                        nonKey.filterIndexed { i, _ -> rows.getBoolean(live.size + key.size + i + 1) }
                    },
                    values = if (op == DiffOp.D) emptyMap() else current,
                )
            }
            return changed
        }

        private fun columnsOf(connection: Connection, from: String): List<String> =
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT * FROM $from LIMIT 0").use { rows ->
                    (1..rows.metaData.columnCount).map { rows.metaData.getColumnName(it) }
                }
            }

        private fun parquet(file: Path): String =
            "read_parquet('${literal(file.toAbsolutePath().toString())}')"
    }
}

/**
 * The consumer side of the archive: what changed since the version this ETL last processed.
 *
 * **The correctness rule that carries this class:** the baseline checkpoint must have been
 * taken at or before the ETL's last processed moment. Everything else follows. [withDiff]
 * takes a watermark the caller *recorded*, never "the newest checkpoint available now" - one
 * published after the last run describes state that run never processed, and diffing against
 * it silently drops every change in the gap. Under-reporting is impossible by construction
 * here; over-reporting is bounded by one archive interval and is safe against idempotent
 * consumers, so the watermark predicate is allowed to err old and does.
 *
 * The lease is held for the whole diff by [SnapshotCache.withSnapshot] scoping, so the live
 * side of the comparison cannot shift underneath it and no exit path can leak it. The cost is
 * that [Diff.snapshot] and [Diff.Incremental.changes] are valid only inside the block; a
 * `Diff` that escapes it holds a released lease and is nobody's to use.
 *
 * [primaryKeys] is explicit configuration for the same reason [Archiver.tables] is: a stable
 * primary key is a property of the schema contract every archived table signs up to, not of
 * whatever the snapshot happens to have attached.
 */
class EtlDiff(
    private val cache: SnapshotCache,
    private val manifest: ManifestDao,
    private val objects: ObjectStore,
    private val primaryKeys: Map<GroupId, Map<String, List<String>>>,
    private val downloadRoot: Path,
) : AutoCloseable {

    // A constructor knob no caller ever set; config for a value that never changes is not
    // config. P9's wiring can make it configurable again when it has a reason to.
    private val downloads = Executors.newFixedThreadPool(DOWNLOAD_THREADS, named("etl-diff-download"))

    /**
     * Runs [block] against the diff of [watermark]'s checkpoint versus the live snapshot.
     *
     * [watermark] is the version the caller recorded on its last run, or null on its first.
     * Absent, purged and not-COMPLETE all come back as [Diff.FullCompare] rather than as an
     * exception: a consumer that has fallen behind retention is doing ordinary work, not
     * recovering from a fault.
     *
     * The downloaded checkpoint lives in a per-call temp directory that is deleted on every
     * exit path, including the block's own exception.
     */
    fun <T> withDiff(group: GroupId, watermark: Long?, block: (Diff) -> T): T {
        Files.createDirectories(downloadRoot)
        val temp = Files.createTempDirectory(downloadRoot, "$group-")
        try {
            return cache.withSnapshot(group) { snapshot ->
                val next = { manifest.watermark(group, snapshot.dataAsOf) }
                val found = watermark?.let { manifest.find(group, it) }
                block(
                    if (found != null && found.status == ArchiveStatus.COMPLETE) {
                        log.debugf(
                            "diffing group '%s' generation %d against archive version %d (data_as_of %s)",
                            group, snapshot.generation, found.version, found.dataAsOf,
                        )
                        Diff.Incremental(
                            snapshot = snapshot,
                            watermark = next,
                            baselineVersion = found.version,
                            baseline = download(found, temp),
                            primaryKeys = requireNotNull(primaryKeys[group]) {
                                "no primary keys are configured for group '$group'"
                            },
                        )
                    } else {
                        val reason = when {
                            watermark == null -> FallbackReason.ABSENT
                            found == null -> FallbackReason.PURGED
                            else -> FallbackReason.NOT_COMPLETE
                        }
                        log.infof(
                            "group '%s' has no usable diff baseline (%s: watermark %s); the " +
                                "consumer full-compares against generation %d, which is correct " +
                                "and only more expensive",
                            group, reason, watermark ?: "none", snapshot.generation,
                        )
                        Diff.FullCompare(snapshot, next, reason)
                    },
                )
            }
        } finally {
            temp.toFile().deleteRecursively()
        }
    }

    /**
     * Downloads one version's objects in parallel, per its inventory.
     *
     * The inventory is the list of what the version contains - the bucket is never asked - so
     * a COMPLETE row whose objects a purge has since removed surfaces as a download failure
     * rather than as a short answer. That ordering makes it unreachable:
     * the purge marks a version FAILED before deleting anything, and a FAILED version is
     * never a baseline.
     */
    private fun download(entry: ManifestEntry, temp: Path): Map<String, Path> {
        val futures = Inventory.decode(entry.inventory).map { obj ->
            downloads.submit(
                Callable {
                    val file = temp.resolve(obj.objectKey)
                    objects.get(objectKey(entry, obj.objectKey, objects.bucket), file)
                    obj.table to file
                },
            )
        }
        return try {
            futures.associate { it.get() }
        } catch (e: Exception) {
            futures.forEach { it.cancel(true) }
            when (e) {
                is ExecutionException -> throw IllegalStateException(
                    "could not download archive version ${entry.version} for group '${entry.group}'",
                    e.cause ?: e,
                )

                else -> throw e
            }
        }
    }

    /** Stops the download pool. Nothing else is owned: this class holds no state between runs. */
    override fun close() {
        downloads.shutdownNow()
    }

    private companion object {

        /** Per-table checkpoint downloads within one diff. */
        const val DOWNLOAD_THREADS = 4

        val log: Logger = Logger.getLogger(EtlDiff::class.java)

    }
}
