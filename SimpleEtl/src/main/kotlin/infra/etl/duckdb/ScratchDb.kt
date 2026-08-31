package infra.etl.duckdb

import org.duckdb.DuckDBConnection
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/** The database file inside the run directory. File mode, never in-memory. */
private const val DB_FILE = "scratch.duckdb"

/**
 * The per-run DuckDB working area: one instance and one file per task run, created on
 * first reference and closed and deleted at run end, on success and failure alike.
 *
 * **Lazy.** Construction touches no filesystem. A task shape that never references `scratch`
 * never calls [connection], so no database file exists. Measured on
 * duckdb_jdbc 1.1.3 (Windows, Java 22, P4 scratchpad probe): the file appears the moment
 * `DriverManager.getConnection` returns, before any statement runs. Opening the connection is
 * therefore the only thing that can be deferred, and deferring it is enough.
 *
 * **Emptied on every path.** Nothing inside the file is ever reclaimed - `TRUNCATE` is an alias
 * for unqualified `DELETE`, `VACUUM` does not trigger deletion vacuuming, `VACUUM FULL` is
 * unimplemented, and `DROP TABLE` does not shrink the file. Closing the instance and
 * deleting the file is the only reclamation point there is, which is why callers reach [close]
 * through `use { }`: success, failure, and a throw from inside the run block all arrive there.
 *
 * [close] removes everything *under* [directory] and leaves the directory itself, because the
 * database file is not the only artefact a run leaves there. Parquet materialisations land beside
 * it, and DuckDB writes a `<file>.wal` that was measured present mid-run and gone
 * after a clean close - the recursive sweep covers the unclean case too. Give this object a
 * directory of its own: for the duration of the run it owns the contents.
 *
 * **[close] closes every connection it issued, including duplicates.** Measured on the same
 * probe: on Windows, deleting the database file throws `FileSystemException` while *any*
 * connection into it is open, and closing the one [connection] returned is not enough - an
 * outstanding [duplicate] keeps the lock and stays usable after the write connection is closed.
 * A caller that forgot to close a duplicate would otherwise leak the file silently.
 *
 * **Settings at open**, in the form `DuckDbGenerationStore.configure` already uses in the
 * snapshot cache: `SET memory_limit` and `SET temp_directory`. `SET threads` is left at the engine
 * default. Both settings are inherited by a [duplicate] - measured - so a duplicate is never
 * reconfigured. Note that DuckDB reads `MB` as a power of ten and echoes the setting back in
 * binary units, so `memoryLimitMb = 512` reads back as `488.2 MiB`; that is the request being
 * honoured, not truncated.
 *
 * @param directory the run's scratch directory. Created if absent, emptied at [close], never
 *   itself deleted.
 * @param memoryLimitMb the DuckDB `memory_limit`, a database-level setting and therefore not
 *   multiplied by the number of connections.
 * @param tempDirectory where DuckDB spills. Defaults to a directory inside [directory], where spill
 *   is reclaimed with the run and counted against the one volume whose `sizeLimit` is derived
 *   from file plus spill; a caller-supplied one outside [directory] is neither, and is the caller's
 *   to size and clean. Left unset, DuckDB 1.1.3 spills into `<dbfile>.tmp/` anyway -
 *   nothing fails that would otherwise succeed - but a gigabyte of it then sits somewhere nobody
 *   reading the configuration can see.
 */
class ScratchDb(
    private val directory: Path,
    private val memoryLimitMb: Int,
    private val tempDirectory: Path = directory.resolve("spill"),
) : AutoCloseable {

    init {
        require(memoryLimitMb > 0) { "scratch memoryLimitMb must be positive, was $memoryLimitMb" }
    }

    private val lock = Any()
    private val issued = mutableListOf<Connection>()
    private var primary: DuckDBConnection? = null
    private var opened = false
    private var closed = false

    /**
     * The single write connection, opening the instance on first call and returning
     * the same connection afterwards. Writes are sequential; a `Connection` used from two threads
     * at once crashes the JVM rather than raising an error, so a concurrent reader takes a
     * [duplicate] instead.
     */
    fun connection(): Connection = synchronized(lock) { open() }

    /**
     * An additional connection onto the same instance, for a concurrent read. The caller may close
     * it; [close] closes it regardless, because the file cannot be deleted while it is open.
     */
    fun duplicate(): Connection = synchronized(lock) {
        open().duplicate().also { issued += it }
    }

    /**
     * Total bytes of every regular file under [directory], recursively. 0 if the instance was
     * never opened, because then the directory does not exist either.
     *
     * **The whole directory is summed, not the database file**, and that is measured rather than
     * reasoned: after 500,000 appended rows the database file held **12,288 bytes** while
     * `scratch.duckdb.wal` beside it held **10,416,115**. `Files.size(dbFile)` would under-report
     * a live run by three orders of magnitude. Parquet materialisations land in the
     * same directory and are included for the same reason.
     *
     * **No `CHECKPOINT` is taken first.** Measured during the P8a spike round on the same 500,000
     * row state - the numbers below are that run's, and are the one pair in this KDoc not
     * re-measured in the P8b review: checkpointing folded
     * 10,428,403 bytes into 2,633,728 - a factor of four - so sampling after one would tell an
     * operator to size the scratch volume at a quarter of what the run actually needed. It is also
     * a write against a database that is about to be deleted, on the failure path too.
     *
     * **Spill is included only if it is still live**, which at the point this is sampled it
     * usually is not - DuckDB reclaims it as the query that needed it finishes. So this number
     * does **not** carry the spill term of the volume-sizing model, whose own arithmetic makes
     * spill 17.6 of its 30.2 GB. Sizing a volume from this alone covers the smaller half only.
     *
     * **Never throws** - and note that the guards are `runCatching`, which catches `Throwable`, so
     * this also swallows an `Error`. That is deliberate rather than sloppy: the caller is a
     * `finally` inside `use`, a Kotlin `finally` that throws *replaces* the in-flight exception
     * with no suppression, and the metrics guard around this call catches only `Exception`. This
     * is the one place in the module where the `Exception`/`Throwable` distinction is deliberately
     * widened, so it is stated rather than left to be discovered. Per file and overall: it is called from an observability path inside a
     * `finally`, and a `finally` that throws replaces the run's real failure. An unreadable path
     * contributes 0 rather than an exception. That branch is not covered by a test: this build
     * runs as root, so a permission-based case would be vacuous rather than green.
     *
     * Takes no lock. It reads the filesystem rather than this object's state, so there is nothing
     * for a lock to protect. An earlier wording justified this by contention with concurrently
     * running tasks, which cannot happen: one [ScratchDb] is constructed per run, so no
     * other task ever calls into this instance. The only concurrent caller is an intra-run
     * [duplicate] reader, and racing [close] there degrades to a partial sum or 0 - never a throw,
     * which is the property that matters here.
     */
    fun diskBytes(): Long = runCatching {
        if (!Files.isDirectory(directory)) return@runCatching 0L
        Files.walk(directory).use { paths ->
            paths.filter { Files.isRegularFile(it) }
                .mapToLong { path -> runCatching { Files.size(path) }.getOrElse { 0L } }
                .sum()
        }
    }.getOrElse { 0L }

    /**
     * Closes every issued connection and empties [directory]. Idempotent, and silent if the
     * instance was never opened.
     *
     * @throws IllegalStateException if the run left a temporary table behind, or if anything under
     *   [directory] survived. Both are raised *after* the cleanup, never instead of it, so a run
     *   that threw still gets its cleanup and `use` records this as a suppressed exception with
     *   the run's own failure left as the primary one.
     */
    override fun close() {
        synchronized(lock) {
            if (closed) return
            closed = true
            val temporary = mutableListOf<String>()
            issued.asReversed().forEach { connection ->
                runCatching { if (!connection.isClosed) temporary += temporaryTables(connection) }
                runCatching { connection.close() }
            }
            issued.clear()
            primary = null
            val undeleted = if (opened) deleteContents(directory) else emptyList()
            opened = false
            report(temporary.distinct(), undeleted)
        }
    }

    private fun open(): DuckDBConnection {
        check(!closed) { "scratch database in $directory is closed; the run that owned it has ended." }
        primary?.let { return it }
        Files.createDirectories(directory)
        Files.createDirectories(tempDirectory)
        opened = true
        val connection = DriverManager.getConnection("jdbc:duckdb:${directory.resolve(DB_FILE).toAbsolutePath()}")
            as DuckDBConnection
        try {
            connection.createStatement().use { statement ->
                statement.execute("SET memory_limit = '${memoryLimitMb}MB'")
                statement.execute("SET temp_directory = '${sqlLiteral(tempDirectory)}'")
            }
        } catch (failure: Exception) {
            runCatching { connection.close() }
            throw failure
        }
        issued += connection
        primary = connection
        return connection
    }

    /**
     * The temporary table is banned outright, because `CHECKPOINT` has no effect on one and so
     * it removes even the theoretical reclamation path. This is where that ban is enforced against
     * SQL no source scan can read: the statements of a `sql` step arrive from a task file at run
     * time, not from a Kotlin literal, and ArchUnit reads bytecode rather than SQL either way. The
     * catalog is the witness - `duckdb_tables()` carries a `temporary` flag on 1.1.3, measured, and
     * an offending table shows up there under database `temp`.
     *
     * Asked once per issued connection because the temporary catalog is per connection: measured on
     * the P4 probe, such a table created on the write connection is invisible from a [duplicate].
     * A check that consulted only [connection] would pass while the ban was being broken elsewhere.
     */
    private fun temporaryTables(connection: Connection): List<String> =
        connection.createStatement().use { statement ->
            statement.executeQuery("select table_name from duckdb_tables() where temporary").use { rows ->
                generateSequence { if (rows.next()) rows.getString(1) else null }.toList()
            }
        }

    /** Deletes everything under [root] depth first, returning what survived rather than stopping at it. */
    private fun deleteContents(root: Path): List<Path> = runCatching {
        val failed = mutableListOf<Path>()
        if (Files.isDirectory(root)) {
            Files.walk(root).use { paths ->
                paths.sorted(Comparator.reverseOrder())
                    .filter { it != root }
                    // add(), not +=: a Path is itself an Iterable<Path>, so += resolves to plus().
                    .forEach { path -> if (runCatching { Files.deleteIfExists(path) }.isFailure) failed.add(path) }
            }
        }
        failed.toList()
    }.getOrElse { listOf(root) }

    private fun report(temporary: List<String>, undeleted: List<Path>) {
        val problems = buildList {
            if (temporary.isNotEmpty()) {
                add(
                    "scratch in $directory held temporary table(s) $temporary. Spec 7.2 bans them: " +
                        "CHECKPOINT has no effect on one, which removes even the theoretical reclamation " +
                        "path. Write an attempt-suffixed ordinary table instead (spec 5.5)."
                )
            }
            if (undeleted.isNotEmpty()) {
                add(
                    "scratch in $directory was not fully reclaimed; ${undeleted.size} path(s) survived, " +
                        "starting ${undeleted.take(3)}. Spec 7.2 makes this the only reclamation point " +
                        "there is, so the volume fills run by run."
                )
            }
        }
        check(problems.isEmpty()) { problems.joinToString(" ") }
    }
}
