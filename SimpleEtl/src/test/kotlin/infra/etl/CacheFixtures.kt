package infra.etl

import infra.snapshotcache.api.CopyOutResult
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCache
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * P9 test support: a [SnapshotCache] double over a real DuckDB generation file.
 *
 * Its own file, not an addition to `TaskFixtures` (already 598 lines), for P6's reason: a phase's
 * fixture belongs to the phase that can change it.
 *
 * ### Why the copy is real
 *
 * `copyOut` here performs the same ATTACH / USE / CTAS / USE / DETACH against a genuine second
 * DuckDB file that `DuckDbGenerationStore.copyOut` performs, in the same order and with the same
 * quoting. A double that ran `create table t as select 1` on the target connection would satisfy
 * every row assertion in this phase and leave the whole interaction with `ScratchDb.connection()`
 * untested - the cross-catalog CTAS, the `USE` that has to be put back, and the fact that the
 * engine hands over the scratch **write** connection rather than a duplicate.
 *
 * ### What it refuses to do
 *
 * [acquire] and [withSnapshot] throw an `Error`, which the engine's `catch (Exception)` does not
 * absorb, so a run that touched either dies loudly rather than being reported as an ordinary task
 * failure a test might mistake for the one it injected. That is the whole of contract 2.2: the
 * framework owns no lease, so `copyOut` is the only channel to a generation it may use. [acquire]
 * cannot be *counted* into a passing assertion instead - a counting double asserts `0 == 0`, which
 * is green against an engine that has no cache executor at all.
 *
 * [currentInfo] is counted rather than thrown from: it takes no lease, so calling it is not a
 * contract breach, but the engine has no reason to and a call would mean it is deciding something
 * for itself that spec 7.3 gives to `copyOut`.
 *
 * ### The generation file is closed between uses
 *
 * Measured (duckdb_jdbc 1.1.3): a file already open in this process cannot be ATTACHed - "File is
 * already open". [seed] therefore opens, writes and closes, and [readGeneration] is only legal
 * once a run has returned.
 */
class FakeSnapshotCache(
    val generationFile: Path,
    val generation: Long = 42L,
    val dataAsOf: Instant = Instant.parse("2026-02-01T03:00:00Z"),
    override val defaultWaitBudget: Duration = Duration.ofSeconds(7),
) : SnapshotCache {

    /**
     * Raised instead of copying, for the no-generation case. A function of the group because
     * `NotReadyException` carries the group and names it in its message (contract 7.5).
     */
    var failure: ((GroupId) -> Throwable)? = null

    /**
     * Deletes the generation file from inside [copyOut]'s own `finally`, which is where the real
     * cache's reclamation becomes possible once the lease is released.
     *
     * This is the assertion that separates a materialised CTAS from a view over an attached
     * generation: with the file gone, a later step reading the copied dataset can only be reading
     * bytes that live inside scratch.
     */
    var deleteGenerationInsideCopyOut: Boolean = false

    /** Every [CopyOutSpec] this cache was handed, in order. */
    val copyOuts: MutableList<CopyOutSpec> = CopyOnWriteArrayList()

    /** Every group asked for, in order - the other half of `copyOut(group, spec)`. */
    val groups: MutableList<GroupId> = CopyOnWriteArrayList()

    /** Counted, never expected to move: see the class KDoc. */
    val currentInfoCalls: AtomicInteger = AtomicInteger()

    val acquireCalls: AtomicInteger = AtomicInteger()
    val withSnapshotCalls: AtomicInteger = AtomicInteger()

    /** True until [deleteGenerationInsideCopyOut] has had its effect. */
    fun generationExists(): Boolean = Files.exists(generationFile)

    /** A table of [rows] rows inside the generation, built and then closed. */
    fun seed(table: String, rows: Int) {
        Files.createDirectories(generationFile.toAbsolutePath().parent)
        DriverManager.getConnection("jdbc:duckdb:${generationFile.toAbsolutePath()}").use { connection ->
            connection.createStatement().use {
                it.execute(
                    "create table $table as " +
                        "select cast(i as bigint) as lot_id, cast(i * 1.5 as decimal(18,3)) as qty, " +
                        "cast('F12' as varchar) as site from range(0, $rows) t(i)",
                )
            }
        }
    }

    /** Reads the generation once a run has released it, so "a subset" can be asserted as a subset. */
    fun <T> readGeneration(block: (Connection) -> T): T {
        check(generationExists()) { "no generation file at $generationFile" }
        return DriverManager.getConnection("jdbc:duckdb:${generationFile.toAbsolutePath()}").use(block)
    }

    override fun copyOut(group: GroupId, spec: CopyOutSpec, waitBudget: Duration): CopyOutResult {
        groups += group
        copyOuts += spec
        failure?.let { throw it(group) }
        val alias = "copyout_${copyOuts.size}"
        spec.targetConnection.createStatement().use { statement ->
            val home = statement.one("select current_database()")
            val target = "${ident(home)}.${ident(spec.targetTable)}"
            statement.execute("ATTACH '${literal(generationFile)}' AS $alias (READ_ONLY)")
            try {
                statement.execute("USE $alias")
                try {
                    statement.execute("CREATE TABLE $target AS ${spec.sql}")
                } finally {
                    statement.execute("USE ${ident(home)}")
                }
            } finally {
                statement.execute("DETACH $alias")
                if (deleteGenerationInsideCopyOut) Files.deleteIfExists(generationFile)
            }
            return CopyOutResult(generation, dataAsOf, statement.one("select count(*) from $target").toLong())
        }
    }

    override fun <T> withSnapshot(group: GroupId, waitBudget: Duration, block: (Snapshot) -> T): T {
        withSnapshotCalls.incrementAndGet()
        throw CacheLeaseError("withSnapshot")
    }

    override fun acquire(group: GroupId, waitBudget: Duration): Snapshot {
        acquireCalls.incrementAndGet()
        throw CacheLeaseError("acquire")
    }

    override fun currentInfo(group: GroupId): GenerationInfo? {
        currentInfoCalls.incrementAndGet()
        return GenerationInfo(generation, dataAsOf, dataAsOf, emptyMap())
    }

    /** DuckDB's own quoting, copied from `DuckDbGenerationStore` so the double cannot be gentler. */
    private fun ident(name: String): String = "\"${name.replace("\"", "\"\"")}\""

    private fun literal(path: Path): String =
        path.toAbsolutePath().toString().replace(File.separatorChar, '/').replace("'", "''")

    private fun Statement.one(sql: String): String =
        executeQuery(sql).use { rows ->
            check(rows.next()) { "no row from: $sql" }
            rows.getString(1)
        }
}

/**
 * What a lease-taking call raises. An `Error` on purpose: `TaskEngine`'s step loop catches
 * `Exception`, so an `Exception` here would be reported as an ordinary step failure and a test
 * asserting "the run failed" would pass whether the engine took a lease or not.
 */
class CacheLeaseError(method: String) : Error(
    "probe: the framework called SnapshotCache.$method. copyOut owns the lease lifecycle, and a " +
        "task holding one across steps stalls every refresh of the cache (spec 7.3, contract 2.2).",
)
