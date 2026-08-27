package infra.snapshotcache.duckdb

import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.spi.Candidate
import infra.snapshotcache.spi.GenerationStore
import infra.snapshotcache.spi.OpenGeneration
import infra.snapshotcache.spi.ident
import infra.snapshotcache.spi.literal
import infra.snapshotcache.spi.queryLong
import infra.snapshotcache.spi.queryString
import org.duckdb.DuckDBConnection
import org.jboss.logging.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * The only DuckDB-touching code in the framework (plan P7). One generation is one
 * standalone file under [directory] (spec 3.1):
 *
 *     gen_0000000123.db        promoted
 *     gen_0000000124.db.tmp    being built
 *
 * Serving uses one in-memory DuckDB instance owned by this store; [open] ATTACHes the
 * generation file onto it READ_ONLY (D3), consumer connections are duplicates of the
 * serving connection with the generation as their default database, and [close] DETACHes.
 * Reclaim is DETACH + file delete - DuckDB 1.1.3 has no file-shrinking vacuum (spec 14.5,
 * D2), so deleting the file is the only way disk is genuinely returned.
 *
 * Every connection this store issues is tracked. [close] refuses to DETACH while an issued
 * connection into that generation is still open: DuckDB 1.1.3 does not reliably fail the
 * DETACH itself, and detaching under a live reader would break the A4 defer-to-next-GC
 * safeguard of spec 9.2, so the store enforces the contract that its own bookkeeping can
 * prove. The core treats the throw as "defer reclamation" either way.
 *
 * Thread-safety follows the SPI contract: calls for one generation are sequenced by the
 * core; calls for different generations may overlap, hence the concurrent collections.
 */
class DuckDbGenerationStore(
    private val directory: Path,
    private val tempDirectory: Path,
    private val memoryLimit: String,
    /** Serving instance only; candidate builds keep the engine default and use the cores. */
    private val servingThreads: Int? = null,
) : GenerationStore, AutoCloseable {

    private val log = Logger.getLogger(DuckDbGenerationStore::class.java)

    /** Every connection issued for a generation: the candidate write connection and reader duplicates. */
    private val issued = ConcurrentHashMap<Long, CopyOnWriteArrayList<Connection>>()

    /** Generations currently ATTACHed to [serving]; what makes [close] idempotent. */
    private val attached = ConcurrentHashMap.newKeySet<Long>()
    private val copyOutSequence = AtomicLong()
    private val serving: DuckDBConnection

    init {
        Files.createDirectories(directory)
        Files.createDirectories(tempDirectory)
        serving = DriverManager.getConnection("jdbc:duckdb:") as DuckDBConnection
        configure(serving)
        if (servingThreads != null) {
            serving.createStatement().use { it.execute("SET threads = $servingThreads") }
        }
    }

    override fun createCandidate(gen: Long): Candidate {
        val connection = DriverManager.getConnection("jdbc:duckdb:${tmpPath(gen)}")
        try {
            configure(connection)
        } catch (failure: Exception) {
            runCatching { connection.close() }
            throw failure
        }
        track(gen, connection)
        return DuckDbCandidate(gen, connection)
    }

    override fun promote(gen: Long) {
        Files.move(tmpPath(gen), finalPath(gen), StandardCopyOption.ATOMIC_MOVE)
    }

    override fun open(gen: Long): OpenGeneration {
        serving.createStatement().use { statement ->
            statement.execute("ATTACH '${literal(finalPath(gen).toString())}' AS ${alias(gen)} (READ_ONLY)")
        }
        attached += gen
        return DuckDbOpenGeneration(gen)
    }

    override fun close(gen: Long) {
        // Idempotent (SPI contract): reclaim retries close+delete as one unit, so a detach
        // that already succeeded must not fail the retry - DETACH of an unattached alias is
        // a catalog error, and that would defer the generation forever.
        if (gen !in attached) return
        // No TOCTOU between the count and the DETACH: the SPI sequences all calls for one
        // generation, so no new connection can be issued into [gen] while close runs.
        val inUse = issued[gen]?.count { !it.isClosed } ?: 0
        check(inUse == 0) { "generation $gen still has $inUse open connection(s); DETACH deferred" }
        serving.createStatement().use { statement ->
            statement.execute("DETACH ${alias(gen)}")
        }
        attached -= gen
        issued.remove(gen)
    }

    override fun delete(gen: Long) {
        // Both forms plus their WAL siblings, so P9's startup wipe removes a crashed
        // build completely (spec 10.1, D10). A promoted, checkpointed file has no WAL.
        for (path in listOf(finalPath(gen), tmpPath(gen))) {
            Files.deleteIfExists(path)
            Files.deleteIfExists(path.resolveSibling("${path.fileName}.wal"))
        }
        // Abort paths reach delete without close (a failed build never opens the
        // generation), so the tracking entry is dropped here too or it would grow one
        // entry per failed round forever. Safe: the SPI sequencing contract guarantees
        // every connection into a deleted generation is already closed.
        issued.remove(gen)
        attached -= gen
    }

    override fun listOnDisk(): List<Long> {
        val names = Files.list(directory).use { entries ->
            entries.map { it.fileName.toString() }.toList()
        }
        return names.mapNotNull { FILE_NAME.matchEntire(it)?.groupValues?.get(1)?.toLong() }
            .distinct()
            .sorted()
    }

    /**
     * File-to-file copy (spec 6.5, A7): the caller's target instance ATTACHes the
     * generation file directly and CTASes the subset; no row passes through the
     * application. The target connection's default database is restored afterwards.
     */
    override fun copyOut(opened: OpenGeneration, spec: CopyOutSpec): Long {
        val alias = "copyout_${copyOutSequence.incrementAndGet()}"
        spec.targetConnection.createStatement().use { statement ->
            val home = statement.queryString("SELECT current_database()")
            val target = "${ident(home)}.${ident(spec.targetTable)}"
            statement.execute("ATTACH '${literal(finalPath(opened.generation).toString())}' AS $alias (READ_ONLY)")
            val rows = try {
                statement.execute("USE $alias")
                try {
                    statement.execute("CREATE TABLE $target AS ${spec.sql}")
                } finally {
                    // Best effort: on a connection that died mid-CTAS this throws too, and
                    // replacing the CTAS failure with "USE failed" would lose the root cause.
                    runCatching { statement.execute("USE ${ident(home)}") }
                        .onFailure { log.warnf("could not restore database %s after copyOut: %s", home, it.message) }
                }
                statement.queryLong("SELECT COUNT(*) FROM $target")
            } catch (failure: Exception) {
                runCatching { statement.execute("DETACH $alias") }
                    .onFailure { log.warnf("could not detach %s after a failed copyOut: %s", alias, it.message) }
                throw failure
            }
            // Propagates rather than being swallowed: the lease still pins the generation
            // here, so failing the copyOut is safe - whereas a surviving attach on the
            // caller's instance is a dangling alias once the next reclaim deletes the file.
            statement.execute("DETACH $alias")
            return rows
        }
    }

    /** Closes every issued connection and the serving instance. For the P9 shutdown path and tests. */
    override fun close() {
        issued.values.flatten().forEach { runCatching { it.close() } }
        issued.clear()
        attached.clear()
        runCatching { serving.close() }
    }

    /** Issued connections not yet closed. Leak evidence for the rotation tests (plan P7 acceptance). */
    internal fun openIssuedConnections(): Int = issued.values.sumOf { list -> list.count { !it.isClosed } }

    /** Generations with a live tracking entry. Leak evidence for the abort-path rotation test. */
    internal fun trackedGenerations(): Int = issued.size

    /** Tracking entries held for [gen], closed ones included. Evidence that [track] prunes. */
    internal fun trackedConnections(gen: Long): Int = issued[gen]?.size ?: 0

    private inner class DuckDbCandidate(
        override val generation: Long,
        private val write: Connection,
    ) : Candidate {

        private val closed = AtomicBoolean(false)

        override fun connection(): Connection = write

        /**
         * Folds the WAL via CHECKPOINT, then closes the write connection (spec 4.2
         * BUILDING). Idempotent and never throws: close runs inside the abort path's
         * `use {}`, and throwing there would mask the exception that aborted the round
         * (P0 progress note). A failed CHECKPOINT is only logged - closing the
         * connection folds the WAL anyway, and the verify gate reopens the file.
         */
        override fun close() {
            if (!closed.compareAndSet(false, true)) return
            try {
                write.createStatement().use { it.execute("CHECKPOINT") }
            } catch (failure: Exception) {
                log.warnf("CHECKPOINT of candidate %d failed: %s", generation, failure.message)
            } finally {
                runCatching { write.close() }
            }
        }
    }

    private inner class DuckDbOpenGeneration(override val generation: Long) : OpenGeneration {

        /**
         * A duplicate of the serving connection with this generation as its default
         * database, so unqualified table names resolve inside the generation file.
         * Writes are rejected by the READ_ONLY attach (A3). The caller closes it.
         */
        override fun connection(): Connection {
            val duplicate = serving.duplicate()
            try {
                duplicate.createStatement().use { it.execute("USE ${alias(generation)}") }
            } catch (failure: Exception) {
                runCatching { duplicate.close() }
                throw failure
            }
            track(generation, duplicate)
            return duplicate
        }

        override fun fileBytes(): Long = Files.size(finalPath(generation))
    }

    private fun configure(connection: Connection) {
        connection.createStatement().use { statement ->
            statement.execute("SET memory_limit = '${literal(memoryLimit)}'")
            statement.execute("SET temp_directory = '${literal(tempDirectory.toString())}'")
        }
    }

    /**
     * Consumers acquire per request, so one long-lived current generation would otherwise
     * accumulate an entry per issued connection for as long as it stays current. Closed
     * entries are dropped on every append, so the list only holds connections still open.
     */
    private fun track(gen: Long, connection: Connection) {
        val list = issued.computeIfAbsent(gen) { CopyOnWriteArrayList() }
        list.removeIf { it.isClosed }
        list.add(connection)
    }

    private fun finalPath(gen: Long): Path = directory.resolve("gen_${gen.padded()}.db")
    private fun tmpPath(gen: Long): Path = directory.resolve("gen_${gen.padded()}.db.tmp")
    private fun alias(gen: Long): String = "g$gen"
    private fun Long.padded(): String = toString().padStart(10, '0')

    private companion object {
        /** spec 3.1 layout: zero-padded final files, `.tmp` while building; both count as on-disk leftovers. */
        val FILE_NAME = Regex("gen_(\\d{10})\\.db(\\.tmp)?")
    }
}
