package infra.snapshotcache.spi

import infra.snapshotcache.api.CopyOutSpec
import java.sql.Connection

/**
 * The only component that touches generation files (spec 17.1).
 *
 * Production attaches and detaches DuckDB files; tests use an in-memory fake that records
 * every call and can be scripted to fail. Keeping this the sole file-touching seam is what
 * lets the whole core state machine be tested without DuckDB on the runtime path.
 *
 * No DuckDB type may appear in this interface.
 *
 * Threading: the core decides under its lock and calls these methods outside it (plan 2.5),
 * so implementations may block. Calls for one generation are sequenced by the core; calls
 * for different generations may overlap.
 */
interface GenerationStore {

    /** Creates the `.tmp` file for [gen] and opens a write connection to it. */
    fun createCandidate(gen: Long): Candidate

    /** Renames [gen] from `.tmp` to its final name. Atomic within one filesystem (spec 4.2). */
    fun promote(gen: Long)

    /** Attaches [gen] read-only for serving (spec 3.1, D3). */
    fun open(gen: Long): OpenGeneration

    /**
     * Detaches [gen]. Throws if a connection is still using it, in which case the core
     * defers reclamation to the next pass (spec 9.2, A4).
     *
     * Idempotent: detaching a generation that is not attached is a no-op. Reclaim retries
     * close + delete as one unit, so a close that already succeeded before a transient
     * delete failure is called again on the next pass, and throwing there would defer the
     * generation forever.
     */
    fun close(gen: Long)

    /** Deletes the file for [gen], whether candidate or promoted. Reclaims real disk (spec 3.1). */
    fun delete(gen: Long)

    /** Generation numbers whose files exist on disk, including leftovers from a crashed run (spec 10.1). */
    fun listOnDisk(): List<Long>

    /**
     * Runs [CopyOutSpec.sql] against [opened] and writes the rows into the caller's target,
     * returning the row count. The copy goes file-to-file rather than through the
     * application (spec 6.5, A7).
     */
    fun copyOut(opened: OpenGeneration, spec: CopyOutSpec): Long
}

/** A generation being built. Closing folds the WAL into the file and releases the write connection (spec 4.2 BUILDING). */
interface Candidate : AutoCloseable {
    val generation: Long

    /** Write connection handed to the [infra.snapshotcache.api.GenerationSource]. */
    fun connection(): Connection

    /**
     * Idempotent, and must not throw: it runs on the abort path, where throwing would mask
     * the failure that aborted the round. A failure folding the WAL is logged, not raised.
     */
    override fun close()
}

/** An attached, read-only generation being served. */
interface OpenGeneration {
    val generation: Long

    /** Fresh read-only connection into this generation. The caller closes it. */
    fun connection(): Connection

    /** Size of the generation file, for `snapshot_db_file_bytes` and the admin endpoint (spec 12.4, 12.7). */
    fun fileBytes(): Long
}
