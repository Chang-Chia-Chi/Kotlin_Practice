package dynacache.engine.persist

import dynacache.engine.ApEngine
import dynacache.engine.Value
import java.io.IOException
import java.io.OutputStream
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Random

/**
 * Local persistence of spec 2.8: RDB snapshots at `<dir>/dump.rdb`, saved on an interval and at
 * shutdown, and, with an [fsync] policy, the write-ahead log at `<dir>/wal.<seq>`. A save takes
 * every partition's point-in-time view on that partition's executor and serializes off the
 * executors, so the command path never waits on the disk (C9 by construction;
 * `snapshot_does_not_block_reads`).
 *
 * A save is the log's checkpoint: the views are one cut across the partitions, the log continues
 * in a fresh file named for the cut's seq, the snapshot records that seq, and once it is on disk
 * the files holding only entries at or below it are deleted. Recovery is the reverse: the
 * snapshot, then every logged entry after its seq (C14).
 *
 * The engine owns no thread: the server's scheduler calls [maybeSave] and its shutdown hook
 * calls [close]. [save] and [restore] share one monitor, so neither ever runs inside the other.
 * [sink] is where the writer's bytes go, injectable so a test can stall it.
 */
class SnapshotEngine(
    private val engine: ApEngine,
    private val dir: Path,
    private val clock: Clock,
    private val interval: Duration = Duration.ofSeconds(300),
    private val seeds: Random = Random(),
    private val sink: (Path) -> OutputStream = { Files.newOutputStream(it) },
    /**
     * How the log is forced to disk; null is snapshots with no log of their own, and such an
     * engine never rotates, replays or deletes a log file: the engine's log, if it has one, is
     * another [SnapshotEngine]'s to checkpoint.
     */
    private val fsync: FsyncPolicy? = null,
) : AutoCloseable {

    private val file = dir.resolve("dump.rdb")
    private val temp = dir.resolve("dump.rdb.tmp")
    private var lastSave: Instant = clock.instant()

    /**
     * Writes the whole keyspace to a temporary file and renames it over the last snapshot in one
     * step, then drops the log files the snapshot has made redundant. Answers the checkpoint's
     * seq: the last log entry the snapshot holds.
     */
    @Synchronized
    fun save(): Long {
        val now = clock.instant()
        val snapshot = engine.snapshotView(now, ::cut).join()
        sink(temp).use { RdbWriter.write(it, snapshot.entries, now, snapshot.walSeq) }
        Files.move(temp, file, ATOMIC_MOVE, REPLACE_EXISTING)
        lastSave = now
        // Only now is everything at or below the cut in a snapshot on disk.
        if (fsync != null) for (log in logs()) if (seqOf(log) < snapshot.walSeq) Files.delete(log)
        return snapshot.walSeq
    }

    /**
     * Runs at the cut with every partition parked: the checkpoint's seq, and when the log is this
     * engine's, the log continues in a file named for it. A snapshots-only engine (a snapshot
     * set's part, T74) stamps the seq and leaves the log where it is: rotating it here would move
     * the live log under a directory that is deleted whole when the set is aborted (C14).
     */
    private fun cut(): Long {
        val wal = engine.wal ?: return 0
        val seq = wal.lastSeq
        if (fsync != null) wal.rotate(FileChannelSink(logFile(seq)))
        return seq
    }

    /**
     * Loads the last snapshot into the engine, if there is one, then redoes the log after it and
     * attaches the log to the engine; answers how many keys the snapshot held.
     */
    @Synchronized
    fun restore(): Int {
        val snapshot = if (Files.exists(file)) Files.newInputStream(file).use { RdbReader(seeds).read(it) } else RdbSnapshot(0, emptyList())
        // A sorted set is never empty (T07), so an empty one is a writer bug, not an empty set.
        if (snapshot.entries.any { entry -> entry.value.let { it is Value.ZSet && it.scores.size == 0 } }) {
            throw IOException("$file holds a sorted set with no members")
        }
        engine.restore(snapshot.entries).join()
        if (fsync != null) {
            check(engine.wal == null) { "the log is already attached" }
            engine.wal = replay(snapshot.walSeq, fsync)
        }
        return snapshot.entries.size
    }

    /**
     * Redo: every logged entry after [checkpoint], oldest file first, through the engine's own
     * `submit`, so accounting, the wheel and the kind checks all apply. An entry at or below the
     * last one applied is skipped, which is what makes replay idempotent: an entry the log holds
     * twice redoes once. The newest file continues as the log, cut at its last whole entry so
     * nothing is ever appended after a torn tail.
     */
    private fun replay(checkpoint: Long, policy: FsyncPolicy): WalWriter {
        var applied = checkpoint
        var newest: Path? = null
        for (log in logs()) {
            val scan = WalReader(log).readAll()
            for (entry in scan.entries) if (entry.seq > applied) {
                CommandCodec.decode(entry.op, entry.payload).forEach { engine.submit(it).join() }
                applied = entry.seq
            }
            FileChannel.open(log, WRITE).use { it.truncate(scan.stoppedAt) }
            newest = log
        }
        return WalWriter(newest ?: logFile(checkpoint), applied + 1, policy, clock)
    }

    /** The interval hook: saves when [interval] has passed since the last save. */
    fun maybeSave(now: Instant) {
        if (!now.isBefore(lastSave.plus(interval))) save()
    }

    /** The graceful-shutdown save, to be called once nothing submits any more and before the engine closes. */
    override fun close() {
        save()
        engine.wal?.close()
    }

    /** The log file holding the entries after [seq]. */
    private fun logFile(seq: Long): Path = dir.resolve("wal.$seq")

    private fun seqOf(log: Path): Long = log.fileName.toString().removePrefix("wal.").toLong()

    /** Every log file, oldest first. */
    private fun logs(): List<Path> =
        Files.list(dir).use { paths -> paths.filter { it.fileName.toString().startsWith("wal.") }.toList() }.sortedBy(::seqOf)
}
