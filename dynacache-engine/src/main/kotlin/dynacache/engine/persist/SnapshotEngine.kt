package dynacache.engine.persist

import dynacache.engine.ApEngine
import dynacache.engine.Value
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Random

/**
 * Local RDB snapshots of spec 2.8: `<dir>/dump.rdb`, saved on an interval and at shutdown,
 * restored at startup. A save takes every partition's point-in-time view on that partition's
 * executor and serializes off the executors, so the command path never waits on the disk
 * (C9 by construction; `snapshot_does_not_block_reads`).
 *
 * The engine owns no thread: the server's scheduler calls [maybeSave] and its shutdown hook
 * calls [close]. [save] and [restore] share one monitor, so neither ever runs inside the other.
 * [sink] is where the writer's bytes go, injectable so a test can stall it.
 */
class SnapshotEngine(
    private val engine: ApEngine,
    dir: Path,
    private val clock: Clock,
    private val interval: Duration = Duration.ofSeconds(300),
    private val seeds: Random = Random(),
    private val sink: (Path) -> OutputStream = { Files.newOutputStream(it) },
) : AutoCloseable {

    private val file = dir.resolve("dump.rdb")
    private val temp = dir.resolve("dump.rdb.tmp")
    private var lastSave: Instant = clock.instant()

    /** Writes the whole keyspace to a temporary file and renames it over the last snapshot in one step. */
    @Synchronized
    fun save() {
        val now = clock.instant()
        val entries = engine.snapshotView(now).join()
        sink(temp).use { RdbWriter.write(it, entries, now) }
        Files.move(temp, file, ATOMIC_MOVE, REPLACE_EXISTING)
        lastSave = now
    }

    /** Loads the last snapshot into the engine, if there is one; answers how many keys came back. */
    @Synchronized
    fun restore(): Int {
        if (!Files.exists(file)) return 0
        val entries = Files.newInputStream(file).use { RdbReader(seeds).read(it) }
        // A sorted set is never empty (T07), so an empty one is a writer bug, not an empty set.
        if (entries.any { entry -> entry.value.let { it is Value.ZSet && it.scores.size == 0 } }) {
            throw IOException("$file holds a sorted set with no members")
        }
        engine.restore(entries).join()
        return entries.size
    }

    /** The interval hook: saves when [interval] has passed since the last save. */
    fun maybeSave(now: Instant) {
        if (!now.isBefore(lastSave.plus(interval))) save()
    }

    /** The graceful-shutdown save, to be called before the engine closes. */
    override fun close() = save()
}
