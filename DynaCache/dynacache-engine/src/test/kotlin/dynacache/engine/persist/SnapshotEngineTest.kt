package dynacache.engine.persist

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore

/**
 * The snapshot engine through its two seams: `save`/`restore` on the file, and
 * `CommandEngine.submit` racing them. Nothing here reads the file layout; that is RdbTest's.
 */
class SnapshotEngineTest {

    @TempDir
    lateinit var dir: Path

    private val now: Instant = Instant.parse("2026-09-06T12:00:00Z")
    private val clock: Clock = Clock.fixed(now, ZoneOffset.UTC)
    private val engines = ArrayList<ApEngine>()

    private fun engine(): ApEngine = ApEngine(4, clock, Random(20260906)).also(engines::add)

    @AfterEach
    fun close() = engines.forEach { it.close() }

    private fun key(name: String) = Key(name.toByteArray())
    private fun bytes(text: String) = text.toByteArray()
    private fun ApEngine.run(command: Command): Reply = submit(command).get()
    private fun ApEngine.get(name: String): String? = (run(Command.Get(key(name))) as Reply.Bulk).bytes?.toString(Charsets.ISO_8859_1)
    private fun texts(reply: Reply): List<String> = (reply as Reply.Array).items.map { (it as Reply.Bulk).bytes!!.toString(Charsets.ISO_8859_1) }

    @Test
    fun snapshot_restore_on_startup() {
        val before = engine()
        before.run(Command.Set(key("plain"), bytes("hello"), null, null))
        before.run(Command.Set(key("timed"), bytes("soon"), null, Duration.ofSeconds(90)))
        before.run(Command.HSet(key("profile"), listOf(bytes("name") to bytes("ada"))))
        before.run(Command.Push(key("queue"), listOf(bytes("a"), bytes("b")), Command.End.TAIL))
        before.run(Command.ZAdd(key("board"), listOf(bytes("2.5") to bytes("bob"), bytes("1") to bytes("al"))))
        SnapshotEngine(before, dir, clock).save()

        val after = engine()
        assertEquals(5, SnapshotEngine(after, dir, clock).restore())
        assertEquals("hello", after.get("plain"))
        assertEquals(Reply.Integer(90), after.run(Command.Ttl(key("timed"), Command.Ttl.Precision.SECONDS)))
        assertEquals(listOf("name", "ada"), texts(after.run(Command.HGetAll(key("profile")))))
        assertEquals(listOf("a", "b"), texts(after.run(Command.LRange(key("queue"), 0, -1))))
        assertEquals(listOf("al", "bob"), texts(after.run(Command.ZRange(key("board"), 0, -1, withScores = false, reverse = false))))
    }

    @Test
    fun snapshot_atomic_rename_leaves_no_tmp() {
        val engine = engine()
        val snapshots = SnapshotEngine(engine, dir, clock)
        assertEquals(0, snapshots.restore(), "no file yet is an empty restore, not an error")

        engine.run(Command.Set(key("k"), bytes("v")))
        snapshots.save()
        snapshots.save()

        assertEquals(listOf("dump.rdb"), Files.list(dir).use { it.map { p -> p.fileName.toString() }.toList() })
    }

    /** A sink that parks the writer at its first byte until the test lets it go. */
    private class StalledSink(path: Path, val stalled: CountDownLatch, val release: CountDownLatch) : FileOutputStream(path.toFile()) {
        override fun write(b: ByteArray, off: Int, len: Int) {
            stalled.countDown()
            release.await()
            super.write(b, off, len)
        }
    }

    @Test
    fun snapshot_does_not_block_reads() {
        val engine = engine()
        engine.run(Command.Set(key("k"), bytes("v")))
        val stalled = CountDownLatch(1)
        val release = CountDownLatch(1)
        val snapshots = SnapshotEngine(engine, dir, clock, sink = { StalledSink(it, stalled, release) })

        val saving = Thread(snapshots::save).apply { start() }
        stalled.await()

        assertEquals("v", engine.get("k"), "the read completes while the writer is parked")

        release.countDown()
        saving.join()
        assertEquals(1, SnapshotEngine(engine(), dir, clock).restore())
    }

    /** Runs [round] with rising stamps on its own thread until halted; [rounds] gains a permit per finished round. */
    private class Writer(private val round: (Long) -> Unit) : Thread() {
        val rounds = Semaphore(0)
        @Volatile var stamp = 0L
        @Volatile private var stopped = false

        override fun run() {
            while (!stopped) {
                round(++stamp)
                rounds.release()
            }
        }

        fun halt() {
            stopped = true
            join()
        }
    }

    private fun restored(): ApEngine = engine().also { SnapshotEngine(it, dir, clock).restore() }

    @Test
    fun rdb_concurrent_writes() {
        val engine = engine()
        val fields = (0 until 32).map { bytes("f%02d".format(it)) }
        // Each round writes the fields in order with its stamp, so a point-in-time copy is a
        // prefix of one round over the tail of the previous: never anything else.
        val writer = Writer { stamp ->
            for (field in fields) engine.run(Command.HSet(key("h"), listOf(field to bytes(stamp.toString()))))
        }
        val stalled = CountDownLatch(1)
        val release = CountDownLatch(1)
        val snapshots = SnapshotEngine(engine, dir, clock, sink = { StalledSink(it, stalled, release) })

        writer.start()
        writer.rounds.acquire(3)
        val saving = Thread(snapshots::save).apply { start() }
        // The view is taken before the first byte reaches the sink; the writer then moves on
        // three whole rounds while the sink is parked, so a view that shared the live hash
        // would serialize stamps the partition never held at one instant.
        stalled.await()
        val atStall = writer.stamp
        writer.rounds.drainPermits()
        writer.rounds.acquire(3)
        release.countDown()
        saving.join()
        writer.halt()

        val stamps = texts(restored().run(Command.HMGet(key("h"), fields))).map(String::toLong)
        assertEquals(stamps.sortedDescending(), stamps, "a later field never carries a newer stamp than an earlier one")
        assertTrue(stamps.max() - stamps.min() <= 1, "the fields span at most two adjacent rounds: $stamps")
        assertTrue(stamps.max() <= atStall, "nothing written after the view was taken is in the file: $stamps")
    }

    @Test
    fun C9_snapshot_never_contains_half_a_batch() {
        val engine = engine()
        val keys = (0 until 10).map { key("{b}k$it") }
        val writer = Writer { stamp ->
            engine.atomically(keys) { ctx -> keys.forEach { ctx.execute(Command.Set(it, bytes(stamp.toString()))) } }.get()
        }

        writer.start()
        writer.rounds.acquire(3)
        SnapshotEngine(engine, dir, clock).save()
        writer.halt()

        val values = keys.map { restored().get(it.bytes.toString(Charsets.ISO_8859_1)) }
        assertEquals(1, values.distinct().size, "all ten keys carry one batch's stamp or none: $values")
    }

    /**
     * The engine makes its own directory. Every caller hands it a path that came from
     * configuration -- a command-line argument, a set's part -- and no caller above the persist
     * package should have to prepare the directory before the engine can use it (T81). With a
     * log, the first `restore` lists the directory too, so a missing one used to fail there.
     */
    @Test
    fun a_data_directory_that_does_not_exist_yet_is_the_engine_s_to_create() {
        val fresh = dir.resolve("node-1").resolve("data")
        val engine = engine()
        val snapshots = SnapshotEngine(engine, fresh, clock, fsync = FsyncPolicy.NEVER)

        assertTrue(Files.isDirectory(fresh), "the engine left its own directory to its caller")
        assertEquals(0, snapshots.restore(), "a directory with nothing in it restores nothing")
        engine.run(Command.Set(key("plain"), bytes("hello"), null, null))
        snapshots.close()

        assertTrue(Files.isRegularFile(SnapshotEngine.stateFile(fresh)), "the shutdown save wrote no snapshot")
    }
}
