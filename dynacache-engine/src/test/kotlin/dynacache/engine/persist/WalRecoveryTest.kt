package dynacache.engine.persist

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.SECONDS

/**
 * The WAL in the write path, through the engine's own seams: `submit` for the reply-after-durable
 * rule (C14) and `SnapshotEngine.save`/`restore` for checkpoint and recovery. Nothing here reads
 * the log's layout; that is WalTest's.
 */
class WalRecoveryTest {

    @TempDir
    lateinit var dir: Path

    private val now: Instant = Instant.parse("2026-09-06T12:00:00Z")
    private val clock: Clock = Clock.fixed(now, ZoneOffset.UTC)
    private val engines = ArrayList<ApEngine>()

    private fun engine(): ApEngine = ApEngine(4, clock, Random(35)).also(engines::add)

    @AfterEach
    fun close() = engines.forEach { it.close() }

    private fun key(name: String) = Key(name.toByteArray())
    private fun bytes(text: String) = text.toByteArray()
    private fun ApEngine.run(command: Command): Reply = submit(command).get(5, SECONDS)
    private fun ApEngine.set(name: String, value: String) = run(Command.Set(key(name), bytes(value)))
    private fun ApEngine.get(name: String): String? = (run(Command.Get(key(name))) as Reply.Bulk).bytes?.toString(Charsets.ISO_8859_1)

    @Test
    fun C14_reply_only_after_durable_append() {
        val engine = engine()
        val sink = StalledSink(FileChannelSink(dir.resolve("wal.0")))
        engine.wal = WalWriter(sink, 1, FsyncPolicy.ALWAYS, clock)

        val write = engine.submit(Command.Set(key("a"), bytes("1")))
        assertTrue(sink.stalled.await(5, SECONDS))
        // Another partition owes the log nothing for a read: it answers while the write waits.
        val elsewhere = generateSequence(0) { it + 1 }.map { key("r$it") }.first { engine.partitionOf(it) != engine.partitionOf(key("a")) }
        assertEquals(Reply.Bulk(null), engine.submit(Command.Get(elsewhere)).get(5, SECONDS))
        assertFalse(write.isDone, "the reply must wait for the fsync")

        sink.release.countDown()
        assertEquals(Reply.Simple("OK"), write.get(5, SECONDS))
    }

    private fun persistence(engine: ApEngine, policy: FsyncPolicy = FsyncPolicy.NEVER) =
        SnapshotEngine(engine, dir, clock, fsync = policy)

    @Test
    fun wal_full_recovery() {
        val before = engine()
        val snapshots = persistence(before)
        assertEquals(0, snapshots.restore())
        before.set("a", "1")
        before.set("b", "2")
        before.run(Command.IncrBy(key("n"), 5))
        snapshots.save()
        before.set("c", "3")
        before.run(Command.Set(key("d"), bytes("4"), null, java.time.Duration.ofSeconds(90)))
        before.run(Command.IncrBy(key("n"), 5))
        before.run(Command.Del(key("a")))
        // The crash: the engine is dropped without a close, so nothing is saved on the way out.

        val after = engine()
        persistence(after).restore()
        assertEquals(null, after.get("a"))
        assertEquals("2", after.get("b"))
        assertEquals("3", after.get("c"))
        assertEquals("4", after.get("d"))
        assertEquals(Reply.Integer(90), after.run(Command.Ttl(key("d"), Command.Ttl.Precision.SECONDS)))
        assertEquals("10", after.get("n"))
    }

    @Test
    fun wal_checkpoint_truncates() {
        val engine = engine()
        val snapshots = persistence(engine)
        snapshots.restore()
        engine.set("a", "1")
        engine.set("b", "2")
        assertEquals(2, snapshots.save(), "the cut is the last entry the snapshot holds")
        engine.set("c", "3")

        // One snapshot and one log, and the log holds only what came after the cut.
        assertEquals(listOf("dump.rdb", "wal.2"), names())
        assertEquals(listOf(3L), WalReader(dir.resolve("wal.2")).readAll().entries.map { it.seq })

        assertEquals(3, snapshots.save())
        assertEquals(listOf("dump.rdb", "wal.3"), names())
        assertEquals("3", engine().also { persistence(it).restore() }.get("c"))
    }

    @Test
    fun wal_replay_idempotent() {
        val before = engine()
        persistence(before).restore()
        before.run(Command.IncrBy(key("n"), 1))
        before.run(Command.Push(key("q"), listOf(bytes("x")), Command.End.TAIL))
        // The log holds each entry twice, so a redo that trusted the bytes would count twice.
        val log = dir.resolve("wal.0")
        Files.write(log, Files.readAllBytes(log), StandardOpenOption.APPEND)

        val after = engine()
        persistence(after).restore()
        assertEquals("1", after.get("n"))
        assertEquals(Reply.Integer(1), after.run(Command.LLen(key("q"))))
    }

    @Test
    fun wal_reads_append_nothing() {
        val engine = engine()
        persistence(engine).restore()
        engine.set("a", "1")
        engine.run(Command.HSet(key("h"), listOf(bytes("f") to bytes("v"))))
        engine.run(Command.Push(key("q"), listOf(bytes("x")), Command.End.TAIL))
        val logged = engine.wal!!.lastSeq
        assertEquals(3, logged)

        engine.get("a")
        engine.run(Command.Exists(key("a")))
        engine.run(Command.Ttl(key("a"), Command.Ttl.Precision.SECONDS))
        engine.run(Command.HGetAll(key("h")))
        engine.run(Command.LRange(key("q"), 0, -1))
        engine.run(Command.DbSize)
        engine.run(Command.Ping)
        engine.get("missing")
        // A refused write and an empty pop changed nothing either.
        assertEquals(Reply.Bulk(null), engine.run(Command.Set(key("a"), bytes("2"), Command.Set.Condition.NX)))
        assertEquals(Reply.Bulk(null), engine.run(Command.Pop(key("missing"), Command.End.HEAD)))
        assertEquals(logged, engine.wal!!.lastSeq)
    }

    @Test
    fun wal_batch_is_replayed_as_written() {
        val before = engine()
        persistence(before).restore()
        val a = key("{u}.a")
        val b = key("{u}.b")
        val replies = before.atomically(listOf(a, b)) { ctx ->
            listOf(ctx.execute(Command.Set(a, bytes("1"))), ctx.execute(Command.Set(b, bytes("2"))), ctx.execute(Command.IncrBy(b, 1)))
        }.get(5, SECONDS)
        assertEquals(listOf(Reply.Simple("OK"), Reply.Simple("OK"), Reply.Integer(3)), replies)

        val after = engine()
        persistence(after).restore()
        assertEquals("1", after.get("{u}.a"))
        assertEquals("3", after.get("{u}.b"))
    }

    private fun names(): List<String> = Files.list(dir).use { it.map { p -> p.fileName.toString() }.toList() }.sorted()

    /** The filesystem is a true boundary: this adapter holds the first fsync until the test lets it go. */
    private class StalledSink(private val delegate: WalSink) : WalSink {
        val stalled = CountDownLatch(1)
        val release = CountDownLatch(1)

        override fun write(bytes: ByteBuffer) = delegate.write(bytes)

        override fun fsync() {
            stalled.countDown()
            assertTrue(release.await(5, SECONDS))
            delegate.fsync()
        }

        override fun close() = delegate.close()
    }
}
