package dynacache.engine.persist

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.Value
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.WRITE
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Random
import kotlin.io.path.exists
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * One node's part of a snapshot set on disk (spec 2.8 step 5): the state it cut and the bytes it
 * recorded per channel. Nothing here knows what the recorded bytes mean; that is the cluster's.
 */
class SnapshotPartsTest {

    @TempDir
    lateinit var root: Path

    /** The node's own data directory (spec 2.8), where its live log lives; never under [root]. */
    @TempDir
    lateinit var data: Path

    private val clock: Clock = Clock.fixed(Instant.parse("2026-09-06T12:00:00Z"), ZoneOffset.UTC)
    private val engines = ArrayList<ApEngine>()

    private fun engine(): ApEngine = ApEngine(2, clock, Random(20260906)).also(engines::add)

    private fun parts(engine: ApEngine, self: String = "node-1"): SnapshotParts =
        FileSnapshotParts(root, self, engine, clock)

    @AfterEach
    fun close() = engines.forEach { it.close() }

    private fun bytes(text: String) = text.toByteArray()

    private fun channelLog(self: String, id: String, peer: String): Path =
        root.resolve(id).resolve(self).resolve("from-$peer.wal")

    @Test
    fun a_channel_replays_what_was_recorded_on_it_in_order() {
        val parts = parts(engine())
        parts.cut("s1")
        listOf("one", "two", "three").forEach { parts.record("s1", "node-2", bytes(it)) }
        parts.record("s1", "node-3", bytes("elsewhere"))

        assertEquals(listOf("one", "two", "three"), parts.replay("s1", "node-2").map { it.decodeToString() })
        assertEquals(listOf("elsewhere"), parts.replay("s1", "node-3").map { it.decodeToString() })
        assertEquals(emptyList<ByteArray>(), parts.replay("s1", "node-4"), "a channel that recorded nothing")
    }

    /** The shape a crash mid-append leaves: the last record is half on disk, so it is no record. */
    @Test
    fun snapshot_part_with_torn_channel_log_replays_the_complete_prefix() {
        val parts = parts(engine())
        parts.cut("s1")
        listOf("one", "two", "three").forEach { parts.record("s1", "node-2", bytes(it)) }
        val log = channelLog("node-1", "s1", "node-2")
        FileChannel.open(log, WRITE).use { it.truncate(Files.size(log) - 3) }

        assertEquals(listOf("one", "two"), parts.replay("s1", "node-2").map { it.decodeToString() })
    }

    /** A record whose bytes do not match its checksum is corruption, not a torn tail: it is not skipped. */
    @Test
    fun a_channel_log_with_a_corrupt_record_is_rejected() {
        val parts = parts(engine())
        parts.cut("s1")
        parts.record("s1", "node-2", bytes("one"))
        val log = channelLog("node-1", "s1", "node-2")
        FileChannel.open(log, WRITE).use { it.write(ByteBuffer.wrap(bytes("X")), Files.size(log) - 1) }

        val failure = assertThrows(IOException::class.java) { parts.replay("s1", "node-2") }
        assertTrue(failure.message!!.contains("from-node-2.wal"), failure.message)
    }

    @Test
    fun a_part_holds_the_state_it_cut() {
        val before = engine()
        before.submit(Command.Set(Key(bytes("k")), bytes("1"), null, null)).get()
        parts(before).cut("s1")
        before.submit(Command.Set(Key(bytes("k")), bytes("2"), null, null)).get()

        val after = engine()
        parts(after).restore("s1")
        assertEquals(Reply.Bulk(bytes("1")), after.submit(Command.Get(Key(bytes("k")))).get())
    }

    @Test
    fun a_set_is_deleted_as_a_whole() {
        parts(engine()).cut("s1")
        parts(engine(), self = "node-2").cut("s1")
        parts(engine()).cut("s2")

        parts(engine()).delete("s1")
        assertFalse(root.resolve("s1").exists(), "every node's part of the set went")
        assertTrue(root.resolve("s2").exists(), "another set is untouched")
        parts(engine()).delete("s1")
    }

    /**
     * A node with a data directory logs every write; its part of a set holds the state as of the
     * log's seq at the cut and no log at all, so the live log is never inside a part and deleting
     * the set can never take a file recovery needs (C14, spec 2.8 recovery).
     */
    @Test
    fun snapshot_part_holds_the_log_up_to_the_cut_only() {
        val live = engine()
        SnapshotEngine(live, data, clock, fsync = FsyncPolicy.NEVER).restore()
        live.submit(Command.Set(Key(bytes("k")), bytes("before"), null, null)).get()
        val atCut = live.wal!!.lastSeq
        parts(live).cut("s1")
        live.submit(Command.Set(Key(bytes("k")), bytes("after"), null, null)).get()
        live.submit(Command.Set(Key(bytes("later")), bytes("1"), null, null)).get()
        live.wal!!.close()

        val part = root.resolve("s1").resolve("node-1")
        assertEquals(emptyList<Path>(), Files.walk(root).use { it.filter { p -> p.fileName.toString().startsWith("wal.") }.toList() }, "no log file under any part")
        val checkpoint = Files.newInputStream(part.resolve("dump.rdb")).use { RdbReader(Random(1)).read(it) }
        assertEquals(atCut, checkpoint.walSeq, "the part's state is the log up to the cut")
        assertEquals(listOf("before"), checkpoint.entries.map { (it.value as Value.Str).bytes.decodeToString() })
        val log = WalReader(data.resolve("wal.0")).readAll()
        assertEquals((1..atCut + 2).toList(), log.entries.map { it.seq }, "the live log continued under the data directory")

        val restarted = engine()
        SnapshotEngine(restarted, data, clock, fsync = FsyncPolicy.NEVER).restore()
        assertEquals(Reply.Bulk(bytes("after")), restarted.submit(Command.Get(Key(bytes("k")))).get())
        assertEquals(Reply.Bulk(bytes("1")), restarted.submit(Command.Get(Key(bytes("later")))).get())
    }

    /** Before this ticket a channel log was length-delimited protobuf in `from-<peer>.log`. */
    @Test
    fun a_part_written_before_the_channel_log_became_a_wal_is_rejected() {
        val parts = parts(engine())
        parts.cut("s1")
        Files.newOutputStream(channelLog("node-1", "s1", "node-2").resolveSibling("from-node-2.log"), CREATE, APPEND)
            .use { it.write(bytes("stale")) }

        val failure = assertThrows(IOException::class.java) { parts.restore("s1") }
        assertTrue(failure.message!!.contains("from-node-2.log"), failure.message)
    }
}
