package dynacache.cluster

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.Value
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The versioned store at its own seam (T66, spec 2.5 and 5.3): the (value, version) pair moves
 * as one task on the key's partition, and spec 5.3 is decided here whether the arriving pair
 * is a command or a value.
 */
class VersionedStoreTest {

    private val self = NodeId("node-1")
    private val other = NodeId("node-2")
    private val key = Key("orders:4711")
    private val clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
    private val engines = mutableListOf<ApEngine>()

    private fun store(): VersionedStore {
        val engine = ApEngine(1, clock).also(engines::add)
        return VersionedStore(engine, DotCounter.of(self, emptyList()))
    }

    @AfterEach
    fun close() = engines.forEach { it.close() }

    /**
     * The review's race: a value read and a version read as two steps, with a write in between.
     * The partition thread is parked; a read, a write and an install queue up behind it in that
     * order. Under the old shape the write's bump was already in the table before the read ran,
     * so the read paired the old value with the new version. Here each pair is one task, so the
     * read sees the old pair whole and the reads after each install see that install's pair.
     */
    @Test
    fun read_never_pairs_a_value_with_another_installs_version() {
        val store = store()
        val engine = engines.single()
        val (_, v1) = store.write(Command.Set(key, "v1".toByteArray())).get()
        // Another node's write on top of the second write below, `(node-1, 2)`, which it has seen.
        val v3 = Dvv(Dot(other, 1), mapOf(self to 2L))

        val gate = CountDownLatch(1)
        engine.atomically(listOf(key)) { gate.await() }
        val read = store.read(Command.Get(key))
        val held = store.held(key)
        val write = store.write(Command.Set(key, "v2".toByteArray()))
        val afterWrite = store.read(Command.Get(key))
        val install = store.install(key, Value.Str("v3".toByteArray()), null, v3)
        val afterInstall = store.held(key)
        gate.countDown()

        assertEquals(Reply.Bulk("v1".toByteArray()) to v1, read.get())
        assertEquals("v1", str(held.get()))
        assertEquals(v1, held.get()!!.dvv)
        val (_, v2) = write.get()
        assertTrue(v2.dominates(v1), "$v2 descends from $v1")
        assertEquals(Reply.Bulk("v2".toByteArray()) to v2, afterWrite.get())
        assertEquals(Outcome.TAKEN, install.get().outcome)
        assertEquals("v3", str(afterInstall.get()))
        assertEquals(v3, afterInstall.get()!!.dvv)
    }

    /**
     * One concurrent pair, arriving on one node as the replicated command and on another as
     * the value anti-entropy ships: the same spec 5.3 branch, the same stored value, and a
     * version descending from both siblings either way. The string's remote side is the last
     * writer (the higher node name, spec 2.5), so applying the command over the local value
     * and merging the two values agree; a hash agrees whichever side is last, since the
     * command sets a field and the merge unions fields.
     */
    @Test
    fun spec_5_3_decided_once() {
        val hash = Key("orders:4712")
        val local = Dvv(Dot(self, 1), emptyMap())
        val remote = Dvv(Dot(other, 1), emptyMap())
        val asCommand = store()
        val asValue = store()
        for (store in listOf(asCommand, asValue)) {
            assertEquals(Outcome.TAKEN, store.install(key, Value.Str("a".toByteArray()), null, local).get().outcome)
            assertEquals(Outcome.TAKEN, store.install(hash, hashOf("f1" to "1"), null, local).get().outcome)
        }

        assertEquals(Outcome.MERGED, asCommand.apply(listOf(Command.Set(key, "b".toByteArray())), remote).get())
        assertEquals(Outcome.MERGED, asCommand.apply(listOf(Command.HSet(hash, listOf("f2".toByteArray() to "2".toByteArray()))), remote).get())
        assertEquals(Outcome.MERGED, asValue.install(key, Value.Str("b".toByteArray()), null, remote).get().outcome)
        assertEquals(Outcome.MERGED, asValue.install(hash, hashOf("f2" to "2"), null, remote).get().outcome)

        for (k in listOf(key, hash)) {
            val command = asCommand.held(k).get()!!
            val value = asValue.held(k).get()!!
            assertEquals(canon(command.value!!), canon(value.value!!), "$k stored the same either way")
            for (dvv in listOf(command.dvv, value.dvv)) {
                assertTrue(dvv.dominates(local) && dvv.dominates(remote), "$dvv descends from both siblings")
                assertEquals(self, dvv.dot.node, "the merged version is this node's dot")
            }
        }
        assertEquals("b", str(asValue.held(key).get()))
        assertEquals(mapOf("f1" to "1".toByteArray().toList(), "f2" to "2".toByteArray().toList()), canon(asValue.held(hash).get()!!.value!!))
    }

    /**
     * ADR 0003's consequence, pinned where it is decided. When the local side is the last
     * writer, spec 2.5's string rule keeps the local value for a pair that arrives as a value,
     * but a pair that arrives as a command carries no value to merge and is applied over the
     * local one. Both land under a version descending from both siblings, so the two nodes are
     * one anti-entropy round from agreeing.
     */
    @Test
    fun spec_5_3_command_arrival_applies_over_local_where_a_value_arrival_keeps_last_writer() {
        val local = Dvv(Dot(other, 1), emptyMap())
        val remote = Dvv(Dot(self, 1), emptyMap())
        val asCommand = store()
        val asValue = store()
        for (store in listOf(asCommand, asValue)) store.install(key, Value.Str("a".toByteArray()), null, local).get()

        assertEquals(Outcome.MERGED, asCommand.apply(listOf(Command.Set(key, "b".toByteArray())), remote).get())
        assertEquals(Outcome.MERGED, asValue.install(key, Value.Str("b".toByteArray()), null, remote).get().outcome)

        assertEquals("b", str(asCommand.held(key).get()), "the command applied over the local value")
        assertEquals("a", str(asValue.held(key).get()), "the value merge kept the last writer")
    }

    /** Spec 5.3's other two branches, and a tombstone: a dominated or equal arrival is kept out, a dominating tombstone deletes. */
    @Test
    fun dominated_arrival_is_kept_out_and_a_dominating_tombstone_deletes() {
        val store = store()
        val (_, v1) = store.write(Command.Set(key, "v1".toByteArray())).get()
        val older = Dvv(Dot(self, 0), emptyMap())

        assertEquals(Outcome.KEPT, store.apply(listOf(Command.Set(key, "old".toByteArray())), older).get())
        assertEquals(Outcome.KEPT, store.install(key, Value.Str("old".toByteArray()), null, older).get().outcome)
        assertEquals(Outcome.KEPT, store.apply(listOf(Command.Set(key, "same".toByteArray())), v1).get())
        assertEquals("v1", str(store.held(key).get()))

        val tombstone = v1.bump(DotCounter.of(other, emptyList()))
        val installed = store.install(key, null, null, tombstone).get()
        assertEquals(Outcome.TAKEN, installed.outcome)
        assertNull(installed.held!!.value)
        assertEquals(tombstone, store.version(key))
        assertEquals(Reply.Bulk(null) to tombstone, store.read(Command.Get(key)).get())
        assertEquals(tombstone, store.held(key).get()!!.dvv)
        assertEquals(listOf(tombstone), store.held(listOf(key)).get().map { it.dvv }, "a tombstone is held, for anti-entropy to carry")
        assertEquals(listOf(tombstone), store.held { it == key }.get().map { it.dvv })
    }

    private fun str(held: Held?): String = String((held!!.value as Value.Str).bytes)

    private fun hashOf(vararg fields: Pair<String, String>): Value.Hash =
        Value.Hash().also { hash -> fields.forEach { (name, value) -> hash.fields.put(name, value.toByteArray()) } }
}
